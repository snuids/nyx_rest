"""
Elasticsearch helpers: index/translation caching, access-control check,
AMQC event sender, and the handleAPICalls background thread.
"""

import os
import re
import json
import time
import logging
from datetime import datetime, timedelta

import state
import metrics_aggregate

logger = logging.getLogger()


def refresh_indices():
    if state.last_indices_refresh + timedelta(seconds=state.indices_refresh_seconds) > datetime.now():
        return
    logger.info("Refresh Indices")
    if state.elkversion >= 7:
        state.indices = state.es.search(index="nyx_indice", body={})["hits"]["hits"]
    else:
        state.indices = state.es.search(index="nyx_indice", body={}, doc_type="doc")["hits"]["hits"]
    state.last_indices_refresh = datetime.now()


def refresh_translations():
    if state.last_translation_refresh + \
            timedelta(seconds=state.last_translation_refresh_seconds) > datetime.now():
        return
    logger.info("Refreshing Translations")
    if state.elkversion >= 7:
        translationsrec = state.es.search(
            index="nyx_translation", body={"size": 1000}
        )["hits"]["hits"]
    else:
        translationsrec = state.es.search(
            index="nyx_translation", body={"size": 1000}, doc_type="doc"
        )["hits"]["hits"]

    for tran in translationsrec:
        source = tran["_source"]
        for key in source:
            if len(key) == 2:
                if key not in state.translations:
                    state.translations[key] = {}
                if source["area"] not in state.translations[key]:
                    state.translations[key][source["area"]] = {}
                state.translations[key][source["area"]][source["item"]] = source[key]

    logger.info(state.translations)
    state.last_translation_refresh = datetime.now()


def get_translated_item(language, area, item):
    if language not in state.translations or area not in state.translations[language]:
        return item
    if item not in state.translations[language][area]:
        return item
    return state.translations[language][area][item]


NYX_USER_ROOTS = ("nyx_user", "reindexed-nyx_user")

# Indices an ordinary account must not be able to write to: they carry rights, not data.
# `nyx_user` holds the privileges and the password hashes, `nyx_app` decides what each
# screen may do and `nyx_privilege` what each role may see. `nyx_indice` (the access rules
# applied to data indices) and `nyx_filter` (the filter catalogue) decide who sees what:
# rewriting them amounted to opening a forbidden index without touching `nyx_privilege`.
ADMIN_WRITE_ROOTS = (
    "nyx_user", "reindexed-nyx_user",
    "nyx_app", "reindexed-nyx_app",
    "nyx_privilege", "reindexed-nyx_privilege",
    "nyx_privileges", "reindexed-nyx_privileges",
    "nyx_indice", "reindexed-nyx_indice",
    "nyx_filter", "reindexed-nyx_filter",
)


def has_wildcard(name):
    return "*" in name or "?" in name


def expression_reaches(expression, root):
    """True if this index expression may designate the `root` index.

    Three spellings lead to the same index and must be treated alike: the exact name
    (`nyx_user`), the reindexing alias (`reindexed-nyx_user-v8`, suffix after `-` or `.`)
    and the pattern (`nyx_user*`, `nyx_*`, `*`). A pattern is compared by prefix: as soon
    as it may cover the root, it is considered to target the index — `nyx_apicalls*` is
    not concerned, `nyx_*` is.
    """
    name = str(expression or "").lower()
    if not name:
        return False
    if not has_wildcard(name):
        return (name == root or name.startswith(root + "-") or name.startswith(root + "."))
    prefix = re.split(r"[*?]", name, 1)[0]
    return root.startswith(prefix) or prefix.startswith(root)


def is_nyx_user_indice(indice):
    """`nyx_user`, its reindexing alias, or a pattern that may bring it back.

    The exact name is not enough: `generic_search/nyx_user*` is used by the interface to
    list accounts, and a wider pattern (`nyx_*`, `*`) brings the index back too. Letting
    them through amounted to bypassing the restriction through the spelling of the index.
    """
    return any(expression_reaches(indice, root) for root in NYX_USER_ROOTS)


def requires_admin_for_write(indice):
    """True for the configuration and account indices, reserved for admin writes."""
    return any(expression_reaches(indice, root) for root in ADMIN_WRITE_ROOTS)


def is_admin(user):
    return "admin" in ((user or {}).get("privileges") or [])


def own_account_ids(user):
    """Identifiers under which this account may appear in `nyx_user`.

    Eight accounts carry their e-mail in `_id` and a short name in `login`: restricting the
    lookup to `_id` alone would leave a user unable to read their own record, and the
    password change form with it.
    """
    ids = []
    for key in ("id", "login"):
        value = (user or {}).get(key)
        if isinstance(value, str) and value and value not in ids:
            ids.append(value)
    return ids


def nyx_user_read_filter(user):
    """The "own account" clause. `None` when the token carries no identity."""
    ids = own_account_ids(user)
    if not ids:
        return None
    return {
        "bool": {
            "should": [
                {"ids": {"values": ids}},
                {"terms": {"login.keyword": ids}},
            ],
            "minimum_should_match": 1,
        }
    }


def can_read_nyx_user(user, object_id):
    """True if `user` is allowed to read the `object_id` document of `nyx_user`."""
    if is_admin(user):
        return True
    return str(object_id) in own_account_ids(user)


def can_use_indice(indice, user, query):
    refresh_indices()

    if query is None:
        query = {
            "bool": {
                "must": [{
                    "query_string": {
                        "query": "*",
                        "analyze_wildcard": True,
                        "default_field": "*",
                    }
                }]
            }
        }
    logger.info(query)

    queryindex = -1
    for index, que in enumerate(query["bool"]["must"]):
        if "query_string" in que:
            queryindex = index
            oldquery = que["query_string"]["query"]
            break

    if queryindex == -1:
        query["bool"]["must"].insert(0, {
            "query_string": {
                "query": "*",
                "analyze_wildcard": True,
                "default_field": "*",
            }
        })
        oldquery = ""
        queryindex = 0

    resultsmustbefiltered = None

    # An ordinary account only reads its own record in `nyx_user`. The index holds
    # everyone's pbkdf2 hashes: leaving them readable by an account with a mere application
    # tag amounts to publishing the platform's passwords. The restriction is placed here,
    # before the rule loop, to cover `generic_search`, `queryFilter` and the exports in one
    # go — and to survive the early returns of the `filtercolumn` / `privileges` branches,
    # which rewrite the `query_string` but not `bool.filter`.
    if is_nyx_user_indice(indice) and not is_admin(user):
        own = nyx_user_read_filter(user)
        if own is None:
            logger.info("nyx_user : aucune identité dans le jeton, lecture refusée")
            return (False, query, resultsmustbefiltered)
        query.setdefault("bool", {}).setdefault("filter", []).append(own)

    for ind in state.indices:
        pat = ind["_source"]["indicepattern"]

        if re.search(pat, indice) is not None and \
                "privilegecolumn" in ind["_source"] and \
                ind["_source"]["privilegecolumn"] != "":
            resultsmustbefiltered = ind["_source"]["privilegecolumn"]

        if re.search(pat, indice) is not None and \
                "privileges" in ind["_source"] and \
                ind["_source"]["privileges"] != "":
            if len([value for value in user["privileges"]
                    if value in ind["_source"]["privileges"]]) == 0:
                logger.info("Not allowed")
                return (False, query, resultsmustbefiltered)

        if re.search(pat, indice) is not None:
            if "filtercolumn" in ind["_source"] and ind["_source"]["filtercolumn"] != "":
                if "filters" in user and len(user["filters"]) > 0:
                    newquery = " OR ".join(
                        [ind["_source"]["filtercolumn"] + ":" + x for x in user["filters"]]
                    )
                    if len(oldquery) == 0:
                        query["bool"]["must"][queryindex]["query_string"]["query"] = newquery
                        return (True, query, resultsmustbefiltered)
                    else:
                        query["bool"]["must"][queryindex]["query_string"]["query"] = \
                            oldquery + " AND (" + newquery + ")"
                        return (True, query, resultsmustbefiltered)
                else:
                    return (True, query, resultsmustbefiltered)
            else:
                return (True, query, resultsmustbefiltered)

    return (True, query, resultsmustbefiltered)


def send_event(user, indice, method, _id, doc_type=None, obj=None):
    notif_dest = None

    for ind in state.indices:
        pat = ind["_source"]["indicepattern"]
        if re.search(pat, indice) is not None:
            tmp = ind["_source"].get('notifications')
            if tmp is not None and tmp != '':
                notif_dest = tmp
                break

    if notif_dest is not None:
        obj_to_send = {
            'user': user,
            'method': method,
            'indice': indice,
            'id': _id,
        }
        if doc_type is not None:
            obj_to_send['doc_type'] = doc_type
        if obj is not None:
            obj_to_send['obj'] = obj
        print(obj_to_send)
        state.conn.send_message(notif_dest, json.dumps(obj_to_send))
    else:
        print('no notif to send')


def handleAPICalls():
    from common import getELKVersion
    while True:
        try:
            logger.debug("APIs history")
            state.elkversion = getELKVersion(state.es)
            with state.userlock:
                apis = state.userActivities[:]
                state.userActivities = []
                if len(apis) > 0:
                    messagebody = ""
                    indexdatepattern = "nyx_apicalls-" + datetime.now().strftime("%Y.%m.%d").lower()
                    for api_entry in apis:
                        action = {}
                        if state.elkversion >= 7:
                            action["index"] = {"_index": indexdatepattern}
                        else:
                            action["index"] = {"_index": indexdatepattern, "_type": "doc"}
                        messagebody += json.dumps(action) + "\r\n"
                        messagebody += json.dumps(api_entry) + "\r\n"
                    state.es.bulk(messagebody)
            # Each worker publishes its counters: /metrics adds them up afterwards. The
            # loop already exists (one cycle every 5 s), so no extra thread.
            metrics_aggregate.publish(state.redisserver, os.getpid(),
                                      metrics_aggregate.snapshot(state))
            if state.conn is not None:
                logger.debug("Sending Life Sign")
                state.conn.send_life_sign()
                state.last_lifesign = datetime.now()
                logger.debug("Sleeping")
        except Exception as e:
            logger.error("Unable to send life sign or api history.")
            logger.error(e)

        time.sleep(5)
