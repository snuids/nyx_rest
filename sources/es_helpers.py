"""
Elasticsearch helpers: index/translation caching, access-control check,
AMQC event sender, and the handleAPICalls background thread.
"""

import re
import json
import time
import logging
from datetime import datetime, timedelta

import state

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
            if state.conn is not None:
                logger.debug("Sending Life Sign")
                state.conn.send_life_sign()
                logger.debug("Sleeping")
        except Exception as e:
            logger.error("Unable to send life sign or api history.")
            logger.error(e)

        time.sleep(5)
