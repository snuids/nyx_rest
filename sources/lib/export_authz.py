"""Access control re-applied where the queue worker reads the data.

The API checks `can_use_indice` on every read, but the worker talks to Elasticsearch directly: a
message on `/queue/REST_LOAD_DATA` names an index, and the worker exports it without asking
anyone. Its only barrier used to be the route that publishes the message — and that route
accepted any destination from any account. Together they let an ordinary authenticated account
export any index, `nyx_user` included, and have the CSV mailed to an address taken from the
message rather than from the account.

`can_use_indice` itself cannot be reused here: it reads `state`, which only the API process
initialises. These rules are read from the same `nyx_indice` documents and applied with the
client the worker already holds.

The failure mode is deliberate: an unreadable rule set refuses the export. A worker that cannot
tell whether an account may read an index must not guess in its favour.
"""

import logging
import re

from lib.index_expression import expression_reaches

logger = logging.getLogger()

# `nyx_user` holds every account's password hash. A read may return the caller's own record; an
# export has no row filter — it writes the whole result to a file and mails it — so only
# administrators may export it at all.
ACCOUNT_INDEX_ROOTS = ("nyx_user", "reindexed-nyx_user")

ADMIN_PRIVILEGE = "admin"


def is_admin(user):
    return ADMIN_PRIVILEGE in ((user or {}).get("privileges") or [])


def load_rules(es_client, size=1000):
    """The `nyx_indice` documents, or `None` when they cannot be read."""
    try:
        hits = es_client.search(index="nyx_indice", body={"size": size})["hits"]["hits"]
    except Exception as error:  # network, mapping, missing index: all mean "unknown"
        logger.error("Cannot read the index rules, exports refused: %s", error)
        return None
    return [hit.get("_source", {}) for hit in hits]


def kibana_indices(matchrequest):
    """The indices an msearch payload targets.

    `matchrequest` is the raw msearch body: each request is an index line followed by a JSON line.
    An expression that cannot be read means the export is refused, not allowed by default.
    """
    indices = []
    for line in str(matchrequest or "").splitlines():
        candidate = line.strip()
        if not candidate or candidate.startswith("{") or candidate.startswith("}"):
            continue
        if re.fullmatch(r"[A-Za-z0-9_\-.*?, ]+", candidate):
            indices.extend(part.strip() for part in candidate.split(",") if part.strip())
    return indices


def may_export(indice, user, rules):
    """True when this account may export `indice`.

    `rules` is what `load_rules` returned; passing `None` (rules unknown) refuses.
    """
    if not indice or not user:
        return False
    # An administrator is not subject to the rules, so unreadable rules cannot hold them back.
    if is_admin(user):
        return True
    if rules is None:
        # Fail closed: without the rules we cannot tell whether this index is restricted.
        return False
    if any(expression_reaches(indice, root) for root in ACCOUNT_INDEX_ROOTS):
        logger.info("Export of %s refused: account index, administrators only", indice)
        return False

    privileges = user.get("privileges") or []
    for rule in rules:
        pattern = rule.get("indicepattern")
        if not pattern:
            continue
        try:
            matches = re.search(pattern, indice) is not None
        except re.error:
            # An unreadable rule means we cannot tell whether this index is restricted: refuse
            # rather than let the remaining rules decide in the account's favour.
            logger.error("Invalid index pattern in nyx_indice, exports refused: %r", pattern)
            return False
        if not matches:
            continue
        required = rule.get("privileges") or ""
        if required and not [value for value in privileges if value in required]:
            logger.info("Export of %s refused: missing a required privilege", indice)
            return False
    return True
