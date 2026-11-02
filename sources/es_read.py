"""Reading a document by identifier.

This module depends on neither Flask nor the Elasticsearch client: it takes an `es` object and
returns an `(error, document)` pair. That is what makes it testable with a fake client, without
mounting an application or installing pandas.

It exists because a missing document is not an outage: creating a user starts with a GET on
their future identifier, and answering "unable to get data" there pushed a normal operation
into the API's error counter.
"""

import logging

logger = logging.getLogger(__name__)


def is_not_found(exc):
    """True if the exception reflects a missing document, and not an outage."""
    return (getattr(exc, "status_code", None) == 404
            or type(exc).__name__ == "NotFoundError")

def find_user_by_login(es, login, elkversion):
    """Finds a user by login when their _id does not hold it.

    Eight production accounts are indexed under their e-mail address while their `login`
    field carries a short name (`amarchand@icloud.com` -> `amarchand`). Without this
    fallback those accounts cannot be found by login — and the interface, which checks
    whether an e-mail is available before creating an account, concludes it is free and
    creates a duplicate.
    """
    body = {"size": 1, "query": {"term": {"login.keyword": str(login).lower()}}}
    try:
        if elkversion >= 7:
            res = es.search(index="nyx_user", body=body)
        else:
            res = es.search(index="nyx_user", doc_type="doc", body=body)
    except Exception:
        logger.warning("recherche par login impossible", exc_info=True)
        return None
    hits = res.get("hits", {}).get("hits", [])
    return hits[0] if hits else None

def fetch_document(es, index, object_id, elkversion, doc_type="doc"):
    """Reads a document by identifier. A missing document is not an API error.

    Creating a user starts with a GET on their future identifier: the document obviously
    does not exist yet, and the interface relies on that absence to allow the creation.
    Answering "unable to get data" therefore pushed a normal operation into the error
    counter — three per account creation.
    """
    try:
        if elkversion >= 7:
            return "", es.get(index=index, id=object_id)
        return "", es.get(index=index, id=object_id, doc_type=doc_type)
    except Exception as exc:
        if not is_not_found(exc):
            logger.error("unable to get data", exc_info=True)
            return "unable to get data", None
    if index == "nyx_user":
        return "", find_user_by_login(es, object_id, elkversion)
    return "", None
