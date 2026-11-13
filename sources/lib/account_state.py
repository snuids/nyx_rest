"""The `inactive` flag on a `nyx_user` document, and what it does to a live session.

Two questions, one answer each, and they must not disagree:

* **At sign-in** — may this account get a token at all? `finalize_login` asks `is_inactive()` on the
  document it is about to hand out a session for.
* **On every request** — is the account behind this token still allowed in? A token is a snapshot
  taken at sign-in and Redis keeps it for a day, so it cannot answer that: the document can. The
  answer is cached in Redis for a minute (`account_is_inactive`), which keeps an Elasticsearch round
  trip out of the hot path without making the administrator wonder whether the switch did anything.

The flag is **opt-in**: a document that does not carry `inactive` is an **active** account, so
nothing had to be migrated when it was introduced. A hand-written or imported document may carry
`"true"` or `1` rather than `true`, hence the tolerant reading.
"""

import logging

import state

logger = logging.getLogger(__name__)

# How long Redis is trusted to answer "is this account still active?".
CACHE_SECONDS = 60

CACHE_PREFIX = "nyx_accountstate_"

TRUE_WORDS = ("true", "1", "yes")


def is_inactive(usr):
    """Whether a `nyx_user` document — or the payload built from one — is disabled."""
    value = ((usr or {}).get("_source") or usr or {}).get("inactive", False)
    if isinstance(value, str):
        return value.strip().lower() in TRUE_WORDS
    return bool(value)


def cache_key(docid):
    return CACHE_PREFIX + str(docid)


def account_is_inactive(usr):
    """Whether the account behind a live token has been disabled since it signed in.

    Any failure — Redis down, document unreadable — is read as **active**: this check exists to
    close a door on an administrator's decision, not to lock the platform out on an outage.
    """
    docid = (usr or {}).get("docid") or (usr or {}).get("id")
    if not docid:
        # A session built without a document behind it (the AD path, a test harness): there is
        # no flag to read, and inventing one would keep those users out for ever.
        return False

    cached = _read_cache(docid)
    if cached is not None:
        return cached

    try:
        document = state.es.get(index="nyx_user", id=docid) or {}
    except Exception:
        logger.warning("Account state unreadable for %s, session kept", docid, exc_info=True)
        return False

    inactive = is_inactive(document)
    _write_cache(docid, inactive)
    return inactive


def forget(docid):
    """Drop the cached answer, so the next request reads the document again.

    Called by the routes that write an account: an administrator who disables someone from the
    console must not wait a minute for the session to end.
    """
    if not docid:
        return
    try:
        state.redisserver.delete(cache_key(docid))
    except Exception:
        logger.warning("Unable to forget the account state of %s", docid, exc_info=True)


def _read_cache(docid):
    try:
        value = state.redisserver.get(cache_key(docid))
    except Exception:
        return None
    if value is None:
        return None
    if isinstance(value, bytes):
        value = value.decode("utf-8", "ignore")
    return str(value) == "inactive"


def _write_cache(docid, inactive):
    try:
        state.redisserver.set(cache_key(docid), "inactive" if inactive else "active", CACHE_SECONDS)
    except Exception:
        # A cache that cannot be written is a slower check, not a broken one.
        logger.debug("Unable to cache the account state of %s", docid)
