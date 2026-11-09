"""
Authentication middleware: token_required, check_post_parameters decorators,
auth helper functions, and DateTimeEncoder.
"""

import hashlib
import json
import logging
from functools import wraps
from datetime import datetime

from flask import request, jsonify
from cachetools import cached, TTLCache

import state
from password_policy import must_change_password

logger = logging.getLogger()


BEARER_PREFIX = "bearer "

# The refusals of `token_required` are answered with an HTTP status, not only with the `error`
# field of a 200. Everything used to come back 200, so a probe, an nginx `limit_req`, a cache or
# an alert rule saw a healthy API while every call was being refused — the refusal lived only in
# the body, where nothing looks. The body keeps its shape (`{"error": "<code>"}`) so a client that
# reads it, like the console, keeps working.
AUTH_ERROR_STATUS = {
    "NO_TOKEN": 401,
    "UNKNOWN_TOKEN": 401,
    "MUST_CHANGE_PASSWORD": 403,
    "NO_PRIVILEGE": 403,
}

# Routes that may still receive the token as a query parameter: the console and the mobile app
# were built that way. The header is preferred, and the query form is what the logs of an
# operator reveal when a client has not been migrated.
def request_token(request):
    """Return ``(token, source)`` for the request, source being "header", "query" or None."""
    header = request.headers.get("Authorization", "")
    if header.lower().startswith(BEARER_PREFIX):
        token = header[len(BEARER_PREFIX):].strip()
        if token:
            return token, "header"
    token = request.args.get("token")
    if token:
        return token, "query"
    return None, None


def fingerprint(secret):
    """Short, non-reversible identifier for a secret. Lets a token be
    correlated across log lines without ever writing the secret itself."""
    if not secret:
        return "<none>"
    return hashlib.sha256(str(secret).encode("utf-8")).hexdigest()[:12]


class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, datetime.date, datetime.time)):
            return obj.isoformat()
        elif isinstance(obj, datetime.timedelta):
            return (datetime.min + obj).time().isoformat()
        return super(DateTimeEncoder, self).default(obj)


@cached(cache=TTLCache(maxsize=1024, ttl=60))
def getAPIKey(token):
    return state.es.get(index="nyx_apikey", id=token)
def checkAPIKey(request):
    """True when the request carries a valid machine key (a `nyx_apikey` document).

    A user token used to be accepted here as well, through `state.tokens` and the Redis
    session keys: the route documented as "the only one that accepts `?apikey=` instead of a
    token" therefore ran for any console session. A machine key outlives the person who
    configured it, a session token does not; they are two credentials with two lifetimes and
    this check now only knows the first.

    The key is an Elasticsearch document id, not a shared secret compared in memory: a
    missing document is a refusal, an unreachable cluster is a refusal too — an authorisation
    check cannot answer "yes" because the store did not.
    """
    token = request.args.get("apikey")
    if not token:
        return False
    try:
        return getAPIKey(token) is not None
    except Exception:
        logger.warning("API key lookup failed for " + fingerprint(token), exc_info=True)
        return False


def getUserFromToken(request, token=None, fresh=False):
    """Token payload; `fresh=True` ignores this worker's in-memory copy.

    Each gunicorn worker keeps its own copy of the payload for five minutes (`TTLCache`), and
    only a logout empties it earlier. A password change writes the new payload into Redis and
    into the worker that served the request: the other two therefore kept answering
    MUST_CHANGE_PASSWORD to a user who had just chosen their password, for up to five minutes
    and on one call in three (measured in production). Re-reading Redis at the moment of the
    refusal costs one read-only request, in the only throttled phase.
    """
    if token is None:
        token, _ = request_token(request)
    if token is None:
        return None
    with state.tokenlock:
        if not fresh and token in state.tokens:
            return state.tokens[token]
        try:
            redusr = state.redisserver.get("nyx_tok_" + token)
        except Exception:
            # Redis unreachable: better to keep the current session than to turn everyone
            # away; the in-memory copy is authoritative until the cache is back.
            logger.warning("Tokens: Redis unreachable, in-memory copy kept", exc_info=True)
            return state.tokens.get(token)
        if redusr is not None:
            logger.info("Retrieved user for token " + fingerprint(token) + " from redis.")
            redusrobj = json.loads(redusr)
            state.tokens[token] = redusrobj
            logger.info("Token reinitialized from redis cluster.")
            return redusrobj
        # The token no longer exists on the Redis side (logout done elsewhere): the local
        # copy is stale, we drop it rather than keep honouring it.
        state.tokens.pop(token, None)

    logger.info("Invalid Token:" + fingerprint(token))
    return None


def pushHistoryToELK(request, timespan, usr, token, error):
    rec = {
        "url": request.path,
        "method": request.method,
        "timespan": timespan,
        "user": usr["login"] if usr else "",
        "token": fingerprint(token),
        "error": error,
        "@timestamp": int(datetime.now().timestamp()) * 1000,
    }
    if "login" in request.path:
        agent = {
            "browser": request.user_agent.browser,
            "version": request.user_agent.version,
            "platform": request.user_agent.platform,
            "language": request.user_agent.language,
            "string": request.user_agent.string,
        }
        rec["agent"] = agent
    with state.userlock:
        state.userActivities.append(rec)


def check_post_parameters(*parameters):
    def wrapper(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            try:
                req = json.loads(request.data.decode("utf-8"))
                for param in parameters:
                    if param not in req:
                        return {'error': "MISSING_PARAM:" + param}
            except Exception as e:
                logger.error("Unable to decode body")
                return {'error': "UNABLE_TO_DECODE_BODY"}
            return f(*args, **kwargs)
        return decorated_function
    return wrapper


def token_required(*roles):
    def wrapper(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            logger.info(">>> START:" + request.path + ">>>>" + request.method)
            starttime = int(datetime.now().timestamp() * 1000)
            ret = None
            usr = None
            token, source = request_token(request)
            if token is None:
                ret = {'error': "NO_TOKEN"}
            else:
                if source == "query":
                    logger.debug(
                        "Token passed as a query parameter on %s; an Authorization: Bearer header "
                        "is preferred.", request.path
                    )
                usr = getUserFromToken(request, token)
                if usr is not None and must_change_password(usr, request.path):
                    # The flag may have been cleared by another worker: we re-read Redis
                    # before refusing. Without that, a user who has just chosen their
                    # password stays throttled for up to five minutes on the workers that
                    # had cached the throttled token — and cannot understand why it is
                    # asked for again.
                    refreshed = getUserFromToken(request, token, fresh=True)
                    if refreshed is not None:
                        usr = refreshed
                if usr is None:
                    ret = {'error': "UNKNOWN_TOKEN"}
                elif must_change_password(usr, request.path):
                    # Provisional password: the token only opens the password change and
                    # the logout. This is not a courtesy of the interface.
                    ret = {'error': "MUST_CHANGE_PASSWORD"}
                else:
                    ok = False
                    if len(roles) == 0:
                        ok = True
                    elif "admin" in usr["privileges"]:
                        ok = True
                    else:
                        for priv in usr["privileges"]:
                            if priv in roles:
                                ok = True
                                break
                    if ok:
                        kwargs["user"] = usr
                        ret = f(*args, **kwargs)
                    else:
                        ret = {'error': "NO_PRIVILEGE"}

            endtime = int(datetime.now().timestamp() * 1000)
            timespan = endtime - starttime
            logger.info("<<< FINISH:" + request.path)

            error = ''
            if isinstance(ret, dict):
                error = ret.get("error", "")

            state.requests_total += 1
            if error:
                state.errors_total += 1

            if token is not None:
                pushHistoryToELK(request, timespan, usr, token, error)

            if not isinstance(ret, dict):
                return ret

            ret["timespan"] = timespan
            response = jsonify(ret)
            # Only the refusals of this decorator carry a status: a route that answers
            # `{"error": "unable to post data"}` keeps its 200, because the console reads that
            # body in `.then()` and a client that lost the habit would show nothing at all.
            status = AUTH_ERROR_STATUS.get(error)
            if status is not None:
                response.status_code = status
                if status == 401:
                    response.headers["WWW-Authenticate"] = 'Bearer realm="nyx"'
            return response
        return decorated_function
    return wrapper
