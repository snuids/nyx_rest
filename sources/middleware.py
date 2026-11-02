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
    if state.elkversion >= 7:
        return state.es.get(index="nyx_apikey", id=token)
    else:
        return state.es.get(index="nyx_apikey", id=token, doc_type="_doc")


def checkAPIKey(request):
    if "apikey" not in request.args:
        return False

    token = request.args.get('apikey')
    try:
        api = getAPIKey(token)
        if api is not None:
            return True
    except:
        pass

    with state.tokenlock:
        if token in state.tokens:
            return True
        redusr = state.redisserver.get("nyx_tok_" + token)
        if redusr is not None:
            return True

    return False


def getUserFromToken(request, token=None):
    if token is None:
        token, _ = request_token(request)
    if token is None:
        return None
    with state.tokenlock:
        if token in state.tokens:
            return state.tokens[token]
        redusr = state.redisserver.get("nyx_tok_" + token)
        if redusr is not None:
            logger.info("Retrieved user for token " + fingerprint(token) + " from redis.")
            redusrobj = json.loads(redusr)
            state.tokens[token] = redusrobj
            logger.info("Token reinitialized from redis cluster.")
            return redusrobj

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
                if usr is None:
                    ret = {'error': "UNKNOWN_TOKEN"}
                elif must_change_password(usr, request.path):
                    # Mot de passe provisoire : le jeton n'ouvre que le changement de mot de
                    # passe et la déconnexion. Ce n'est pas une politesse de l'interface.
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
            if type(ret) == dict:
                error = ret["error"]

            state.requests_total += 1
            if error:
                state.errors_total += 1

            if token is not None:
                pushHistoryToELK(request, timespan, usr, token, error)

            if type(ret) != dict:
                return ret

            ret["timespan"] = timespan
            return jsonify(ret)
        return decorated_function
    return wrapper
