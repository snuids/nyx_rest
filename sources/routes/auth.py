"""
Authentication routes: OAuth, Google login, standard login, logout,
reset/change password, and login helper functions.
"""

import json
import hmac
import uuid
import random
import logging
import secrets

import httplib2
import requests
from oauth2client import client
from passlib.hash import pbkdf2_sha256
from flask import request, jsonify, make_response
from flask_restx import Resource, fields
from datetime import datetime, timezone

import state
from config import settings
from password_policy import check_new_password
from middleware import (
    token_required, check_post_parameters, pushHistoryToELK, fingerprint, request_token
)
from kibana_helpers import computeMenus
from common import loadData
from auth.auth_ad import authenticate_ad
from auth.double_phase import verify_double_code
from auth.role_mapper import extract_roles_from_ad
import login_guard as guard
from lib import github_oauth
from es_read import find_user_by_login, fetch_document

logger = logging.getLogger()


def record_auth_failure(login, ip, reason, user_agent=""):
    """Records an authentication failure in ``nyx_authfail``.

    Without that record, a brute-force attack is invisible: the call has no token yet, so it
    appears neither in ``nyx_apicalls`` nor in the application metrics.
    """
    try:
        doc = {
            "@timestamp": datetime.utcnow().isoformat() + "Z",
            "login": login,
            "ip": ip,
            "reason": reason,
            "user_agent": (user_agent or "")[:300],
        }
        # Dated index, like nyx_apicalls: the curator expires it beyond seven days. An
        # attempted login, an IP and a User-Agent are personal data; they must not survive
        # indefinitely in a single index that keeps on growing.
        index = "nyx_authfail-" + datetime.utcnow().strftime("%Y.%m.%d")
        try:
            state.es.index(index=index, document=doc)        # elasticsearch-py 8
        except TypeError:
            # the client actually installed is opensearchpy, which expects body=
            state.es.index(index=index, body=doc)
    except Exception:
        logger.warning("authentication failure not recorded", exc_info=True)


OAUTH_STATE_TTL = 600        # ten minutes between the redirect and the callback


def github_configured():
    """True when the three settings the sign-in needs are present."""
    return bool(settings.GITHUB_CLIENT_ID and settings.GITHUB_CLIENT_SECRET
                and settings.GITHUB_REDIRECT_URI)


def find_account(es, login, email):
    """The `nyx_user` document behind a GitHub identity, or `None`.

    The same pair of lookups as the password route: by document id first, then by
    `login.keyword` — eight accounts are indexed under their e-mail while their `login` field holds
    a short name. GitHub is not asked twice for the same answer.
    """
    for candidate in github_oauth.candidates(login, email):
        try:
            return es.get(index="nyx_user", id=candidate)
        except Exception:
            pass
        hit = find_user_by_login(es, candidate)
        if hit is not None:
            return hit
    return None


def github_unknown_account_policy():
    """`refuse` (default) or `inactive`.

    Anything else is read as `refuse`: a misspelt value must not open a door, and `refuse` is what
    the platform did before the setting existed.
    """
    return str(getattr(settings, "GITHUB_UNKNOWN_ACCOUNTS", "refuse") or "").strip().lower()


def provision_inactive_account(es, profile):
    """Files a disabled, privilege-less `nyx_user` for a GitHub identity nobody knows.

    The account cannot sign in — `finalize_login` refuses it, so the caller ends up answering
    `ACCOUNT_INACTIVE` — which means creating it grants nothing. It exists so an administrator sees
    the request in the users list: activate it, grant the privileges, and the person then signs in
    with GitHub without ever holding a password on this platform.

    The document keeps the shape the console expects: `_id` is the address when GitHub gives a
    verified one, `login` the short name. The name comes from the profile, and falls back on the
    login when the person never filled it; the phone is left out because GitHub has no such field —
    an administrator completes it when they call back.
    """
    login = profile.get("login") or ""
    email = profile.get("email") or ""
    document = {
        "login": login or email,
        "firstname": profile.get("firstname") or login,
        "lastname": profile.get("lastname") or "",
        "email": email,
        "privileges": [],
        "filters": [],
        "language": "en",
        "inactive": True,
        "source": "github",
        "created": datetime.now(timezone.utc).isoformat(),
    }
    document_id = email or login
    try:
        es.index(index="nyx_user", id=document_id, document=document, refresh="wait_for")
    except TypeError:
        # elasticsearch-py 7 names the same argument `body`.
        es.index(index="nyx_user", id=document_id, body=document, refresh="wait_for")
    return {"_id": document_id, "_index": "nyx_user", "_source": document}


def setACookie(privilege, privileges, resp, token):
    if "admin" in privileges or (len(privileges) > 0 and privilege in privileges):
        state.redisserver.set("nyx_" + privilege.lower() + "_" + str(token), "OK", 3600 * 24)
        logger.info("Setting cookie for " + privilege)
        logger.info("Token fingerprint " + fingerprint(token))
        a = resp.set_cookie('nyx_' + privilege.lower(), str(token),
                            secure=state.COOKIESECURE, httponly=True)
        logger.info(a)


def user_document_id(user):
    """The `nyx_user` document id behind a token payload.

    `id` in a payload is the string typed in the login form, which is not always the document
    id: eight production accounts are indexed under their address while their `login` field
    carries a short name (`amarchand@icloud.com` -> `amarchand`), and the login form accepts
    either. Writing a preference on `user["id"]` then answers 404
    `document_missing_exception` — what the console showed as `storage_failed`. `docid`, set
    at login, carries the document id from now on; a token issued before that is resolved by
    the same by-login fallback the login route uses, so an open session needs no re-login.
    """
    if not user:
        return None
    if user.get("docid"):
        return user["docid"]
    candidate = user.get("id") or user.get("login")
    error, document = fetch_document(state.es, "nyx_user", candidate)
    if error == "" and document and document.get("_id"):
        return document["_id"]
    return candidate


def is_inactive(usr):
    """Whether an account is disabled.

    The field is opt-in: a `nyx_user` document that does not carry `inactive` is an **active**
    account, so nothing had to be migrated when the flag was introduced. A hand-written document
    may carry `"true"` or `1` rather than a boolean, hence the tolerant reading.
    """
    value = ((usr or {}).get("_source") or {}).get("inactive", False)
    if isinstance(value, str):
        return value.strip().lower() in ("true", "1", "yes")
    return bool(value)


def finalize_login(usr, data, es, conn):
    """Mint the session, or refuse it.

    An inactive account never gets a token — this is the single place every sign-in path goes
    through (password, GitHub, Google), which is why the check lives here rather than in each route.
    The console turns the `ACCOUNT_INACTIVE` answer into the page that explains what to do.
    """
    if is_inactive(usr):
        logger.warning("Sign-in refused, account inactive: %s",
                       (usr.get("_source") or {}).get("login"))
        state.auth_blocked_total += 1
        return jsonify({'error': 'ACCOUNT_INACTIVE'})

    token = uuid.uuid4()

    with state.tokenlock:
        state.tokens[str(token)] = usr["_source"]

    usr["_source"]["password"] = ""
    usr["_source"]["id"] = data["login"]
    # Kept apart from `id`, which stays what the user typed: the preference and password
    # routes address a document, and that document is not always indexed under the login.
    usr["_source"]["docid"] = usr.get("_id") or usr["_source"]["id"]

    try:
        state.redisserver.set("nyx_tok_" + str(token), json.dumps(usr["_source"]), 3600 * 24)
    except:
        logger.error("Unable to set redis token for " + fingerprint(token), exc_info=True)
        raise Exception("Unable to set redis token for " + fingerprint(token))

    apptag = data.get("app", "console")
    finalcategory = computeMenus(usr, str(token), apptag)

    all_priv, all_filters = [], []
    if "admin" in usr["_source"]["privileges"] or "user" in usr["_source"]["privileges"]:
        all_priv = loadData(es, conn, 'nyx_privilege', {}, 'doc', False,
                            (None, None, None), True, usr['_source'], None, None, None)['records']
        all_filters = loadData(es, conn, 'nyx_filter', {}, 'doc', False,
                               (None, None, None), True, usr['_source'], None, None, None)['records']

    resp = make_response(jsonify({
        'version': state.VERSION,
        'error': "",
        'cred': {'token': token, 'user': usr["_source"]},
        "menus": finalcategory,
        "all_priv": all_priv,
        "all_filters": all_filters,
    }))
    resp.set_cookie('nyx_kibananyx', str(token), secure=state.COOKIESECURE, httponly=True)
    resp.set_cookie('nyx_grafananyx', str(token), secure=state.COOKIESECURE, httponly=True)
    state.redisserver.set("nyx_grafananyx_" + str(token), "OK", 3600 * 24)

    for app in ["nodered", "anaconda", "cerebro", "grafana", "kibana", "logs", "amqc", "redis"]:
        setACookie(app, usr["_source"]["privileges"], resp, token)

    pushHistoryToELK(request, 0, usr["_source"], str(token), "")
    return resp


def register(api, name_space):

    loginGoogleAPI = api.model('login_google_model', {
        'auth_code': fields.String(description="The google auth code.", required=True),
        'app': fields.String(description="The app tag.", required=False),
    })

    @name_space.route('/cred/oauth/state', methods=['GET'])
    @api.doc(description="Start a GitHub sign-in: the URL to open, and the state cookie that goes "
                         "with it.")
    class loginOAuthState(Resource):
        def get(self):
            """The browser is sent to GitHub with a one-time `state`, kept here for ten minutes.

            The state is what stops the callback from being a login CSRF: an attacker who prepares
            a link with *their* code cannot know the value this browser was given, and the value is
            also written to an HttpOnly cookie, so a state obtained elsewhere does not work here.
            """
            if not github_configured():
                logger.info("GitHub sign-in asked for but not configured")
                return {'error': 'github login not configured'}

            oauth_state = secrets.token_urlsafe(24)
            try:
                state.redisserver.set("nyx_oauth_state_" + oauth_state, "1", OAUTH_STATE_TTL)
            except Exception:
                logger.error("Unable to store the GitHub sign-in state", exc_info=True)
                return {'error': 'unable to start the sign-in'}

            try:
                url = github_oauth.authorize_url(settings.GITHUB_CLIENT_ID,
                                                 settings.GITHUB_REDIRECT_URI, oauth_state)
            except ValueError:
                return {'error': 'github login not configured'}

            response = make_response(jsonify({'error': '', 'url': url}))
            response.set_cookie('nyx_oauth_state', oauth_state, max_age=OAUTH_STATE_TTL,
                                secure=state.COOKIESECURE, httponly=True, samesite='Lax')
            return response

    githubLoginAPI = api.model('github_login_model', {
        'code': fields.String(description="The code GitHub put on the redirect.", required=True),
        'state': fields.String(description="The state returned with it.", required=True),
        'app': fields.String(description="The app tag.", required=False),
    })

    @name_space.route('/cred/login_github', methods=['POST'])
    @api.doc(description="Sign in with GitHub. Only an existing account may sign in.")
    @api.expect(githubLoginAPI)
    class loginGithubRest(Resource):
        def post(self):
            logger.info(">> LOGIN IN GITHUB")
            try:
                data = json.loads(request.data.decode("utf-8"))
            except ValueError:
                return {'error': 'Bad Request'}
            if not isinstance(data, dict):
                return {'error': 'Bad Request'}

            code = data.get('code') or ''
            provided = data.get('state') or ''
            ip = guard.client_ip(request.headers, request.remote_addr)
            agent = request.headers.get("User-Agent")

            if not code or not provided or not github_configured():
                return {'error': 'Bad Credentials'}

            # One use, one browser: the state must be the one this browser was given.
            try:
                stored = state.redisserver.get("nyx_oauth_state_" + provided)
                state.redisserver.delete("nyx_oauth_state_" + provided)
            except Exception:
                logger.error("Unable to verify the GitHub sign-in state", exc_info=True)
                return {'error': 'unable to verify the sign-in'}
            cookie = request.cookies.get('nyx_oauth_state', '')
            if stored is None or not hmac.compare_digest(str(provided), str(cookie)):
                record_auth_failure("github", ip, "OAUTH_STATE", agent)
                state.auth_failures_total += 1
                guard.delay()
                return {'error': 'Bad Credentials'}

            try:
                with requests.Session() as session:
                    token = github_oauth.exchange_code(
                        session, settings.GITHUB_CLIENT_ID, settings.GITHUB_CLIENT_SECRET,
                        code, settings.GITHUB_REDIRECT_URI,
                    )
                    profile = github_oauth.fetch_profile(session, token)
                    login, email = profile["login"], profile["email"]
            except github_oauth.GithubError:
                record_auth_failure("github", ip, "GITHUB_EXCHANGE", agent)
                state.auth_failures_total += 1
                guard.delay()
                return {'error': 'Bad Credentials'}

            # The token has done its work; it is not stored anywhere and never becomes the session
            # token: the platform mints its own (finalize_login).
            usr = find_account(state.es, login, email)
            if usr is None:
                if github_unknown_account_policy() == "inactive":
                    # An account is filed, disabled and without privileges: `finalize_login` refuses
                    # it, so nothing is granted — the administrator decides afterwards.
                    usr = provision_inactive_account(state.es, profile)
                    logger.info("GitHub sign-in for %s (%s): disabled account created for %s %s, "
                                "no privileges; published e-mail %s",
                                login, email or "no verified e-mail",
                                profile.get("firstname") or "-", profile.get("lastname") or "",
                                profile.get("public_email") or "none")
                    return finalize_login(usr, {'login': usr['_source']['login'],
                                                'app': data.get('app', 'console')},
                                          state.es, state.conn)
                # Only an existing account signs in: GitHub says who you are, `nyx_user` says what
                # you may open, and an unknown account has no privileges to grant.
                logger.warning("GitHub sign-in for %s (%s): no NYX account", login, email or "no e-mail")
                record_auth_failure(login, ip, "NO_ACCOUNT_GITHUB", agent)
                state.auth_failures_total += 1
                guard.delay()
                return {'error': 'Bad Credentials'}

            account = usr["_source"]
            logger.info("GitHub sign-in: %s -> %s", login, account.get("login"))
            return finalize_login(usr, {'login': account.get('login', login),
                                        'app': data.get('app', 'console')},
                                  state.es, state.conn)

    @name_space.route('/cred/login_google', methods=['POST'])
    class loginGoogleRest(Resource):
        @api.doc(description="Google login function.")
        @api.expect(loginGoogleAPI)
        def post(self):
            logger.info(">> LOGIN IN GOOGLE")
            try:
                data = json.loads(request.data.decode("utf-8"))
                auth_code = data['auth_code']

                credentials = client.credentials_from_clientsecrets_and_code(
                    settings.CLIENT_SECRET_FILE,
                    ['profile', 'email'],
                    auth_code,
                )

                http_auth = credentials.authorize(httplib2.Http())
                cleanlogin = credentials.id_token['email']

                try:
                    usr = state.es.get(index="nyx_user", id=cleanlogin)
                except:
                    logger.info("Not found")
                    usr = None
                    logger.info("Searching by login")
                    body = {
                        "size": "100",
                        "query": {
                            "bool": {
                                "must": [{
                                    "term": {
                                        "login.keyword": {"value": cleanlogin, "boost": 1}
                                    }
                                }]
                            }
                        },
                    }
                    users = state.es.search(index="nyx_user", body=body)
                    if "hits" in users and "hits" in users["hits"] and \
                            len(users["hits"]["hits"]) > 0:
                        usr = users["hits"]["hits"][0]

                logger.info("USR_" * 20)
                logger.info((usr or {}).get("_source", {}).get("login", "unknown"))

                if usr is None:
                    return jsonify({'error': "Bad Credentials"})

                token = credentials.access_token

                with state.tokenlock:
                    state.tokens[str(token)] = usr["_source"]

                usr["_source"]["password"] = ""
                usr["_source"]["id"] = cleanlogin
                usr["_source"]["docid"] = usr.get("_id") or cleanlogin

                state.redisserver.set("nyx_tok_" + str(token), json.dumps(usr["_source"]), 3600 * 1)

                apptag = data.get("app", "console")
                finalcategory = computeMenus(usr, str(token), apptag)

                all_priv, all_filters = [], []
                if "admin" in usr["_source"]["privileges"] or "user" in usr["_source"]["privileges"]:
                    all_priv = loadData(
                        state.es, state.conn, 'nyx_privilege', {}, 'doc', False,
                        (None, None, None), True, usr['_source'], None, None, None
                    )['records']
                    all_filters = loadData(
                        state.es, state.conn, 'nyx_filter', {}, 'doc', False,
                        (None, None, None), True, usr['_source'], None, None, None
                    )['records']

                resp = make_response(jsonify({
                    'version': state.VERSION,
                    'error': "",
                    'cred': {'token': token, 'user': usr["_source"]},
                    "menus": finalcategory,
                    "all_priv": all_priv,
                    "all_filters": all_filters,
                }))
                resp.set_cookie('nyx_kibananyx', str(token),
                                secure=state.COOKIESECURE, httponly=True)
                resp.set_cookie('nyx_grafananyx', str(token),
                                secure=state.COOKIESECURE, httponly=True)

                for app in ["nodered", "anaconda", "cerebro", "grafana",
                            "kibana", "logs", "amqc", "redis"]:
                    setACookie(app, usr["_source"]["privileges"], resp, token)
                pushHistoryToELK(request, 0, usr["_source"], str(token), "")
                return resp

            except Exception as e:
                logger.error("Unable to verify auth code.")
                logger.error(e)
                return jsonify({'error': "Bad Request"})

    loginAPI = api.model('login_model', {
        'login': fields.String(description="The user login", required=True),
        'password': fields.String(description="The user password.", required=True),
        'app': fields.String(description="The app tag.", required=False),
    })

    @name_space.route('/cred/login', methods=['POST'])
    class loginRest(Resource):
        @api.doc(description="login function.")
        @api.expect(loginAPI)
        def post(self):
            logger.info(">> LOGIN IN")
            data = json.loads(request.data.decode("utf-8"))

            if ("login" in data) and ("password" in data):
                cleanlogin = data["login"].split(">")[0]

                # Brute force: Redis counters (account + IP) checked before any password
                # verification, so that a refusal costs one request and not a pbkdf2
                # computation — and an attacker cannot saturate the workers.
                ip = guard.client_ip(request.headers, request.remote_addr)
                wait = guard.retry_after(state.redisserver, cleanlogin, ip)
                if wait > 0:
                    state.auth_blocked_total += 1
                    record_auth_failure(cleanlogin, ip, "LOCKED", request.headers.get("User-Agent"))
                    resp = jsonify({"error": "TOO_MANY_ATTEMPTS", "retry_after": int(wait)})
                    resp.status_code = 429
                    resp.headers["Retry-After"] = str(int(wait))
                    return resp

                try:
                    usr = state.es.get(index="nyx_user", id=cleanlogin)
                except:
                    logger.info("Not found", exc_info=True)
                    usr = None
                    logger.info("Searching by login")
                    body = {
                        "size": "100",
                        "query": {
                            "bool": {
                                "must": [{
                                    "term": {
                                        "login.keyword": {"value": cleanlogin, "boost": 1}
                                    }
                                }]
                            }
                        },
                    }
                    users = state.es.search(index="nyx_user", body=body)
                    if "hits" in users and "hits" in users["hits"] and \
                            len(users["hits"]["hits"]) > 0:
                        usr = users["hits"]["hits"][0]

                logger.info("USR_" * 20)
                logger.info((usr or {}).get("_source", {}).get("login", "unknown"))

                if usr is not None and pbkdf2_sha256.verify(
                        data["password"], usr["_source"]["password"]):

                    guard.reset(state.redisserver, cleanlogin, ip)

                    if usr["_source"].get("doublePhase", False) is True:
                        if "doublecode" in data:
                            logger.info("Must check code")
                            if not verify_double_code(
                                state.redisserver, data["login"], data["doublecode"]
                            ):
                                # A five-digit code can be guessed: the second factor
                                # error counts as a failure, not as an incident.
                                guard.register_failure(state.redisserver, cleanlogin, ip)
                                state.auth_failures_total += 1
                                record_auth_failure(cleanlogin, ip, "DOUBLE_PHASE",
                                                    request.headers.get("User-Agent"))
                                guard.delay()
                                return jsonify({'error': "ErrorDoublePhase"})
                        else:
                            randint = "" + str(random.randint(10000, 99999))
                            state.redisserver.set("nyx_double_" + data["login"], randint, 120)
                            state.conn.send_message(
                                "/topic/AUTH_SMS",
                                json.dumps({
                                    "message": "Your access code is:" + randint,
                                    "phone": usr["_source"]["phone"],
                                }),
                            )
                            return jsonify({'error': "DoublePhase"})

                    if ">" in data["login"] and "admin" in usr["_source"]["privileges"]:
                        otheruser = data["login"].split(">")[1]
                        try:
                            usr = state.es.get(index="nyx_user", id=otheruser)
                        except:
                            usr = None
                            return jsonify({'error': "Unknown User"})

                    return finalize_login(usr, data, state.es, state.conn)

                else:
                    success, ad_info = authenticate_ad(cleanlogin, data["password"])

                    if success:
                        privileges = extract_roles_from_ad(ad_info)
                        logger.info(f"User {cleanlogin} authenticated via AD")
                        usr = {
                            "_source": {
                                "login": cleanlogin,
                                "password": "",
                                "privileges": privileges if privileges else ["user"],
                                "firstname": ad_info.get("givenName", [""])[0]
                                if ad_info.get("givenName") else "User",
                                "language": "en",
                                "conexion_source": "ad",
                                "id": cleanlogin,
                                "cn": ad_info.get("cn", [""])[0] if ad_info.get("cn") else "",
                                "mail": ad_info.get("mail", [""])[0]
                                if ad_info.get("mail") else "",
                            }
                        }
                        logger.info("AD user authenticated: " + cleanlogin)
                        return finalize_login(usr, data, state.es, state.conn)
                    else:
                        logger.info(f"Authentication failed for user {cleanlogin}")
                        guard.register_failure(state.redisserver, cleanlogin, ip)
                        state.auth_failures_total += 1
                        record_auth_failure(cleanlogin, ip, "BAD_CREDENTIALS",
                                            request.headers.get("User-Agent"))
                        # The time floor makes "unknown account" and "wrong password"
                        # indistinguishable, whichever path is taken (local or AD).
                        guard.delay()
                        return jsonify({'error': guard.failure_message()})

            return jsonify({'error': "Bad Request"})

    @name_space.route('/cred/logout')
    class logout(Resource):
        @token_required()
        @api.doc(description="Log the user out.", params={'token': 'A valid token'})
        def get(self, user=None):
            logger.info(">>> Logout")
            token, _ = request_token(request)
            state.redisserver.delete("nyx_tok_" + str(token))
            state.redisserver.delete("nyx_nodered_" + str(token))
            state.redisserver.delete("nyx_cerebro_" + str(token))
            state.redisserver.delete("nyx_redis_" + str(token))
            state.redisserver.delete("nyx_amqc_" + str(token))
            state.redisserver.delete("nyx_grafana_" + str(token))
            state.redisserver.delete("nyx_kibana_" + str(token))
            state.redisserver.delete("nyx_anaconda_" + str(token))
            state.redisserver.delete("nyx_logs_" + str(token))
            if token in state.tokens:
                del state.tokens[token]
            state.conn.send_message("/topic/LOGOUT_EVENT", token)
            return {"error": ""}

    reset_passwordAPI = api.model('reset_password_model', {
        'login': fields.String(description="The user login", required=True),
        'new_password': fields.String(description="The user password.", required=True),
    })

    @name_space.route('/cred/resetpassword')
    class reset_password(Resource):
        @token_required("admin", "useradmin")
        @check_post_parameters("login", "new_password")
        @api.doc(description="Resets a user password.", params={'token': 'A valid token'})
        @api.expect(reset_passwordAPI)
        def post(self, user=None):
            logger.info(">>> Reset password")
            req = json.loads(request.data.decode("utf-8"))
            try:
                usrdb = state.es.get(index="nyx_user", id=req["login"])
            except:
                return {"error": "usernotfound"}

            usrdb["_source"]["password"] = pbkdf2_sha256.hash(req["new_password"])
            # A password set by an administrator is provisional: its holder must choose
            # one at their first login.
            usrdb["_source"]["mustResetPassword"] = True
            state.es.index(index="nyx_user", body=usrdb["_source"], id=req["login"])
            usrdb["_source"]["id"] = usrdb["_id"]

            if "queue" in req:
                state.conn.send_message(
                    req["queue"],
                    json.dumps({
                        "byuser": user,
                        "foruser": usrdb["_source"],
                        "newpassword": req["new_password"],
                    }),
                )

            return {"error": ""}

    change_passwordAPI = api.model('change_password_model', {
        'old_password': fields.String(description="The user old password", required=True),
        'new_password': fields.String(description="The user password.", required=True),
    })

    preferencesAPI = name_space.model('Preferences', {
        'theme': fields.String(description="UI theme: light, dark or auto (follows the hour).", required=False),
    })

    @name_space.route('/cred/preferences')
    class preferences(Resource):
        """Per-user UI preferences, stored on the user's own document.

        The user document already carries `language` and `privileges`; the theme joins them so a
        preference follows the account from one browser to the next. Only the caller's own record is
        ever touched, which is why this does not need the admin privilege that `nyx_user` writes
        require."""

        @token_required()
        @api.doc(description="Save the caller's UI preferences.", params={'token': 'A valid token'})
        @api.expect(preferencesAPI)
        def post(self, user=None):
            logger.info(">>> Save preferences")
            try:
                req = json.loads(request.data.decode("utf-8"))
            except ValueError:
                return {"error": "invalid_json"}, 400
            if not isinstance(req, dict):
                return {"error": "invalid_json"}, 400

            allowed = {"theme": ("light", "dark", "auto")}
            updates = {}
            for key, accepted in allowed.items():
                if key not in req:
                    continue
                value = req[key]
                if not isinstance(value, str) or value not in accepted:
                    return {"error": "invalid_value", "field": key,
                            "accepted": list(accepted)}, 400
                updates[key] = value
            if not updates:
                return {"error": "nothing_to_save"}, 400

            try:
                docid = user_document_id(user)
                state.es.update(index="nyx_user", id=docid, body={"doc": updates})
            except Exception as e:
                logger.error("Unable to save preferences for %s: %s", user.get("id"), e)
                return {"error": "storage_failed"}, 500
            logger.info("Preferences saved for %s: %s", user.get("id"), ",".join(updates))
            return {"error": "", "preferences": updates}

    @name_space.route('/cred/changepassword')
    class change_password(Resource):
        @token_required()
        @check_post_parameters("old_password", "new_password")
        @api.doc(description="Change an user password.", params={'token': 'A valid token'})
        @api.expect(change_passwordAPI)
        def post(self, user=None):
            logger.info(">>> Change password")
            req = json.loads(request.data.decode("utf-8"))
            logger.info("Change password request fields: " + ",".join(req.keys()))
            logger.info("Change password for user: " + str(user.get("id", "")))
            docid = user_document_id(user)
            usrdb = state.es.get(index="nyx_user", id=docid)
            policy_error = check_new_password(req["new_password"], user.get("login", ""))
            if policy_error:
                return {"error": policy_error}
            if pbkdf2_sha256.verify(req["old_password"], usrdb["_source"]["password"]):
                usrdb["_source"]["password"] = pbkdf2_sha256.hash(req["new_password"])
                usrdb["_source"]["mustResetPassword"] = False
                res = state.es.index(index="nyx_user", body=usrdb["_source"], id=docid)
                logger.info(res)
                # The current token stops being throttled: both copies are updated (worker
                # memory and Redis), otherwise the user stays blocked until they log in
                # again.
                token, _ = request_token(request)
                if token:
                    with state.tokenlock:
                        if token in state.tokens:
                            state.tokens[token]["mustResetPassword"] = False
                    payload = state.tokens.get(token)
                    if payload is not None:
                        state.redisserver.set("nyx_tok_" + str(token), json.dumps(payload), 3600 * 24)
                return {"error": ""}
            else:
                return {"error": "wrongpassword"}
