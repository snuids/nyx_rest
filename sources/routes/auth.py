"""
Authentication routes: OAuth, Google login, standard login, logout,
reset/change password, and login helper functions.
"""

import json
import uuid
import random
import logging

import httplib2
import requests
import jwt
from oauth2client import client
from passlib.hash import pbkdf2_sha256
from flask import request, jsonify, make_response
from flask_restx import Resource, fields
from datetime import datetime, timedelta

import state
from config import settings
from middleware import token_required, check_post_parameters, pushHistoryToELK
from kibana_helpers import computeMenus
from common import loadData
from auth.auth_ad import authenticate_ad
from auth.role_mapper import extract_roles_from_ad

logger = logging.getLogger()


def setACookie(privilege, privileges, resp, token):
    if "admin" in privileges or (len(privileges) > 0 and privilege in privileges):
        state.redisserver.set("nyx_" + privilege.lower() + "_" + str(token), "OK", 3600 * 24)
        logger.info("Setting cookie for " + privilege)
        logger.info(str(token))
        a = resp.set_cookie('nyx_' + privilege.lower(), str(token),
                            secure=state.COOKIESECURE, httponly=True)
        logger.info(a)


def finalize_login(usr, data, es, conn):
    token = uuid.uuid4()

    with state.tokenlock:
        state.tokens[str(token)] = usr["_source"]

    usr["_source"]["password"] = ""
    usr["_source"]["id"] = data["login"]

    try:
        state.redisserver.set("nyx_tok_" + str(token), json.dumps(usr["_source"]), 3600 * 24)
    except:
        logger.error("Unable to set redis token for " + str(token), exc_info=True)
        raise Exception("Unable to set redis token for " + str(token))

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

    @name_space.route('/cred/oauth/<string:action>/<string:social>', methods=['POST'])
    class loginOAuthRest(Resource):
        def post(self, action, social):
            logger.info(">> OAUTH LOGIN IN")
            data = json.loads(request.data.decode("utf-8"))

            post_data = {
                "client_secret": "2997acd1b285d45551ecf6606f53b98b8246717b",
                "client_id": data["clientId"],
                "code": data["code"],
            }

            r = requests.post('https://github.com/login/oauth/access_token', data=post_data)
            logger.info(r.text)
            dict_ = {x[0]: x[1] for x in [x.split("=") for x in r.text.split("&")]}

            r2 = requests.get('https://api.github.com/user?access_token=' + dict_["access_token"])
            logger.info(r2.text)

            r3 = requests.get(
                'https://api.github.com/user/emails?access_token=' + dict_["access_token"]
            )
            logger.info(r3.text)

            token = jwt.encode(
                {
                    'sub': "amarchand@icloud.com",
                    'iat': datetime.utcnow(),
                    'exp': datetime.utcnow() + timedelta(minutes=30),
                },
                "2997acd1b285d45551ecf6606f53b98b8246717b",
            )

            return jsonify({
                'id': 1,
                'name': "Arnaud Marchand",
                'email': "amarchand@icloud.com",
                'created_at': datetime.utcnow(),
                'access_token': dict_["access_token"],
            })

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
                    if state.elkversion >= 7:
                        usr = state.es.get(index="nyx_user", id=cleanlogin)
                    else:
                        usr = state.es.get(index="nyx_user", doc_type="doc", id=cleanlogin)
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
                    if state.elkversion >= 7:
                        users = state.es.search(index="nyx_user", body=body)
                    else:
                        users = state.es.search(index="nyx_user", doc_type="doc", body=body)
                    if "hits" in users and "hits" in users["hits"] and \
                            len(users["hits"]["hits"]) > 0:
                        usr = users["hits"]["hits"][0]

                logger.info("USR_" * 20)
                logger.info(usr)

                if usr is None:
                    return jsonify({'error': "Bad Credentials"})

                token = credentials.access_token

                with state.tokenlock:
                    state.tokens[str(token)] = usr["_source"]

                usr["_source"]["password"] = ""
                usr["_source"]["id"] = cleanlogin

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

                try:
                    if state.elkversion >= 7:
                        usr = state.es.get(index="nyx_user", id=cleanlogin)
                    else:
                        usr = state.es.get(index="nyx_user", doc_type="doc", id=cleanlogin)
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
                    if state.elkversion >= 7:
                        users = state.es.search(index="nyx_user", body=body)
                    else:
                        users = state.es.search(index="nyx_user", doc_type="doc", body=body)
                    if "hits" in users and "hits" in users["hits"] and \
                            len(users["hits"]["hits"]) > 0:
                        usr = users["hits"]["hits"][0]

                logger.info("USR_" * 20)
                logger.info(usr)

                if usr is not None and pbkdf2_sha256.verify(
                        data["password"], usr["_source"]["password"]):

                    if usr["_source"].get("doublePhase", False) is True:
                        if "doublecode" in data:
                            logger.info("Must check code")
                            codeindb = state.redisserver.get("nyx_double_" + data["login"])
                            if codeindb is not None:
                                codeindb = codeindb.decode("ascii")
                            logger.info("In redis:")
                            logger.info(codeindb)
                            logger.info(data["doublecode"])
                            if str(codeindb) != data["doublecode"]:
                                state.redisserver.delete("nyx_double_" + data["login"])
                                return jsonify({'error': "ErrorDoublePhase"})
                        else:
                            randint = "" + str(random.randint(10000, 99999))
                            state.redisserver.set("nyx_double_" + data["login"], randint, 120)
                            logger.info("Code is " + randint)
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
                            if state.elkversion >= 7:
                                usr = state.es.get(index="nyx_user", id=otheruser)
                            else:
                                usr = state.es.get(index="nyx_user", doc_type="doc", id=otheruser)
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
                        logger.info(usr)
                        return finalize_login(usr, data, state.es, state.conn)
                    else:
                        logger.info(f"Authentication failed for user {cleanlogin}")
                        return jsonify({'error': "Bad Credentials"})

            return jsonify({'error': "Bad Request"})

    @name_space.route('/cred/logout')
    class logout(Resource):
        @token_required()
        @api.doc(description="Log the user out.", params={'token': 'A valid token'})
        def get(self, user=None):
            logger.info(">>> Logout")
            token = request.args.get('token')
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
                if state.elkversion >= 7:
                    usrdb = state.es.get(index="nyx_user", id=req["login"])
                else:
                    usrdb = state.es.get(index="nyx_user", doc_type="doc", id=req["login"])
            except:
                return {"error": "usernotfound"}

            usrdb["_source"]["password"] = pbkdf2_sha256.hash(req["new_password"])
            if state.elkversion >= 7:
                state.es.index(index="nyx_user", body=usrdb["_source"], id=req["login"])
            else:
                state.es.index(index="nyx_user", body=usrdb["_source"],
                               doc_type="doc", id=req["login"])

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

    @name_space.route('/cred/changepassword')
    class change_password(Resource):
        @token_required()
        @check_post_parameters("old_password", "new_password")
        @api.doc(description="Change an user password.", params={'token': 'A valid token'})
        @api.expect(change_passwordAPI)
        def post(self, user=None):
            logger.info(">>> Change password")
            req = json.loads(request.data.decode("utf-8"))
            logger.info(req)
            logger.info(user)
            if state.elkversion >= 7:
                usrdb = state.es.get(index="nyx_user", id=user["id"])
            else:
                usrdb = state.es.get(index="nyx_user", doc_type="doc", id=user["id"])
            if pbkdf2_sha256.verify(req["old_password"], usrdb["_source"]["password"]):
                usrdb["_source"]["password"] = pbkdf2_sha256.hash(req["new_password"])
                if state.elkversion >= 7:
                    res = state.es.index(index="nyx_user", body=usrdb["_source"], id=user["id"])
                else:
                    res = state.es.index(index="nyx_user", body=usrdb["_source"],
                                         doc_type="doc", id=user["id"])
                logger.info(res)
                return {"error": ""}
            else:
                return {"error": "wrongpassword"}
