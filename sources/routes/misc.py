"""
Miscellaneous routes: /test, /ui_css, /lambdas, /onfleet_webhook,
/config, /status, /error, /sendmessage, /esmapping, /grafana/dashboards.
"""

import json
import time
import uuid
import logging
import operator

import requests
from flask import request, Response, render_template, make_response
from flask_restx import Resource, fields
from datetime import datetime, timedelta

import state
from config import settings
from middleware import token_required, check_post_parameters, checkAPIKey

logger = logging.getLogger()


def register(app, api, name_space):

    # -----------------------------------------------------------------------
    # /test
    # -----------------------------------------------------------------------
    @app.route('/test')
    def test():
        logger.info("Test")
        return render_template('test.html')

    # -----------------------------------------------------------------------
    # /api/v1/ui_css
    # -----------------------------------------------------------------------
    @app.route('/api/v1/ui_css')
    def cssRest():
        logger.info("CSS called")
        if state.elkversion >= 7:
            res = state.es.get(index="nyx_config", id="nyx_css")
        else:
            res = state.es.get(index="nyx_config", id="nyx_css", doc_type="doc")
        return Response(res["_source"]["file"], mimetype='text/css')

    # -----------------------------------------------------------------------
    # /lambdas
    # -----------------------------------------------------------------------
    lambdaAPI = api.model('lambda_model', {})

    @name_space.route('/lambdas/<string:runner>/<string:lambdaname>')
    @api.doc(description="Calls a specific lambda.", params={'apikey': 'A valid token'})
    class lambdasRest(Resource):
        @api.expect(lambdaAPI)
        def post(self, runner, lambdaname, user=None):
            if not checkAPIKey(request):
                return {'error': "BAD API KEY"}

            tosend = {
                "runner": runner,
                "action": "execute",
                "restapi": lambdaname,
                "body": json.loads(request.data.decode("utf-8")),
                "guid": str(uuid.uuid4()),
            }
            state.restapiresults = []
            state.conn.send_message("/topic/NYX_LAMBDA_COMMAND", json.dumps(tosend))

            starttime = datetime.now()
            while True:
                time.sleep(0.05)
                with state.restapiresultslock:
                    for res in state.restapiresults:
                        if res["guid"] == tosend["guid"]:
                            if "return" in res and res["return"] != "null":
                                return json.loads(res["return"])
                            else:
                                return {'error': "Unknown lambda or lambda crashed"}

                if starttime + timedelta(seconds=5) < datetime.now():
                    break

            return {'error': "No answer"}

    # -----------------------------------------------------------------------
    # /onfleet_webhook
    # -----------------------------------------------------------------------
    @name_space.route('/onfleet_webhook')
    @api.doc(description="When creating a webook in onfleet we need to validate through a Get call.")
    class onfleetWebhookCreation(Resource):
        def get(self):
            check = str(request.args.get('check'))
            return make_response(check)

        def post(self):
            req = json.loads(request.data.decode("utf-8"))
            logger.info(req)
            state.conn.send_message("/topic/ONFLEET_WEBHOOK", json.dumps(req))
            return {'error': ""}

    # -----------------------------------------------------------------------
    # /config
    # -----------------------------------------------------------------------
    @name_space.route('/config')
    @api.doc(description="Get the instance config.")
    class configRest(Resource):
        def get(self):
            logger.info("Config called")
            return {
                'error': "",
                'status': 'ok',
                'version': state.VERSION,
                'welcome': state.WELCOME,
                'icon': state.ICON,
                'elastic_version': state.elkversion,
            }

    # -----------------------------------------------------------------------
    # /status
    # -----------------------------------------------------------------------
    @name_space.route('/status')
    @api.doc(description="Get the instance status.", params={'token': 'A valid token'})
    class statusRest(Resource):
        @token_required()
        def get(self, user=None):
            return {
                'error': "",
                'status': 'ok',
                'version': state.VERSION,
                'name': state.MODULE,
            }

    # -----------------------------------------------------------------------
    # /error
    # -----------------------------------------------------------------------
    @name_space.route('/error')
    class errorRest(Resource):
        @api.doc(description="Error log debug.", params={'token': 'A valid token'})
        @token_required()
        def get(self, user=None):
            logger.error("ERROR")
            return {
                'error': "",
                'status': 'ok',
                'version': state.VERSION,
                'name': state.MODULE,
            }

    # -----------------------------------------------------------------------
    # /sendmessage
    # -----------------------------------------------------------------------
    sendMessageAPI = api.model('sendMessage_model', {
        'destination': fields.String(
            description="The destinaiton example: /queue/TEST", required=True
        ),
        'body': fields.String(description="The message as a string.", required=True),
        'headers': fields.String(description="The headers as a string (STRINGIFIED)."),
    })

    @name_space.route('/sendmessage')
    class sendMessage(Resource):
        @token_required()
        @check_post_parameters("destination", "body")
        @api.doc(description="Send a message to the broker.", params={'token': 'A valid token'})
        @api.expect(sendMessageAPI)
        def post(self, user=None):
            req = json.loads(request.data.decode("utf-8"))
            headers = None
            if "headers" in req and len(req["headers"]) > 0:
                headers = json.loads(req["headers"])
            state.conn.send_message(req["destination"], req["body"], headers=headers)
            return {'error': ""}

    # -----------------------------------------------------------------------
    # /esmapping
    # -----------------------------------------------------------------------
    @name_space.route('/esmapping/<string:index_pattern>')
    @api.doc(description="Get ES mapping based on an index pattern.",
             params={'token': 'A valid token'})
    class esMapping(Resource):
        @token_required()
        def get(self, index_pattern='*', user=None):
            logger.info('get ES mapping')
            try:
                mappings = state.es.indices.get_mapping(index=index_pattern)
                mappings = [
                    {"id": x, "obj": mappings[x]}
                    for x in mappings if not x.startswith('.')
                ]
                mappings.sort(key=operator.itemgetter('id'))
                return {"error": "", "data": mappings}
            except Exception as e:
                return {"error": "", "data": None}

    # -----------------------------------------------------------------------
    # /grafana/dashboards
    # -----------------------------------------------------------------------
    @name_space.route('/grafana/dashboards')
    @api.doc(description="Get Grafana Dashboards.", params={'token': 'A valid token'})
    class grafanaDashboards(Resource):
        @token_required()
        def get(self, user=None):
            GRAFANA_URL = settings.GRAFANA_URL
            GRAFANA_API_KEY = settings.GRAFANA_API_KEY

            if len(GRAFANA_URL) == 0 or len(GRAFANA_API_KEY) == 0:
                logger.error("Grafana URL or API Key not set in environment variables.")
                return {"error": "", "data": []}
            else:
                url = f"{GRAFANA_URL}/api/search?type=dash-db"
                headers = {
                    "Authorization": f"Bearer {GRAFANA_API_KEY}",
                    "Content-Type": "application/json",
                }
                response = requests.get(url, headers=headers)
                response.raise_for_status()
                return {"error": "", "data": response.json()}
