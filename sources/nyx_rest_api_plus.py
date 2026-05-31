#marmar.snuids.be:440/?api=http://localhost:5001/api/v1/&user=amarchand@icloud.com&password=bagstage01
"""
v2.11.0 AMA 31/OCT/2019  Fixed a security issue that occured when the login is the mail address and get tokenized.
v2.12.0 VME 07/JAN/2020  Send a message to delete a token from all instances of the rest api when Logout.
v2.13.0 VME 23/JAN/2020  TTL tokens dictionnary, to avoid an alive token in the rest api and dead in redis.
v2.14.0 VME 05/FEB/2020  File system v1
v2.14.3 AMA 05/FEB/2020  Scrolls IDs are now correctly deleted
v2.15.0 VME 20/FEB/2020  Login will send all privileges and filters if admin
v2.15.1 VME 20/FEB/2020  Bug fixing
v3.0.0  AMA 23/FEB/2020  Compatible with elastic version 7.4.2
v3.0.1  VME 05/MAR/2020  Redisign of the files end point
v3.0.2  VME 15/MAR/2020  Fixed a few postgresql issues
v3.1.0  VME 15/MAR/2020  Fixed an issue when % character is used in kibana
v3.3.1  AMA 06/Apr/2020  Fixed a privilege issue for collections with filtered columns
v3.3.2  AMA 09/Apr/2020  Token added to upload route
v3.3.3  AMA 10/Apr/2020  Added headers to send message API
v3.4.0  AMA 15/Apr/2020  Query filter can use elastic seacrh queries
v3.5.0  VME 15/Apr/2020  passing header "upload_headers" to broker when calling upload endpoint
v3.6.0  AMA 17/Apr/2020  PG queries can use an offset
v3.6.3  AMA 18/Apr/2020  PG queries support ordering
v3.7.2  AMA 22/Apr/2020  Pagination supported in Elastic Search
v3.8.0  AMA 07/May/2020  Dynamic query filters
v3.9.0  AMA 07/May/2020  Lambda rest api added
v3.9.1  AMA 07/May/2020  App tag added
v3.10.0 VME 19/May/2020  Elastic version send back to ui (/config)
v3.10.1 VME 24/Jun/2020  Add querySize parameter for query selecter
v3.10.2 AMA 15/Jul/2020  Filters and privileges retrieved for user with the "user" privilege
v3.11.0 AMA 12/Nov/2020  Cookie flags added: secure=True,httponly=True
v3.12.0 VME 26/Jul/2021  Google Login
v3.13.0 VME 27/Jul/2021  Fix bug on status and error (requiring A1 and A2 privileges...)
v3.14.0 VME 19/Nov/2021  Get endpoint for onfleet webhook creation
v3.14.1 VME 24/Nov/2021  Modification of the onfleet webhook
v3.14.2 VME 02/Oct/2023  WOOP - Creation of the woop deliveries endpoint
v3.14.3 VME 18/Oct/2023  WOOP - Fix api bug (date format)
v3.14.4 VME 21/Nov/2023  WOOP - Add metadata on Onfleet task
v3.14.5 VME 21/Nov/2023  WOOP - Add order number in task notes
v3.15.0 AMA 14/Jun/2025  Added SQL Server support
v3.16.0 JIG 23/Sep/2025  Added AD support
v3.17.0 AMA 26/Sep/2025  Create Kibana short url
v3.18.0 AMA 26/Sep/2025  Added elastic 8 support
v3.18.7 AMA 18/Apr/2026  Re added datasource via API
v3.18.8 AMA 18/Apr/2026  AMQC privilege added to login and logout
v3.18.9 AMA 16/May/2026  Datasource endpoint supports flat parameter to return records directly
v3.19.0 AMA 31/May/2026  Pydantic for configuration and environment variables
v3.20.0 AMA xx/xxx/2026  Split into multiple modules
"""

import os
import json
import importlib
import threading
import logging
from logging.handlers import TimedRotatingFileHandler

import redis
from flask import Flask, Blueprint, url_for
from flask_cors import CORS
from flask_restx import Api
from logstash_async.handler import AsynchronousLogstashHandler
from amqstompclient import amqstompclient
from opensearchpy import OpenSearch as ES

from config import settings
from common import getELKVersion
import state

# ---------------------------------------------------------------------------
# Initialise state constants from settings
# ---------------------------------------------------------------------------
state.COOKIESECURE = True
state.WELCOME = settings.WELCOMEMESSAGE
state.ICON = settings.ICON
state.OUTPUT_FOLDER = settings.OUTPUT_FOLDER
state.OUTPUT_URL = settings.OUTPUT_URL

if not settings.COOKIESECURE:
    state.COOKIESECURE = False

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(module)s - %(funcName)s: %(message)s',
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger()

lshandler = None
if settings.USE_LOGSTASH:
    logger.info("Adding logstash appender")
    lshandler = AsynchronousLogstashHandler("logstash", 5001, database_path='logstash_test.db')
    lshandler.setLevel(logging.ERROR)
    logger.addHandler(lshandler)

handler = TimedRotatingFileHandler(
    "logs/nyx_rest_api.log", when="d", interval=1, backupCount=30
)
logFormatter = logging.Formatter(
    '%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s'
)
handler.setFormatter(logFormatter)
logger.addHandler(handler)

logger.info("Starting...")
logger.info("REST API %s" % state.VERSION)

if not settings.COOKIESECURE:
    logger.warning("Cookie set to unsecure !!!!!!!!!!!!!!!!")

# ---------------------------------------------------------------------------
# Flask + Flask-RESTX app setup
# ---------------------------------------------------------------------------
app = Flask(__name__, static_folder='temp', static_url_path='/temp')
blueprint = Blueprint('api', __name__, url_prefix='')


class Custom_API(Api):
    @property
    def specs_url(self):
        return url_for(self.endpoint('specs'), _external=False)


api = Custom_API(
    blueprint, doc='/api/doc/', version='1.0',
    title='Nyx Rest API', description='Nyx Rest API',
)

app.register_blueprint(blueprint)
name_space = api.namespace('api/v1', description='Main APIs')
CORS(app)

# ---------------------------------------------------------------------------
# Redis
# ---------------------------------------------------------------------------
logger.info("Starting redis connection")
logger.info(f"IP=>{settings.REDIS_IP}<")
state.redisserver = redis.Redis(host=settings.REDIS_IP, port=6379, db=0)

# ---------------------------------------------------------------------------
# Register route modules
# ---------------------------------------------------------------------------
from routes.misc import register as register_misc
from routes.auth import register as register_auth
from routes.files import register as register_files
from routes.data import register as register_data

register_misc(app, api, name_space)
register_auth(api, name_space)
register_files(app, api, name_space)
register_data(app, api, name_space)

# ---------------------------------------------------------------------------
# AMQC message handler
# ---------------------------------------------------------------------------
def messageReceived(destination, message, headers):
    if "LOGOUT_EVENT" in destination:
        if message in state.tokens:
            del state.tokens[message]
    elif "NYX_LAMBDA_RESTAPI" in destination:
        with state.restapiresultslock:
            state.restapiresults.append(json.loads(message))
    else:
        logger.error("Unknown destination %s" % destination)


server = {
    "ip": settings.AMQC_URL,
    "port": settings.AMQC_PORT,
    "login": settings.AMQC_LOGIN,
    "password": settings.AMQC_PASSWORD,
}
state.conn = amqstompclient.AMQClient(
    server,
    {"name": state.MODULE, "version": state.VERSION, "lifesign": "/topic/NYX_MODULE_INFO"},
    ['/topic/LOGOUT_EVENT', '/topic/NYX_LAMBDA_RESTAPI'],
    callback=messageReceived,
)

# ---------------------------------------------------------------------------
# Elasticsearch / OpenSearch
# ---------------------------------------------------------------------------
logger.info(settings.ELK_SSL)

if settings.ELK_SSL:
    state.es = ES(
        hosts=[f"https://{settings.ELK_URL}"],
        http_auth=(settings.ELK_LOGIN, settings.ELK_PASSWORD),
        use_ssl=True,
        verify_certs=False,
        ssl_show_warn=False,
    )
else:
    state.es = ES(hosts=[f"http://{settings.ELK_URL}"])

# ---------------------------------------------------------------------------
# Background thread (API call history + life-sign)
# ---------------------------------------------------------------------------
from es_helpers import handleAPICalls, refresh_translations

thread = threading.Thread(target=handleAPICalls)
thread.start()

state.elkversion = getELKVersion(state.es)
refresh_translations()

# ---------------------------------------------------------------------------
# Extension library scanning
# ---------------------------------------------------------------------------
logger.info("Scanning files in lib...")
logger.info("========================")

from middleware import token_required

for ext_lib in os.listdir("lib"):
    if ".py" in ext_lib and "ext" in ext_lib:
        logger.info("Importing 2:" + ext_lib)
        logger.info("lib." + ext_lib.replace(".py", ""))
        try:
            module = importlib.import_module("lib." + ext_lib.replace(".py", ""))
            module.config(api, state.conn, state.es, state.redisserver, token_required)
        except Exception as e:
            logger.info(e)

# ---------------------------------------------------------------------------
# Gunicorn / direct run
# ---------------------------------------------------------------------------
if __name__ != '__main__':
    gunicorn_logger = logging.getLogger("gunicorn.error")
    logger.handlers = gunicorn_logger.handlers
    logger.setLevel(gunicorn_logger.level)
    if lshandler is not None:
        logger.info("ADDING LOGSTASH HANDLER")
        gunicorn_logger.addHandler(lshandler)

if __name__ == '__main__':
    logger.info(f"AMQC_URL          :{settings.AMQC_URL}")
    app.run(threaded=False, host='0.0.0.0', port=5001)
