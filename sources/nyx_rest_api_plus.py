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
v3.20.0 AMA 31/May/2026  Split into multiple modules
v3.21.0 AMA 31/May/2026  Fix datasource endpoint when using ELK SQL and ELK SSL
v3.21.1 AMA 24/Jun/2026  Fix a translation issue in menus
v3.21.2 AMA 01/Oct/2026  Security: restrict /streamfile to allowed roots
v3.21.3 AMA 01/Oct/2026  Security: remove tokens and credentials from logs
v3.21.4 AMA 01/Oct/2026  Fix SQL Server CRUD (POST/DELETE used the Postgres connection)
v3.21.5 AMA 01/Oct/2026  Require a token for /files uploads
v3.21.6 AMA 01/Oct/2026  Restore /streamfile access to /logs by default
v3.21.7 01/Oct/2026  Reject missing two-factor codes, consume valid codes, and stop logging them
v3.21.8 01/Oct/2026  Parameterize PostgreSQL and SQL Server generic CRUD queries
v3.21.9 01/Oct/2026  Restrict file operations to authorized app roots and log access
v3.21.10 01/Oct/2026  Restore legacy /logs paths in listdir and file downloads
v3.21.11 02/Oct/2026  Security: upgrade Flask/Werkzeug/requests/urllib3/cryptography dependencies
v3.21.12 02/Oct/2026  Update gunicorn, redis, cachetools, stomp.py and socketio dependencies
v3.21.13 02/Oct/2026  Security: update urllib3/cryptography/pyasn1/requests and stop storing raw tokens in history
v3.21.18 25/Oct/2026  Retire the legacy Dockerfile/requirements path, add requirements7 CI, and report the correct version
v3.22.0 01/Nov/2026  Configuration bundle: export the platform design and plan/apply its import
v3.22.1 01/Nov/2026  Authorization: Bearer *** on every route; /healthz and /metrics added
v3.22.2 01/Nov/2026  Log rollover is serialised between gunicorn workers (no more lost archives)
v3.22.3 01/Nov/2026  Metrics carry the worker pid, so rate() stops reading the counter as resetting
v3.22.18 03/Nov/2026  A save that cannot read the account is refused instead of resetting its password
v3.22.17 02/Nov/2026  Editing an account no longer resets its password
v3.22.16 02/Nov/2026  Facets: covered, and no longer built on an index the account cannot read
v3.22.15 02/Nov/2026  Coverage badge fed by CI, and master now runs the suite
v3.22.14 02/Nov/2026  File routes downloaded, zipped and uploaded under test; floor at 45
v3.22.13 02/Nov/2026  Repository language: English log messages, API messages and test assertions
v3.22.12 02/Nov/2026  Coverage enters CI: 43 % measured, a floor of 40, and a skip now fails the job
v3.22.11 02/Nov/2026  pyasn1 0.6.5 (four high Dependabot alerts on the ASN.1 parser)
v3.22.10 02/Nov/2026  Counters are summed across workers, and the gunicorn logs finally rotate
v3.22.9 01/Nov/2026  New accounts get a random password and cannot use the API until they change it
v3.22.8 01/Nov/2026  /config exposes the data-controller mentions, so the privacy notice is edited without a rebuild
v3.22.7 01/Nov/2026  Auth failures go to a dated index, so the curator can expire them after 7 days
v3.22.6 01/Nov/2026  A missing document is no longer an API error, and users are found by login when their _id differs
v3.22.5 01/Nov/2026  Auth failures are actually traced (the ES client is opensearchpy, which wants body=)
v3.22.4 01/Nov/2026  Login is protected against brute force (Redis counters per account and per IP,
                     ladder 30 s / 5 min / 1 h, 429 with Retry-After, failures traced in nyx_authfail)
v3.22.23 08/Nov/2026 Security: the public surface is closed by default — no CORS reflection, no
                     auto-loaded extension (WOOP answered unauthenticated calls), the Onfleet
                     webhook echoes as text/plain and needs a shared secret, ?apikey= no longer
                     accepts a session token, and the token refusals answer 401/403
v3.22.24 08/Nov/2026 A dashboard export under 3 000 hits works again: pandas removed
                     ExcelWriter.save() in 2.0 and the image runs 2.2.3, so kibana_load answered
                     500; output=csv writes a CSV instead of an Excel file, the index is no longer
                     exported as a column, and a failed export says why
v3.22.26 08/Nov/2026 GitHub sign-in: an existing account signs in from its GitHub identity
                     (GET /cred/oauth/state returns the URL and binds a one-time state to the
                     browser, POST /cred/login_github finishes the session); the stub that answered
                     a hard-coded identity and logged the provider's answers is gone
v3.22.25 08/Nov/2026 Security: the SQL of a pg_search is no longer built with the request body —
                     the ${FILTER} term is bound, the sort column must be declared by the
                     application, and size/page are integers inside bounds. The worker image is
                     built from the API image of the same release, so the two cannot drift
v3.22.27 09/Nov/2026 A UI preference lands on the caller's document, whatever login the form took:
                     `id` holds the typed login (`amarchand`) while eight accounts are indexed
                     under their address, so the write answered 404 document_missing_exception —
                     `storage_failed` in the console. The payload now carries `docid`, and a token
                     issued before it is resolved by login; the password change uses it too
v3.22.28 09/Nov/2026 Elasticsearch 6 is no longer supported: the 26 `if elkversion >= 7` guards,
                     28 `doc_type=` arguments and the 14 try/except retries they needed are gone,
                     along with the two `hits.total` shapes and the two `_type` fields. The
                     installed client is opensearch-py 2.x, whose calls have no `doc_type`
                     parameter at all — the ES6 branches could only raise. The API now refuses to
                     start on a cluster below 7 instead of answering 400s on every read.
v3.22.29 09/Nov/2026 A facet request on an index created without a mapping answers instead of
                     returning 500: `get_mappings` read `mappings.properties` unguarded, and a
                     `KeyError` came out of `queryFilter`. An unknown mapping now means "no
                     `.keyword` sub-field", which is what the facet code already handles.
v3.22.30 11/Nov/2026 The WOOP/Onfleet extension is removed. It had not been loaded since the
                     extension loader became an explicit setting (`EXT_MODULES`, empty on this
                     instance), and the client integration it existed for is gone. The loader and
                     `sources/extensions/` stay for the next extension.

v3.22.31 11/Nov/2026 The console submits a report through the API instead of forging the job

v3.22.32 11/Nov/2026 An inactive account no longer receives a token

v3.22.33 13/Nov/2026 GitHub identities that have no account can be filed, disabled
  `GITHUB_UNKNOWN_ACCOUNTS=inactive` turns an unknown GitHub identity into a `nyx_user` document
  that is disabled and carries no privileges, instead of `Bad Credentials`. The sign-in is still
  refused — `finalize_login` answers `ACCOUNT_INACTIVE` and the console shows the page explaining
  it — so filing an account grants nothing: an administrator clears `inactive` and grants the
  privileges. The default stays `refuse`, and any other value is read as `refuse`, so a typo closes
  the door rather than opening it.

  `nyx_user.inactive` (opt-in: a document without the field is active) is checked in
  `finalize_login`, the single place every sign-in path goes through — password, GitHub, Google. The
  answer is `{"error": "ACCOUNT_INACTIVE"}`, which the console turns into the page explaining what to
  do. The check runs after the password is verified, so it never tells an anonymous caller whether an
  address exists, and it does not count as a wrong password in the brute-force counters.

  `/report/generate` and `/report/regenerate` build the job from the stored definition, the
  authenticated session and an identifier of their own; the caller sends an id and parameter values.
  `/sendmessage` stays closed to non-administrators, and a `reporteditor` account can finally generate
  a report instead of receiving `403 destination not allowed` with nothing on screen.
"""

import json
import importlib
import threading
import logging

import redis
from flask import Flask, Blueprint, url_for
from flask_cors import CORS
from flask_restx import Api
from logstash_async.handler import AsynchronousLogstashHandler
from amqstompclient import amqstompclient
from opensearchpy import OpenSearch as ES

from config import settings
from common import getELKVersion
from log_setup import MultiProcessTimedRotatingFileHandler
import state

# ---------------------------------------------------------------------------
# Initialise state constants from settings
# ---------------------------------------------------------------------------
state.COOKIESECURE = True
state.WELCOME = settings.WELCOMEMESSAGE
state.ICON = settings.ICON
state.PRIVACY_CONTROLLER = settings.PRIVACY_CONTROLLER
state.PRIVACY_ADDRESS = settings.PRIVACY_ADDRESS
state.PRIVACY_CONTACT = settings.PRIVACY_CONTACT
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

# Three gunicorn workers share this file: the handler serialises the rollover, see log_setup.py.
handler = MultiProcessTimedRotatingFileHandler(
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

# ---------------------------------------------------------------------------
# CORS
# ---------------------------------------------------------------------------
# `CORS(app)` reflected every `Origin` back, which let any page on the internet read a cross-origin
# answer: the console is served from the same origin as this API and needs no header at all, so the
# default is now nothing. A deployment that really has another origin (a separate dashboard host)
# declares it in `CORS_ALLOWED_ORIGINS`, comma separated; there is no wildcard form.
cors_origins = [origin.strip() for origin in settings.CORS_ALLOWED_ORIGINS.split(",") if origin.strip()]
if cors_origins:
    CORS(app, resources={r"/api/*": {"origins": cors_origins}})
    logger.info("CORS allowed for %s", cors_origins)
else:
    logger.info("CORS disabled (no origin declared in CORS_ALLOWED_ORIGINS)")


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
from routes.configio import register as register_configio
from routes.health import register as register_health
from routes.reports import register as register_reports

register_misc(app, api, name_space)
register_auth(api, name_space)
register_files(app, api, name_space)
register_data(app, api, name_space)
register_configio(app, api, name_space)
register_health(app, api, name_space)
register_reports(api, name_space)

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
if state.elkversion < 7:
    # Refuse to run rather than degrade: the routes no longer carry a `doc_type` fallback, so a
    # 6.x cluster would answer 400s on every read and write instead of one clear message here.
    logger.error("Elasticsearch %s is not supported: 7.0 or later is required.",
                 state.elkversion)
    raise SystemExit(1)
logger.info("Elasticsearch %s", state.elkversion)
refresh_translations()

# ---------------------------------------------------------------------------
# Client extensions
# ---------------------------------------------------------------------------
# They are declared, not discovered. `lib/` used to be scanned for any file whose name contained
# "ext" and its `config()` was called with the API, the broker and the cluster: dropping a file
# there mounted routes, with no authentication of its own — that is how the WOOP module answered
# unauthenticated Onfleet calls in production for two years (it now lives in `extensions/`, see
# extensions/README.md). A failure to load is an error, not an `info` line.
logger.info("Loading declared extensions (%s)", settings.EXT_MODULES or "none")
logger.info("==============================")

from middleware import token_required

for module_name in [name.strip() for name in settings.EXT_MODULES.split(",") if name.strip()]:
    qualified = module_name if "." in module_name else "extensions." + module_name
    logger.info("Importing extension %s", qualified)
    try:
        module = importlib.import_module(qualified)
        module.config(api, state.conn, state.es, state.redisserver, token_required)
    except Exception:
        logger.error("Unable to load the extension %s", qualified, exc_info=True)

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
