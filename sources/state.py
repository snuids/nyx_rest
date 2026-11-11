"""
Global mutable state shared across all modules.
Import this module and access state.variable_name rather than using
'from state import variable_name' for mutable values that may be reassigned.
"""

import os
import cachetools
import threading
from datetime import datetime, timedelta

# Application metadata
VERSION = "3.22.31"
MODULE = "nyx_rest_" + str(os.getpid())

# Config-derived constants (set during startup in nyx_rest_api_plus.py)
COOKIESECURE = True
WELCOME = ""
ICON = ""
PRIVACY_CONTROLLER = ""
PRIVACY_ADDRESS = ""
PRIVACY_CONTACT = ""
OUTPUT_FOLDER = ""
OUTPUT_URL = ""

# Elasticsearch major version, read at startup and refreshed by the API-call thread. It is
# reported (`/health`, `/metrics`, `/config`) but no longer branched on: 6 is unsupported since
# the client installed is opensearch-py 2.x, which has no `doc_type` parameter.
elkversion = 7

# Service connections (set during startup)
es = None
conn = None
redisserver = None

# PostgreSQL
pg_connection = None
pg_thread = None

# AMQC lambda results
restapiresults = []
restapiresultslock = threading.RLock()

# Indices cache
indices = {}
indices_refresh_seconds = 60
last_indices_refresh = datetime.now() - timedelta(minutes=10)

# Translations cache
translations = {}
last_translation_refresh_seconds = 60
last_translation_refresh = datetime.now() - timedelta(minutes=10)

# Runtime counters, exposed by /api/v1/metrics (reset when the worker restarts)
requests_total = 0
errors_total = 0
auth_failures_total = 0     # password or two-phase code refused
auth_blocked_total = 0      # attempts turned away by the guard (429)
last_lifesign = None

# Auth tokens
tokens = cachetools.TTLCache(maxsize=1000, ttl=5 * 60)
tokenlock = threading.RLock()
userlock = threading.RLock()
userActivities = []
