"""Test harness for the routes: a real Flask application, a fake cluster.

The repository suite tests the extracted logic (`login_guard`, `can_use_indice`,
`config_bundle`, `es_read`…) but no route: `sources/routes/data.py`,
`sources/routes/auth.py` and `sources/routes/files.py` were at **0 %** coverage, so the
access guards added in 3.22.12 (read limited to one's own account, writing admins only) and
the throttling of the first login were protected by no test in the repository — only by
manual checks that do not survive the session.

This module mounts a real Flask + flask-restx application, registers the requested route
modules, and replaces their collaborators (Elasticsearch, Redis, broker) with doubles that
log the calls. The goal is not to test the business logic — it already has its tests — but
the **wiring**: who is allowed to call what, with which request body, and what is sent to
the cluster.

The route modules import pandas, dateutil, psycopg2, pyodbc: the suite must keep running on
a bare interpreter, so the test classes are `skipUnless` (repository convention).
"""

import copy
import inspect
import json
import sys
import unittest
import uuid
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sources"))

try:                                    # full dependencies: harness usable
    import flask                                                    # noqa: F401
    import flask_restx                                              # noqa: F401
    import pandas                                                   # noqa: F401
    import dateutil.parser                                          # noqa: F401
    import requests                                                 # noqa: F401
    import psycopg2                                                 # noqa: F401
    HAVE_ROUTE_DEPS = True
    DEPS_MISSING = ""
except Exception as exc:                # ImportError, but also binary import errors
    HAVE_ROUTE_DEPS = False
    DEPS_MISSING = type(exc).__name__ + ": " + str(exc)[:80]


class NotFound(Exception):
    """Like opensearchpy: a missing document, not an outage."""
    status_code = 404


class FakeES:
    """Double of the Elasticsearch client, index by name, calls logged.

    `search` returns the documents of the requested index without evaluating the query: the
    tests that bear on the rights therefore check the **body sent** (`es.searches`), not the
    result. A fake cluster that filtered in place of the real one would give false assurance.
    """

    def __init__(self, docs=None, foreign_hits=None):
        self.docs = {index: dict(values) for index, values in (docs or {}).items()}
        self.foreign_hits = foreign_hits or {}
        self.calls = []
        self.written = []
        self.deleted = []
        self.searches = []

    # -- reading ---------------------------------------------------------
    def get(self, index=None, id=None, doc_type=None, **kwargs):
        self.calls.append(("get", index, id))
        source = (self.docs.get(index) or {}).get(id)
        if source is None:
            raise NotFound(f"{index}/{id}")
        # copy: a real client deserialises the JSON, so the calling code cannot modify our
        # fixtures (`finalize_login` sets the password field to empty on the document)
        return {"_id": id, "_index": index, "_source": copy.deepcopy(source)}

    def search(self, index=None, body=None, doc_type=None, size=None, **kwargs):
        self.calls.append(("search", index, body))
        self.searches.append((index, body))
        hits = self.foreign_hits.get(index)
        if hits is None:
            hits = [{"_id": key, "_index": index, "_source": copy.deepcopy(value)}
                    for key, value in (self.docs.get(index) or {}).items()]
        else:
            hits = copy.deepcopy(hits)
        return {"hits": {"hits": list(hits), "total": {"value": len(hits)}},
                "aggregations": {}}

    def info(self, **kwargs):
        # `common.get_es_info` calls es.info(request_timeout=5)
        return {"version": {"number": "8.19.23"}}

    def msearch(self, body, **kwargs):
        self.calls.append(("msearch", None, body))
        return {"responses": []}

    # -- writing ---------------------------------------------------------
    def index(self, index=None, body=None, id=None, doc_type=None, **kwargs):
        parsed = body
        if isinstance(body, str):
            try:
                parsed = json.loads(body)
            except ValueError:
                parsed = body
        # `written` keeps the parsed body: the route sends `json.dumps(...)`, and the tests
        # assert on the fields (hash, flag), not on the serialisation.
        self.written.append((index, id, parsed))
        self.docs.setdefault(index, {})[id] = copy.deepcopy(parsed)
        return {"result": "created", "_id": id}

    def delete(self, index=None, id=None, doc_type=None, **kwargs):
        self.deleted.append((index, id))
        (self.docs.get(index) or {}).pop(id, None)
        return {"result": "deleted"}

    # -- test helpers -------------------------------------------------
    def written_on(self, index):
        return [(doc_id, body) for idx, doc_id, body in self.written if idx == index]

    def last_search_query(self, index):
        for idx, body in reversed(self.searches):
            if idx == index:
                return (body or {}).get("query")
        return None

    def was_touched(self, index, doc_id=None):
        for call in self.calls + [("write", i, d) for i, d, _ in self.written]:
            if call[1] == index and (doc_id is None or call[2] == doc_id):
                return True
        return False


class FakeRedis:
    """Double of redis-py: the keys, the guard counters, and the requested TTLs.

    `get` returns `bytes` like the real client (created without `decode_responses`): that is
    what the code receives, so the code must be exercised with it.
    """

    def __init__(self):
        self.store = {}
        self.ttls = {}
        self.set_calls = []

    def set(self, key, value, ex=None, *args, **kwargs):
        self.set_calls.append((key, ex))
        self.store[key] = value
        if ex:
            self.ttls[key] = int(ex)
        return True

    def setex(self, key, ttl, value):
        return self.set(key, value, ttl)

    def get(self, key):
        value = self.store.get(key)
        if value is None:
            return None
        return value if isinstance(value, bytes) else str(value).encode()

    def incr(self, key, amount=1):
        current = int(self.store.get(key) or 0) + amount
        self.store[key] = str(current)
        return current

    def expire(self, key, ttl):
        self.ttls[key] = int(ttl)
        return True

    def ttl(self, key):
        if key not in self.store:
            return -2
        return self.ttls.get(key, -1)

    def delete(self, *keys):
        removed = 0
        for key in keys:
            if key in self.store:
                del self.store[key]
                removed += 1
        return removed

    def keys(self, pattern="*"):
        return [key.encode() for key in self.store]

    def scan_iter(self, match="*"):
        return iter([])


class FakeBroker:
    """Double of the ActiveMQ client: we log what would leave for the broker."""

    def __init__(self):
        self.messages = []

    def send_message(self, destination, body):
        self.messages.append((destination, body))
        return True

    def sent_to(self, destination):
        return [body for dest, body in self.messages if dest == destination]


@unittest.skipUnless(HAVE_ROUTE_DEPS, "dépendances de route absentes (" + DEPS_MISSING + ")")
class RouteTestCase(unittest.TestCase):
    """Base of the route tests: fakes installed, application mounted, `state` restored.

    Subclasses: fill in `route_modules`, `documents` (index -> {id: source}) and
    `foreign_hits` as needed, then use `call()` and `login_as()`.
    """

    route_modules = ()
    documents = {}
    foreign_hits = {}
    elk_version = 8

    def setUp(self):
        import state
        self.state = state
        self._saved = {}

        self.es = FakeES(self.documents, self.foreign_hits)
        self.redis = FakeRedis()
        self.broker = FakeBroker()

        self._install("es", self.es)
        self._install("redisserver", self.redis)
        self._install("conn", self.broker)
        self._install("elkversion", self.elk_version)
        self._install("indices", [])
        self._install("indices_refresh_seconds", 60)
        self._install("last_indices_refresh", datetime.now())
        self._install("translations", {})
        self._install("last_translation_refresh", datetime.now())
        self._install("tokens", {})
        self._install("COOKIESECURE", False)
        self._install("VERSION", "test")
        self._install("OUTPUT_URL", "http://localhost")
        self._install("OUTPUT_FOLDER", "/tmp")
        self._install("userActivities", [])
        self._install("requests_total", 0)
        self._install("errors_total", 0)
        self._install("auth_failures_total", 0)
        self._install("auth_blocked_total", 0)

        from flask import Flask
        from flask_restx import Api
        self.app = Flask(__name__)
        api = Api(self.app)
        namespace = api.namespace("api/v1")
        for module in self.route_modules:
            # `routes/auth.py` registers on `api` alone; the others take the Flask
            # application first. We look at the first parameter rather than assume it.
            first = list(inspect.signature(module.register).parameters)[:1]
            if first == ["app"]:
                module.register(self.app, api, namespace)
            else:
                module.register(api, namespace)
        self.client = self.app.test_client()

    # -- infrastructure --------------------------------------------------
    def _install(self, name, value):
        """Sets an attribute of `state` and restores it at the end of the test."""
        self._saved[name] = getattr(self.state, name)
        setattr(self.state, name, value)
        self.addCleanup(setattr, self.state, name, self._saved[name])

    def login_as(self, login, privileges, **extra):
        """Creates a valid token in memory and in the fake Redis, as a login would."""
        token = str(uuid.uuid4())
        payload = {"login": login, "id": login, "privileges": list(privileges),
                   "mustResetPassword": False, "language": "en", "filters": []}
        payload.update(extra)
        self.state.tokens[token] = payload
        self.redis.set("nyx_tok_" + token, json.dumps(payload), 3600 * 24)
        return token

    def call(self, method, path, token=None, body=None, headers=None):
        kwargs = {}
        merged = dict(headers or {})
        if token:
            merged["Authorization"] = "Bearer " + token
        if merged:
            kwargs["headers"] = merged
        if body is not None:
            kwargs["json"] = body
        response = getattr(self.client, method)(path, **kwargs)
        return response.status_code, response.get_json(), response

    def capture_load_data(self, module):
        """Intercepts `loadData` of the route module and keeps the query produced.

        `loadData` is the export engine (scroll, aggregations, privileges, CSV): it deserves
        its own tests, and running it here would force us to simulate the whole scroll
        protocol just to observe one thing — the query the route hands to it. So we keep the
        boundary: the route computes, `loadData` executes.
        """
        captured = {"index": None, "query": None, "calls": 0}
        original = module.loadData

        def fake(*args, **kwargs):
            captured["index"] = kwargs.get("index", args[2] if len(args) > 2 else None)
            body = kwargs.get("data", args[3] if len(args) > 3 else None)
            captured["query"] = (body or {}).get("query")
            captured["calls"] += 1
            return {"error": "", "records": [], "aggs": [], "total": 0}

        module.loadData = fake
        self.addCleanup(setattr, module, "loadData", original)
        return captured
