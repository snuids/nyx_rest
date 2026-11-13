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
from typing import Optional

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

    def __init__(self, docs=None, foreign_hits=None, mappings=None, aggregations=None):
        # Deep, not per-index: a test that flips a field on a fixture (`inactive`, a password)
        # would otherwise edit the class attribute and hand the change to the next test.
        self.docs = copy.deepcopy(docs or {})
        self.foreign_hits = foreign_hits or {}
        # field -> type, per index: `common.get_mappings` reads them to decide whether a facet
        # is served on the field itself or on its `.keyword` sub-field
        self.mappings = mappings or {}
        # index -> {field: [buckets]}: what an aggregation returns
        self.aggregations = aggregations or {}
        self.indices = FakeIndices(self.mappings)
        self.calls = []
        self.written = []
        self.deleted = []
        self.searches = []
        # Set to an exception instance to make `get` fail the way a cluster error does —
        # distinct from the `NotFound` a missing document raises.
        self.get_error: "Optional[Exception]" = None

    # -- reading ---------------------------------------------------------
    def get(self, index=None, id=None, **kwargs):
        self.calls.append(("get", index, id))
        if self.get_error is not None:
            raise self.get_error
        source = (self.docs.get(index) or {}).get(id)
        if source is None:
            raise NotFound(f"{index}/{id}")
        # copy: a real client deserialises the JSON, so the calling code cannot modify our
        # fixtures (`finalize_login` sets the password field to empty on the document)
        return {"_id": id, "_index": index, "_source": copy.deepcopy(source)}

    def _matching_indices(self, index):
        """`nyx_reporttask-*` matches every monthly index, as the real client does."""
        if index and index.endswith("-*"):
            prefix = index[:-1]
            return [name for name in self.docs if name.startswith(prefix)]
        return [index]

    def search(self, index=None, body=None, size=None, **kwargs):
        self.calls.append(("search", index, body))
        self.searches.append((index, body))
        if isinstance(body, str):
            # `loadData` posts its query as JSON text, which the real client accepts; decoding it
            # here keeps the `term` and `ids` filters below working for routes that go through it.
            try:
                body = json.loads(body)
            except ValueError:
                body = None
        hits = self.foreign_hits.get(index)
        if hits is None:
            hits = [{"_id": key, "_index": name, "_source": copy.deepcopy(value)}
                    for name in self._matching_indices(index)
                    for key, value in (self.docs.get(name) or {}).items()]
        else:
            hits = copy.deepcopy(hits)
        # One result decides a write: the login lookup that tells an update from a creation
        # in `/generic/nyx_user/<id>`. A fake returning every account would have the route
        # copy a stranger's hash onto the document being saved, so that single term is
        # honoured. Everything else still returns the index untouched, on purpose: the
        # rights tests assert on the body sent, and a fake that filtered for them would
        # give false assurance.
        # An `ids` query asks for specific documents (`/report/regenerate` replays one task): a fake
        # that returned the whole index would hand the route a stranger's document.
        ids_query = ((body or {}).get("query", {}) or {}).get("ids") or {}
        if ids_query.get("values"):
            wanted_ids = {str(value) for value in ids_query["values"]}
            hits = [h for h in hits if str(h.get("_id")) in wanted_ids]

        wanted = ((body or {}).get("query", {}) or {}).get("term", {}) or {}
        for field, value in wanted.items():
            if field.split(".")[0] == "login":
                hits = [h for h in hits
                        if str((h.get("_source") or {}).get("login") or "").lower()
                        == str(value).lower()]
                break
        aggregated = copy.deepcopy(self.aggregations.get(index) or {})
        return {"hits": {"hits": list(hits), "total": {"value": len(hits)}},
                "aggregations": {field: {"buckets": buckets}
                                 for field, buckets in aggregated.items()}}

    def clear_scroll(self, body=None, **kwargs):
        """`loadData` closes its scroll once the page is read; the fake holds nothing open."""
        self.calls.append(("clear_scroll", None, body))
        return {"succeeded": True, "_nodes": {"total": 1, "successful": 1}}

    def info(self, **kwargs):
        # `common.get_es_info` calls es.info(request_timeout=5)
        return {"version": {"number": "8.19.23"}}

    def msearch(self, body, **kwargs):
        self.calls.append(("msearch", None, body))
        return {"responses": []}

    # -- writing ---------------------------------------------------------
    def index(self, index=None, body=None, id=None, **kwargs):
        if "document" in kwargs:
            # The real client is opensearchpy: it wants `body=`, and `document=` raises. Some
            # callers try elasticsearch-py's spelling first inside a `try/except TypeError`
            # (`record_auth_failure`), so a fake that accepted both would silently swallow the
            # write and the test would assert on a document that was never sent.
            raise TypeError("index() got an unexpected keyword argument 'document'")
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

    def update(self, index=None, id=None, body=None, **kwargs):
        """Partial update, like the real client: `{"doc": {...}}` merges into the document.

        The preferences route is the first caller; a fake that ignored the write would let it
        pass while saving nothing."""
        parsed = body
        if isinstance(body, str):
            try:
                parsed = json.loads(body)
            except ValueError:
                parsed = body
        patch = (parsed or {}).get("doc") or {}
        current = (self.docs.get(index) or {}).get(id)
        if current is None:
            raise NotFound(index + "/" + str(id))
        merged = copy.deepcopy(current)
        merged.update(copy.deepcopy(patch))
        self.written.append((index, id, merged))
        self.docs[index][id] = merged
        return {"result": "updated", "_id": id}

    def delete(self, index=None, id=None, **kwargs):
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


class FakeIndices:
    """Double of `es.indices`, for the mapping lookup the facets depend on."""

    def __init__(self, mappings):
        self.mappings = mappings

    def get_mapping(self, index=None, **kwargs):
        properties = {field: {"type": kind}
                      for field, kind in (self.mappings.get(index) or {}).items()}
        return {index: {"mappings": {"properties": properties}}}


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

    def send_message(self, destination, body, headers=None):
        self.messages.append((destination, body))
        self.last_headers = headers
        return True

    def sent_to(self, destination):
        return [body for dest, body in self.messages if dest == destination]


@unittest.skipUnless(HAVE_ROUTE_DEPS, "route dependencies missing (" + DEPS_MISSING + ")")
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
