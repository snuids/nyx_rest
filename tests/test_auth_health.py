"""
Tests for the authentication of a request and for the two probe endpoints.

They run the real decorators and routes through a Flask test client: what is verified is the
behaviour a client sees (a header is accepted, a missing token is refused, a probe answers 503
when a dependency is down), not the internals.
"""

import json
import sys
import unittest
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sources"))

try:
    from flask import Flask
    from flask_restx import Api, Resource

    HAVE_WEB_STACK = True
except Exception:  # pragma: no cover - depends on the machine running the tests
    HAVE_WEB_STACK = False

import state
from middleware import request_token


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def get(self, key=None, default=None):
        return default


class FakeRedis:
    def __init__(self, up=True, entries=None):
        self.up = up
        self.entries = entries or {}

    def get(self, key):
        return None

    def hgetall(self, key):
        if not self.up:
            raise ConnectionError("redis is down")
        return dict(self.entries)

    def ping(self):
        if not self.up:
            raise ConnectionError("redis is down")
        return True


class FakeES:
    def __init__(self, up=True):
        self.up = up

    def info(self, request_timeout=None):
        if not self.up:
            raise ConnectionError("elasticsearch is down")
        return {"version": {"number": "7.2.0"}, "cluster_name": "nyxdata-cluster"}


class FakeRequest:
    """Minimal stand-in for a Flask request, for the request_token unit tests."""

    def __init__(self, args=None, headers=None):
        self.args = args or {}
        self.headers = headers or {}


class RequestTokenTests(unittest.TestCase):
    def test_a_bearer_header_is_preferred(self):
        token, source = request_token(
            FakeRequest({"token": "from-query"}, {"Authorization": "Bearer from-header"})
        )
        self.assertEqual((token, source), ("from-header", "header"))

    def test_the_header_is_case_insensitive_and_tolerates_spaces(self):
        for header in ("bearer abc", "BEARER abc", "Bearer   abc  "):
            with self.subTest(header=header):
                self.assertEqual(request_token(FakeRequest({}, {"Authorization": header})), ("abc", "header"))

    def test_the_query_parameter_still_works(self):
        self.assertEqual(request_token(FakeRequest({"token": "abc"}, {})), ("abc", "query"))

    def test_no_token_at_all(self):
        self.assertEqual(request_token(FakeRequest({}, {})), (None, None))
        self.assertEqual(request_token(FakeRequest({}, {"Authorization": "Basic abc"})), (None, None))
        self.assertEqual(request_token(FakeRequest({}, {"Authorization": "Bearer "})), (None, None))


@unittest.skipUnless(HAVE_WEB_STACK, "flask / flask-restx not installed")
class RouteTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from middleware import token_required
        from routes.health import register as register_health

        cls.app = Flask(__name__)
        api = Api(cls.app, doc="/api/doc/")
        namespace = api.namespace("api/v1")
        register_health(cls.app, api, namespace)

        @namespace.route("/protected")
        @api.doc(description="Test route")
        class Protected(Resource):
            @token_required()
            def get(self, user=None):
                return {"error": "", "login": user["login"]}

        cls.client = cls.app.test_client()
        state.VERSION = "3.22.1"
        state.elkversion = 7

    def setUp(self):
        state.tokens.clear()
        state.tokens["admin-token"] = {
            "login": "tester",
            "id": "tester",
            "privileges": ["admin"],
            "language": "en",
        }
        state.redisserver = FakeRedis()
        state.es = FakeES()
        state.conn = object()
        state.last_lifesign = datetime.now()
        state.requests_total = 0
        state.errors_total = 0
        state.auth_failures_total = 0
        state.auth_blocked_total = 0

    # -- authentication ---------------------------------------------------

    def test_a_token_in_the_query_string_is_accepted(self):
        payload = self.client.get("/api/v1/protected?token=admin-token").get_json()
        self.assertEqual(payload["error"], "")
        self.assertEqual(payload["login"], "tester")

    def test_a_bearer_token_is_accepted(self):
        response = self.client.get(
            "/api/v1/protected", headers={"Authorization": "Bearer admin-token"}
        )
        payload = response.get_json()
        self.assertEqual(payload["error"], "")
        self.assertEqual(payload["login"], "tester")

    def test_a_bearer_token_wins_over_an_invalid_query_token(self):
        response = self.client.get(
            "/api/v1/protected?token=unknown", headers={"Authorization": "Bearer admin-token"}
        )
        self.assertEqual(response.get_json()["error"], "")

    def test_no_token_is_refused(self):
        self.assertEqual(self.client.get("/api/v1/protected").get_json()["error"], "NO_TOKEN")

    def test_an_unknown_bearer_token_is_refused(self):
        response = self.client.get(
            "/api/v1/protected", headers={"Authorization": "Bearer nope"}
        )
        self.assertEqual(response.get_json()["error"], "UNKNOWN_TOKEN")

    def test_the_counters_move(self):
        self.client.get("/api/v1/protected?token=admin-token")
        self.client.get("/api/v1/protected")
        self.assertEqual(state.requests_total, 2)
        self.assertEqual(state.errors_total, 1)

    # -- healthz ----------------------------------------------------------

    def test_healthz_is_up_when_everything_answers(self):
        response = self.client.get("/api/v1/healthz")
        payload = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["version"], "3.22.1")
        self.assertEqual(payload["checks"]["elasticsearch"]["status"], "up")
        self.assertEqual(payload["checks"]["redis"]["status"], "up")
        self.assertEqual(payload["checks"]["broker"]["status"], "up")
        self.assertEqual(response.headers["Cache-Control"], "no-store")

    def test_healthz_needs_no_token(self):
        self.assertEqual(self.client.get("/api/v1/healthz").status_code, 200)

    def test_healthz_is_degraded_when_redis_is_down(self):
        state.redisserver = FakeRedis(up=False)
        response = self.client.get("/api/v1/healthz")
        payload = response.get_json()
        self.assertEqual(response.status_code, 503)
        self.assertEqual(payload["status"], "degraded")
        self.assertEqual(payload["checks"]["redis"]["status"], "down")

    def test_healthz_is_degraded_when_elasticsearch_is_down(self):
        state.es = FakeES(up=False)
        response = self.client.get("/api/v1/healthz")
        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.get_json()["checks"]["elasticsearch"]["status"], "down")

    def test_healthz_is_degraded_when_the_broker_life_sign_is_stale(self):
        state.last_lifesign = datetime.now() - timedelta(minutes=2)
        response = self.client.get("/api/v1/healthz")
        payload = response.get_json()
        self.assertEqual(response.status_code, 503)
        self.assertEqual(payload["checks"]["broker"]["status"], "down")
        self.assertGreater(payload["checks"]["broker"]["lastLifeSignSeconds"], 100)

    def test_healthz_says_so_when_no_life_sign_was_ever_received(self):
        state.last_lifesign = None
        payload = self.client.get("/api/v1/healthz").get_json()
        self.assertEqual(payload["checks"]["broker"]["status"], "down")
        self.assertEqual(payload["checks"]["broker"]["error"], "no life sign yet")

    # -- metrics ----------------------------------------------------------

    def test_metrics_exposes_the_counters_and_the_dependency_state(self):
        state.requests_total = 12
        state.errors_total = 3
        response = self.client.get("/api/v1/metrics")
        body = response.get_data(as_text=True)
        self.assertEqual(response.status_code, 200)
        self.assertIn("text/plain", response.headers["Content-Type"])
        for expected in (
            "nyx_up 1",
            'nyx_info{version="3.22.1",elastic_version="7"} 1',
            "nyx_elasticsearch_up 1",
            "nyx_redis_up 1",
            "nyx_broker_up 1",
        ):
            self.assertIn(expected, body)
        self.assertIn("# TYPE nyx_requests_total counter", body)

    def test_metrics_sums_what_every_worker_published(self):
        """Prometheus ne voyait qu'un worker sur trois : /metrics additionne ce que Redis contient."""
        now = int(datetime.now().timestamp())
        state.requests_total = 999          # valeur locale : elle ne doit pas être exposée seule
        state.errors_total = 999
        state.redisserver = FakeRedis(entries={
            "7": json.dumps({"at": now, "requests_total": 10, "errors_total": 1}),
            "9": json.dumps({"at": now, "requests_total": 5, "errors_total": 0}),
        })
        body = self.client.get("/api/v1/metrics").get_data(as_text=True)
        self.assertIn("nyx_requests_total 15", body)
        self.assertIn("nyx_errors_total 1", body)
        self.assertIn("nyx_counters_workers 2", body)
        self.assertNotIn("worker=", body)

    def test_metrics_falls_back_on_local_counters_without_redis_publishers(self):
        state.requests_total = 12
        state.errors_total = 3
        body = self.client.get("/api/v1/metrics").get_data(as_text=True)
        self.assertIn("nyx_requests_total 12", body)
        self.assertIn("nyx_errors_total 3", body)
        self.assertIn("nyx_counters_workers 0", body)

    def test_metrics_reports_a_down_dependency(self):
        state.es = FakeES(up=False)
        state.redisserver = FakeRedis(up=False)
        body = self.client.get("/api/v1/metrics").get_data(as_text=True)
        self.assertIn("nyx_elasticsearch_up 0", body)
        self.assertIn("nyx_redis_up 0", body)

    def test_metrics_escapes_label_values(self):
        original = state.elkversion
        self.addCleanup(setattr, state, "elkversion", original)
        state.elkversion = '7"open'  # a quote must not break the exposition format
        body = self.client.get("/api/v1/metrics").get_data(as_text=True)
        self.assertIn('elastic_version="7\\"open"', body)


if __name__ == "__main__":
    unittest.main()
