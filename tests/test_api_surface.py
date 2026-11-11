"""
Tests for what the API exposes before anyone logs in.

Three holes were closed together, and each of them is a promise the API makes to the network
rather than to a user, so each one is tested here rather than through the console:

* the Onfleet webhook echoed its `check` parameter as `text/html` (a reflected XSS on the API
  origin) and published any payload to the broker without authentication;
* `?apikey=` accepted a console session token, so `/lambdas` ran for any logged-in account;
* every refusal of `token_required` came back as HTTP 200, invisible to a probe or an alert.

The last one is tested on both sides: the decorator answers 401/403, and a route-level error
(an `{"error": …}` that is not a refusal) keeps its 200, because the console reads that body in
`.then()` and a client that lost the habit would show nothing at all.
"""

import sys
import unittest
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sources"))

from route_harness import RouteTestCase, HAVE_ROUTE_DEPS, DEPS_MISSING  # noqa: E402

from config import settings  # noqa: E402
import middleware  # noqa: E402
import state  # noqa: E402


def mount_misc(test_case):
    """Registers `routes.misc` on an application bound to the fakes of `test_case`."""
    from flask import Flask
    from flask_restx import Api
    from routes import misc

    app = Flask(__name__)
    api = Api(app, doc="/api/doc/")
    misc.register(app, api, api.namespace("api/v1"))
    test_case.client = app.test_client()
    return app


class SettingsMixin:
    """Temporarily sets an attribute of the `settings` singleton."""

    def set_setting(self, name, value):
        saved = getattr(settings, name)
        setattr(settings, name, value)
        self.addCleanup(setattr, settings, name, saved)


@unittest.skipUnless(HAVE_ROUTE_DEPS, "route dependencies missing (" + DEPS_MISSING + ")")
class OnfleetWebhookTests(SettingsMixin, RouteTestCase):
    """`/api/v1/onfleet_webhook` is public: it must stay harmless, and it must not relay."""

    route_modules = ()

    def test_the_validation_token_is_echoed_as_text(self):
        mount_misc(self)
        response = self.client.get("/api/v1/onfleet_webhook?check=abc123")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data, b"abc123", "Onfleet expects its own value back")
        self.assertEqual(response.mimetype, "text/plain")
        self.assertEqual(response.headers.get("X-Content-Type-Options"), "nosniff")

    def test_a_script_in_the_validation_token_is_not_a_document(self):
        mount_misc(self)
        response = self.client.get(
            "/api/v1/onfleet_webhook?check=%3Cscript%3Ealert(1)%3C/script%3E"
        )
        self.assertEqual(response.mimetype, "text/plain",
                         "an HTML mimetype here is a reflected XSS on the API origin")
        self.assertNotIn("text/html", response.headers.get("Content-Type", ""))

    def test_a_missing_check_parameter_does_not_break_the_route(self):
        mount_misc(self)
        response = self.client.get("/api/v1/onfleet_webhook")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data, b"")

    def test_no_configured_secret_refuses_the_payload(self):
        self.set_setting("ONFLEET_WEBHOOK_SECRET", "")
        mount_misc(self)
        response = self.client.post("/api/v1/onfleet_webhook", json={"id": "t"})
        self.assertEqual(response.status_code, 503)
        self.assertEqual(self.broker.sent_to("/topic/ONFLEET_WEBHOOK"), [],
                         "an unauthenticated payload must not reach the broker")

    def test_a_wrong_secret_is_refused(self):
        self.set_setting("ONFLEET_WEBHOOK_SECRET", "s3cret")
        mount_misc(self)
        response = self.client.post(
            "/api/v1/onfleet_webhook", json={"id": "t"},
            headers={"X-Nyx-Webhook-Secret": "guess"},
        )
        self.assertEqual(response.status_code, 403)
        self.assertEqual(self.broker.sent_to("/topic/ONFLEET_WEBHOOK"), [])

    def test_the_configured_secret_publishes_the_payload(self):
        self.set_setting("ONFLEET_WEBHOOK_SECRET", "s3cret")
        mount_misc(self)
        response = self.client.post(
            "/api/v1/onfleet_webhook", json={"id": "task-1"},
            headers={"X-Nyx-Webhook-Secret": "s3cret"},
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["error"], "")
        self.assertEqual(len(self.broker.sent_to("/topic/ONFLEET_WEBHOOK")), 1)

    def test_the_secret_may_travel_in_the_url(self):
        self.set_setting("ONFLEET_WEBHOOK_SECRET", "s3cret")
        mount_misc(self)
        response = self.client.post("/api/v1/onfleet_webhook?secret=s3cret", json={"id": "task-2"})
        self.assertEqual(response.status_code, 200)


@unittest.skipUnless(HAVE_ROUTE_DEPS, "route dependencies missing (" + DEPS_MISSING + ")")
class ApiKeyTests(RouteTestCase):
    """`?apikey=` is a machine credential; a console session token is not one."""

    route_modules = ()

    def check(self, key):
        from flask import request
        app = mount_misc(self)
        with app.test_request_context("/api/v1/lambdas/runner/name?apikey=" + key):
            return middleware.checkAPIKey(request)

    def test_a_console_session_token_is_not_an_api_key(self):
        token = self.login_as("someone", ["user"])
        self.assertFalse(self.check(token),
                         "a session token must not open the machine-only route")

    def test_an_unknown_key_is_refused(self):
        self.assertFalse(self.check("not-a-key"))

    def test_an_absent_key_is_refused(self):
        self.assertFalse(self.check(""))

    def test_a_declared_api_key_is_accepted(self):
        # `getAPIKey` is cached for a minute: a key used by another test must not come back
        # from the cache, hence a fresh identifier per test.
        key = "key-" + str(uuid.uuid4())
        self.es.docs.setdefault("nyx_apikey", {})[key] = {"_source": {"name": "integration"}}
        self.assertTrue(self.check(key))

    def test_an_unreachable_cluster_refuses(self):
        def boom(*args, **kwargs):
            raise ConnectionError("cluster down")

        self.es.get = boom
        self.assertFalse(self.check("key-" + str(uuid.uuid4())),
                         "an authorisation check cannot answer yes when the store does not")


class UnusedRedis:
    def get(self, key):
        return None


class UnusedES:
    def get(self, *args, **kwargs):
        raise ConnectionError("not used in these tests")


@unittest.skipUnless(HAVE_ROUTE_DEPS, "route dependencies missing (" + DEPS_MISSING + ")")
class AuthStatusTests(unittest.TestCase):
    """The refusals of `token_required` carry an HTTP status, their bodies keep their shape."""

    @classmethod
    def setUpClass(cls):
        from flask import Flask
        from flask_restx import Api, Resource
        from middleware import token_required

        cls.app = Flask(__name__)
        api = Api(cls.app, doc="/api/doc/")
        namespace = api.namespace("api/v1")

        @namespace.route("/protected")
        class Protected(Resource):
            @token_required()
            def get(self, user=None):
                return {"error": "", "login": user["login"]}

        @namespace.route("/restricted")
        class Restricted(Resource):
            @token_required("reportadmin")
            def get(self, user=None):
                return {"error": "", "login": user["login"]}

        @namespace.route("/failing")
        class Failing(Resource):
            @token_required()
            def get(self, user=None):
                return {"error": "unable to post data"}

        cls.client = cls.app.test_client()

    def setUp(self):
        self._saved = (state.tokens, state.redisserver, state.es,
                       state.requests_total, state.errors_total)
        state.tokens = {}
        state.redisserver = UnusedRedis()
        state.es = UnusedES()
        state.requests_total = 0
        state.errors_total = 0
        self.addCleanup(self.restore)

    def restore(self):
        (state.tokens, state.redisserver, state.es,
         state.requests_total, state.errors_total) = self._saved

    def login(self, privileges=("user",), must_reset=False):
        token = str(uuid.uuid4())
        state.tokens[token] = {
            "login": "tester", "id": "tester", "privileges": list(privileges),
            "language": "en", "mustResetPassword": must_reset,
        }
        return token

    def test_no_token_is_a_401(self):
        response = self.client.get("/api/v1/protected")
        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.get_json()["error"], "NO_TOKEN")
        self.assertIn("Bearer", response.headers.get("WWW-Authenticate", ""))

    def test_an_unknown_token_is_a_401(self):
        response = self.client.get("/api/v1/protected?token=nope")
        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.get_json()["error"], "UNKNOWN_TOKEN")

    def test_a_missing_privilege_is_a_403(self):
        token = self.login(["user"])
        response = self.client.get("/api/v1/restricted?token=" + token)
        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.get_json()["error"], "NO_PRIVILEGE")

    def test_the_privilege_holder_is_served(self):
        token = self.login(["reportadmin"])
        response = self.client.get("/api/v1/restricted?token=" + token)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["error"], "")

    def test_a_provisional_password_is_a_403(self):
        token = self.login(["user"], must_reset=True)
        response = self.client.get("/api/v1/protected?token=" + token)
        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.get_json()["error"], "MUST_CHANGE_PASSWORD")

    def test_a_route_error_keeps_its_200(self):
        token = self.login()
        response = self.client.get("/api/v1/failing?token=" + token)
        self.assertEqual(response.status_code, 200,
                         "the console reads route errors in .then(); changing that breaks it")
        self.assertEqual(response.get_json()["error"], "unable to post data")

    def test_the_refusal_is_still_counted(self):
        self.client.get("/api/v1/protected")
        self.assertEqual(state.requests_total, 1)
        self.assertEqual(state.errors_total, 1)


if __name__ == "__main__":
    unittest.main()
