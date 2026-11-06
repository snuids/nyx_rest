"""Who may publish on the broker, and who may read the account index.

Two rules that only exist at the route boundary, so testing the helpers alone proves nothing:
`/sendmessage` decides whether a destination is acceptable, and `generic_search` decides whether
the own-record filter is added before the query reaches `loadData`. Both were open: any valid
token could publish anywhere, and an index expression written as a list (`nyx_app,nyx_user`) went
past the filter. The tests assert on what is sent — the body handed to `loadData`, the message
handed to the broker — not on what a fake cluster would have returned.
"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sources"))

from route_harness import RouteTestCase, HAVE_ROUTE_DEPS   # noqa: E402

if HAVE_ROUTE_DEPS:
    import routes.data as data_routes                      # noqa: E402
    import routes.misc as misc_routes                      # noqa: E402
    import routes.auth as auth_routes                      # noqa: E402
else:                                                      # pragma: no cover
    data_routes = misc_routes = auth_routes = None


@unittest.skipUnless(HAVE_ROUTE_DEPS, "route dependencies missing")
class SendMessageTests(RouteTestCase):
    route_modules = (misc_routes,)

    def setUp(self):
        super().setUp()
        self.user = self.login_as("alice@example.com", ["YUL"])
        self.admin = self.login_as("root@example.com", ["admin"])

    def publish(self, destination, token=None):
        return self.call("post", "/api/v1/sendmessage", token or self.user,
                         {"destination": destination, "body": "{\"type\":\"message\"}"})

    def test_a_destination_the_interface_uses_is_published(self):
        code, payload, _ = self.publish("/queue/REST_LOAD_DATA")
        self.assertEqual(code, 200)
        self.assertEqual(payload["error"], "")
        self.assertEqual(len(self.broker.sent_to("/queue/REST_LOAD_DATA")), 1)

    def test_an_ordinary_account_cannot_reach_another_destination(self):
        # The queue workers consume whatever lands here: an arbitrary destination is a way of
        # writing on the platform's internal bus.
        code, payload, _ = self.publish("/queue/NYX_ADMIN")
        self.assertEqual(code, 403)
        self.assertIn("not allowed", payload["error"])
        self.assertEqual(self.broker.messages, [], "nothing may reach the broker")

    def test_an_administrator_may_still_publish_anywhere(self):
        code, payload, _ = self.publish("/queue/ADMIN_DEBUG", token=self.admin)
        self.assertEqual(code, 200)
        self.assertEqual(len(self.broker.sent_to("/queue/ADMIN_DEBUG")), 1)

    def test_no_token_no_publication(self):
        code, payload, _ = self.call("post", "/api/v1/sendmessage", None,
                                     {"destination": "/queue/REST_LOAD_DATA", "body": "{}"})
        self.assertEqual(payload["error"], "NO_TOKEN")
        self.assertEqual(self.broker.messages, [])


@unittest.skipUnless(HAVE_ROUTE_DEPS, "route dependencies missing")
class AccountIndexListTests(RouteTestCase):
    """`nyx_app,nyx_user` is one expression and two indices: the filter must be added."""

    route_modules = (data_routes,)

    def setUp(self):
        super().setUp()
        self.user = self.login_as("alice@example.com", ["YUL"])
        self.captured = self.capture_load_data(data_routes)

    def search(self, index):
        body = {"query": {"bool": {"must": [{"query_string": {"query": "*"}}]}}}
        return self.call("post", "/api/v1/generic_search/" + index, self.user, body)

    def filters(self):
        """The `bool.filter` clauses of the query handed to `loadData`."""
        query = self.captured["query"] or {}
        return ((query.get("bool") or {}).get("filter") or [])

    def test_a_list_naming_the_account_index_is_filtered(self):
        code, payload, _ = self.search("nyx_app,nyx_user")
        self.assertEqual(code, 200)
        self.assertEqual(self.captured["calls"], 1)
        self.assertTrue(self.filters(), "the own-record filter must be added to a list expression")
        # …and it is the account filter: the caller's identifiers, not a constant.
        rendered = str(self.filters())
        self.assertIn("alice@example.com", rendered)

    def test_a_list_without_the_account_index_is_left_alone(self):
        code, payload, _ = self.search("nyx_app,nyx_apicalls")
        self.assertEqual(code, 200)
        self.assertEqual(self.filters(), [])

    def test_the_plain_account_index_is_still_filtered(self):
        self.search("nyx_user")
        self.assertIn("alice@example.com", str(self.filters()))


@unittest.skipUnless(HAVE_ROUTE_DEPS, "route dependencies missing")
class IndexPrivilegeTests(RouteTestCase):
    """An index guarded by a rule is refused before `loadData` is even reached."""

    route_modules = (data_routes,)

    RULE = {"_source": {"indicepattern": "^dossier$", "privileges": "YUL"}}

    def setUp(self):
        super().setUp()
        self.state.indices = [self.RULE]
        self.captured = self.capture_load_data(data_routes)

    def search(self, privileges, index="dossier"):
        token = self.login_as("alice@example.com", privileges)
        body = {"query": {"bool": {"must": [{"query_string": {"query": "*"}}]}}}
        return self.call("post", "/api/v1/generic_search/" + index, token, body)

    def test_the_index_privilege_is_required(self):
        code, payload, _ = self.search(["A1"])
        self.assertEqual(payload.get("error"), "Not Allowed")
        self.assertEqual(self.captured["calls"], 0, "the export must not be attempted")

    def test_the_index_privilege_opens_the_index(self):
        code, payload, _ = self.search(["YUL"])
        self.assertEqual(code, 200)
        self.assertEqual(self.captured["calls"], 1)

    def test_an_administrator_still_needs_the_privilege_declared_on_the_index(self):
        # No admin bypass on this rule: an index lists who may read it, and `admin` has to be in
        # the list like any other privilege. Worth pinning — it is easy to assume otherwise.
        self.search(["admin"])
        self.assertEqual(self.captured["calls"], 0)


@unittest.skipUnless(HAVE_ROUTE_DEPS, "route dependencies missing")
class AnonymousSendMessageTests(RouteTestCase):
    route_modules = (misc_routes,)

    def test_an_anonymous_caller_cannot_publish(self):
        code, payload, _ = self.call("post", "/api/v1/sendmessage", None,
                                     {"destination": "/queue/REST_LOAD_DATA", "body": "{}"})
        self.assertEqual(payload.get("error"), "NO_TOKEN")
        self.assertEqual(self.broker.messages, [])


@unittest.skipUnless(HAVE_ROUTE_DEPS, "route dependencies missing")
class AnonymousDataTests(RouteTestCase):
    route_modules = (data_routes,)

    def test_the_crud_route_needs_a_token(self):
        code, payload, _ = self.call("get", "/api/v1/pg_generic/nyx_audit/id/1", None)
        self.assertEqual(payload.get("error"), "NO_TOKEN")

    def test_the_datasource_route_needs_a_token(self):
        code, payload, _ = self.call("get", "/api/v1/datasource/whatever", None)
        self.assertEqual(payload.get("error"), "NO_TOKEN")


@unittest.skipUnless(HAVE_ROUTE_DEPS, "route dependencies missing")
class ResetPasswordRoleTests(RouteTestCase):
    route_modules = (auth_routes,)

    def setUp(self):
        super().setUp()
        self.user = self.login_as("alice@example.com", ["YUL"])

    def test_an_anonymous_caller_cannot_reset_a_password(self):
        code, payload, _ = self.call("post", "/api/v1/cred/resetpassword", None,
                                     {"login": "bob@example.com", "new_password": "x"})
        self.assertEqual(payload.get("error"), "NO_TOKEN")

    def test_an_ordinary_account_is_refused(self):
        # `useradmin` is enough for the route — that a `useradmin` can reset an administrator's
        # password is the open question, and deliberately not decided here: this test only pins
        # that an ordinary account is refused.
        code, payload, _ = self.call("post", "/api/v1/cred/resetpassword", self.user,
                                     {"login": "bob@example.com", "new_password": "x"})
        self.assertEqual(payload.get("error"), "NO_PRIVILEGE")


if __name__ == "__main__":
    unittest.main()