"""Generic routes: who may read which document, and who may write to which index.

These routes carried on their own the escalation surface found in the audit: an account with
a mere application tag read everyone's hashes and could write to `nyx_user`, `nyx_app`,
`nyx_privilege`, `nyx_indice` and `nyx_filter`. The tests below bear on the *wiring* —
refusal before the cluster, request body produced — not on the access logic, which has its
own tests (`test_nyx_user_access.py`, `test_can_use_indice.py`).
"""

import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sources"))

from route_harness import RouteTestCase, HAVE_ROUTE_DEPS   # noqa: E402

if HAVE_ROUTE_DEPS:
    import routes.data as data_routes                      # noqa: E402
else:                                                      # pragma: no cover
    data_routes = None


USERS = {
    "alice@example.com": {"login": "alice@example.com", "privileges": ["YUL"],
                          "password": "$pbkdf2-sha256$29000$ALICEHASH"},
    "bob@example.com": {"login": "bob@example.com", "privileges": ["admin"],
                        "password": "$pbkdf2-sha256$29000$BOBHASH"},
}


@unittest.skipUnless(data_routes is not None, "routes.data non importable")
class GenericReadTests(RouteTestCase):
    route_modules = (data_routes,)
    documents = {"nyx_user": USERS}

    def setUp(self):
        super().setUp()
        self.alice = self.login_as("alice@example.com", ["YUL"])
        self.admin = self.login_as("bob@example.com", ["admin"])

    def test_a_non_admin_cannot_read_another_account(self):
        code, body, _ = self.call("get", "/api/v1/generic/nyx_user/bob@example.com", self.alice)
        self.assertEqual(body["error"], "Not Allowed")
        self.assertIsNone(body["data"])
        self.assertFalse(self.es.was_touched("nyx_user", "bob@example.com"),
                         "le document ne doit meme pas etre demande au cluster")

    def test_a_non_admin_reads_their_own_account(self):
        code, body, _ = self.call("get", "/api/v1/generic/nyx_user/alice@example.com", self.alice)
        self.assertEqual(body["error"], "")
        self.assertEqual(body["data"]["_id"], "alice@example.com")

    def test_the_reindex_alias_is_guarded_too(self):
        code, body, _ = self.call(
            "get", "/api/v1/generic/reindexed-nyx_user-v8/bob@example.com", self.alice)
        self.assertEqual(body["error"], "Not Allowed")

    def test_an_admin_reads_any_account(self):
        code, body, _ = self.call("get", "/api/v1/generic/nyx_user/alice@example.com", self.admin)
        self.assertEqual(body["error"], "")
        self.assertEqual(body["data"]["_id"], "alice@example.com")

    def test_an_anonymous_call_is_refused_before_anything_else(self):
        code, body, _ = self.call("get", "/api/v1/generic/nyx_user/alice@example.com")
        self.assertEqual(body["error"], "NO_TOKEN")
        self.assertEqual(self.es.calls, [])

    def test_the_token_in_the_query_string_is_accepted(self):
        # the console still sends the token as a parameter: that contract must not break
        code, body, _ = self.call(
            "get", "/api/v1/generic/nyx_user/alice@example.com?token=" + self.alice)
        self.assertEqual(body["error"], "")


@unittest.skipUnless(data_routes is not None, "routes.data non importable")
class GenericWriteTests(RouteTestCase):
    route_modules = (data_routes,)
    documents = {"nyx_user": USERS, "airport_alert-2026.11": {"1": {"site": "A"}}}

    def setUp(self):
        super().setUp()
        self.alice = self.login_as("alice@example.com", ["YUL"])
        self.admin = self.login_as("bob@example.com", ["admin"])

    def test_a_non_admin_cannot_create_or_delete_an_account(self):
        for method, body in (("post", {"login": "bob@example.com", "privileges": ["admin"]}),
                             ("delete", None)):
            code, response, _ = self.call(method, "/api/v1/generic/nyx_user/bob@example.com",
                                          self.alice, body)
            self.assertEqual(response["error"], "Not Allowed", method)
        self.assertEqual(self.es.written, [], "aucune ecriture ne doit atteindre le cluster")
        self.assertEqual(self.es.deleted, [])

    def test_a_non_admin_cannot_promote_themselves(self):
        code, response, _ = self.call("post", "/api/v1/generic/nyx_user/alice@example.com",
                                      self.alice,
                                      {"login": "alice@example.com", "privileges": ["admin"]})
        self.assertEqual(response["error"], "Not Allowed")
        self.assertEqual(self.es.written, [])

    def test_the_configuration_indices_are_closed_to_a_non_admin(self):
        for index in ("nyx_app", "nyx_privilege", "nyx_indice", "nyx_filter"):
            code, response, _ = self.call("post", "/api/v1/generic/%s/x" % index,
                                          self.alice, {"anything": 1})
            self.assertEqual(response["error"], "Not Allowed", index)
        self.assertEqual(self.es.written, [])

    def test_the_alias_and_the_wildcard_do_not_open_a_back_door(self):
        for index in ("reindexed-nyx_user-v8", "nyx_user*", "nyx_*"):
            code, response, _ = self.call("post", "/api/v1/generic/%s/x" % index,
                                          self.alice, {"login": "x"})
            self.assertEqual(response["error"], "Not Allowed", index)
        self.assertEqual(self.es.written, [])

    def test_a_non_admin_still_writes_in_a_business_index(self):
        code, response, _ = self.call("post", "/api/v1/generic/airport_alert-2026.11/2",
                                      self.alice, {"site": "B"})
        self.assertEqual(response["error"], "")
        self.assertEqual(len(self.es.written_on("airport_alert-2026.11")), 1)

    def test_an_admin_creates_an_account_and_the_password_is_hashed(self):
        code, response, _ = self.call(
            "post", "/api/v1/generic/nyx_user/new@example.com", self.admin,
            {"login": "new@example.com", "privileges": ["YUL"], "password": "MotDePasse#2026"})
        self.assertEqual(response["error"], "")
        written = self.es.written_on("nyx_user")
        self.assertEqual(len(written), 1, "l'ecriture doit partir une fois")
        self.assertTrue(str(written[0][1]["password"]).startswith("$pbkdf2-sha256$"))
        self.assertNotIn("MotDePasse#2026", json.dumps(written[0][1]),
                         "le mot de passe ne doit jamais partir en clair")

    def test_an_account_created_without_password_stays_closed(self):
        self.call("post", "/api/v1/generic/nyx_user/x@example.com", self.admin,
                  {"login": "x@example.com"})
        stored = self.es.written_on("nyx_user")[0][1]
        self.assertTrue(stored["mustResetPassword"],
                        "un compte sans mot de passe utilisable doit rester ferme")
        self.assertTrue(str(stored["password"]).startswith("$pbkdf2-sha256$"))

    def test_an_admin_deletes_an_account(self):
        code, response, _ = self.call("delete", "/api/v1/generic/nyx_user/alice@example.com",
                                      self.admin)
        self.assertEqual(response["error"], "")
        self.assertEqual(self.es.deleted, [("nyx_user", "alice@example.com")])


@unittest.skipUnless(data_routes is not None, "routes.data non importable")
class GenericSearchTests(RouteTestCase):
    route_modules = (data_routes,)
    documents = {"nyx_user": USERS, "airport_alert-2026.11": {"1": {"site": "A"}}}

    def setUp(self):
        super().setUp()
        self.alice = self.login_as("alice@example.com", ["YUL"])
        self.admin = self.login_as("bob@example.com", ["admin"])
        self.captured = self.capture_load_data(data_routes)

    def _filter_of(self, index):
        body = self.captured["query"] or {}
        return json.dumps((body.get("bool") or {}).get("filter"))

    def test_a_search_on_nyx_user_is_restricted_to_the_account(self):
        code, body, _ = self.call("post", "/api/v1/generic_search/nyx_user", self.alice,
                                  {"size": 10})
        self.assertEqual(body["error"], "")
        self.assertEqual(self.captured["index"], "nyx_user")
        filters = self._filter_of("nyx_user")
        self.assertIn("alice@example.com", filters)
        self.assertNotIn("bob@example.com", filters)

    def test_the_wildcard_form_is_restricted_too(self):
        # this is the form the interface sends to list the accounts
        self.call("post", "/api/v1/generic_search/nyx_user*", self.alice, {"size": 10})
        self.assertEqual(self.captured["index"], "nyx_user*")
        self.assertIn("alice@example.com", self._filter_of("nyx_user*"))

    def test_the_alias_form_is_restricted_too(self):
        self.call("post", "/api/v1/generic_search/reindexed-nyx_user-v8", self.alice,
                  {"size": 10})
        self.assertIn("alice@example.com", self._filter_of("reindexed-nyx_user-v8"))

    def test_an_admin_search_is_not_restricted(self):
        self.call("post", "/api/v1/generic_search/nyx_user", self.admin, {"size": 10})
        self.assertEqual(self._filter_of("nyx_user"), "null")

    def test_a_business_index_search_is_untouched(self):
        self.call("post", "/api/v1/generic_search/airport_alert-2026.11", self.alice,
                  {"size": 10})
        self.assertEqual(self.captured["index"], "airport_alert-2026.11")
        self.assertEqual(self._filter_of("airport_alert-2026.11"), "null")

    def test_a_token_without_identity_is_refused_rather_than_broadened(self):
        anonymous = self.login_as("", ["YUL"])
        code, body, _ = self.call("post", "/api/v1/generic_search/nyx_user", anonymous,
                                  {"size": 10})
        self.assertEqual(body["error"], "Not Allowed")


if __name__ == "__main__":
    unittest.main()
