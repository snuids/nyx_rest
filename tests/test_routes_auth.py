"""Routes d'authentification : login, garde anti-force-brute, bridage du premier login, logout.

`routes/auth.py` était à 0 % de couverture alors qu'il porte le seul point d'entrée non
authentifié de la plateforme. Les tests ci-dessous exercent la route réelle (décorateurs compris,
donc `token_required` et le garde) contre un faux cluster et un faux Redis.
"""

import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sources"))

from route_harness import RouteTestCase, HAVE_ROUTE_DEPS   # noqa: E402

if HAVE_ROUTE_DEPS:
    import routes.auth as auth_routes                      # noqa: E402
    import routes.misc as misc_routes                      # noqa: E402
    import routes.data as data_routes                      # noqa: E402
    from passlib.hash import pbkdf2_sha256                 # noqa: E402
    PASSWORD = "Correct#Pass2026"
    OTHER_PASSWORD = "Autre#Pass2026"
else:                                                      # pragma: no cover
    auth_routes = misc_routes = data_routes = pbkdf2_sha256 = None
    PASSWORD = OTHER_PASSWORD = ""


def users():
    """Deux comptes : un simple et un administrateur."""
    return {
        "alice@example.com": {"login": "alice@example.com", "privileges": ["YUL"],
                              "password": pbkdf2_sha256.hash(PASSWORD),
                              "language": "en", "filters": []},
        "bob@example.com": {"login": "bob@example.com", "privileges": ["admin"],
                            "password": pbkdf2_sha256.hash(OTHER_PASSWORD),
                            "language": "en", "filters": []},
    }


@unittest.skipUnless(auth_routes is not None, "routes.auth non importable")
class LoginTests(RouteTestCase):
    route_modules = (auth_routes, misc_routes)

    def setUp(self):
        super().setUp()
        self.es.docs["nyx_user"] = users()

    def _login(self, login, password, **extra):
        payload = {"login": login, "password": password}
        payload.update(extra)
        return self.call("post", "/api/v1/cred/login", body=payload)

    def test_a_bad_password_gets_one_indistinguishable_message(self):
        code, body, _ = self._login("alice@example.com", "mauvais")
        self.assertEqual(body["error"], "Bad Credentials")
        self.assertEqual(self.state.auth_failures_total, 1)

    def test_an_unknown_account_gets_the_same_message(self):
        code, body, _ = self._login("inconnu@example.com", "mauvais")
        self.assertEqual(body["error"], "Bad Credentials")

    def test_a_failed_login_is_counted_per_account_and_per_ip(self):
        self._login("alice@example.com", "mauvais")
        self.assertEqual(int(self.redis.get("loginfail:user:alice@example.com")), 1)
        ip_keys = [k for k in self.redis.store if k.startswith("loginfail:ip:")]
        self.assertEqual(len(ip_keys), 1, "un compteur par IP doit aussi exister")

    def test_the_guard_answers_429_after_five_failures(self):
        for _ in range(5):
            self._login("alice@example.com", "mauvais")
        code, body, response = self._login("alice@example.com", "mauvais")
        self.assertEqual(body["error"], "TOO_MANY_ATTEMPTS")
        self.assertIn("retry_after", body)
        self.assertGreater(body["retry_after"], 0)
        self.assertEqual(response.status_code, 429)
        self.assertEqual(self.state.auth_blocked_total, 1)

    def test_a_successful_login_mints_a_token_and_keeps_the_password_out_of_it(self):
        code, body, response = self._login("alice@example.com", PASSWORD)
        self.assertEqual(body["error"], "")
        token = body["cred"]["token"]
        self.assertTrue(token)
        self.assertEqual(self.redis.get("nyx_tok_" + token) is not None, True,
                         "le payload doit etre stocke dans Redis")
        user = body["cred"]["user"]
        self.assertEqual(user["password"], "", "le condensat ne doit pas revenir au client")
        self.assertEqual(user["privileges"], ["YUL"])
        cookies = response.headers.getlist("Set-Cookie")
        self.assertTrue(any("nyx_kibananyx=" in c for c in cookies))
        self.assertEqual(token in self.state.tokens, True)

    def test_a_login_clears_the_failure_counters(self):
        self._login("alice@example.com", "mauvais")
        self._login("alice@example.com", PASSWORD)
        self.assertNotIn("loginfail:user:alice@example.com", self.redis.store)

    def test_a_non_admin_cannot_impersonate_through_the_arrow_syntax(self):
        # « admin>autre » est reserve aux administrateurs : un simple compte doit atterrir sur
        # le sien, c'est-a-dire echouer ici puisque le mot de passe est celui d'alice
        code, body, _ = self._login("alice@example.com>bob@example.com", PASSWORD)
        self.assertEqual(body["error"], "")
        self.assertEqual(body["cred"]["user"]["login"], "alice@example.com")
        self.assertEqual(body["cred"]["user"]["privileges"], ["YUL"])

    def test_a_malformed_body_does_not_reach_the_cluster(self):
        code, body, _ = self.call("post", "/api/v1/cred/login", body={"foo": "bar"})
        self.assertEqual(body["error"], "Bad Request")
        self.assertEqual(self.es.calls, [])


@unittest.skipUnless(auth_routes is not None, "routes.auth non importable")
class BridedPasswordChangeTests(RouteTestCase):
    """Parcours « premier login » : bridage, changement, debridage immediat."""

    route_modules = (auth_routes, misc_routes, data_routes)

    def setUp(self):
        super().setUp()
        accounts = users()
        accounts["alice@example.com"]["mustResetPassword"] = True
        self.es.docs["nyx_user"] = accounts
        code, body, _ = self.call("post", "/api/v1/cred/login",
                                  body={"login": "alice@example.com", "password": PASSWORD})
        self.assertEqual(body["error"], "", "le login doit aboutir malgre le drapeau")
        self.token = body["cred"]["token"]
        self.assertTrue(body["cred"]["user"]["mustResetPassword"])

    def test_every_route_is_closed_except_the_password_change(self):
        for path in ("/api/v1/status", "/api/v1/generic/nyx_user/alice@example.com",
                     "/api/v1/generic_search/nyx_user"):
            code, body, _ = self.call("post" if "search" in path else "get", path, self.token,
                                      {"size": 5} if "search" in path else None)
            self.assertEqual(body["error"], "MUST_CHANGE_PASSWORD", path)

    def test_changing_the_password_unbridles_the_current_token_at_once(self):
        code, body, _ = self.call("post", "/api/v1/cred/changepassword", self.token,
                                  {"old_password": PASSWORD, "new_password": "Nouveau#Pass2026"})
        self.assertEqual(body["error"], "")
        # le meme jeton doit passer immediatement, y compris sur une autre requete
        for _ in range(3):
            code, body, _ = self.call("get", "/api/v1/status", self.token)
            self.assertEqual(body["error"], "", "le jeton doit etre debride sans reconnexion")
        # et la copie Redis doit porter le drapeau retombe
        stored = json.loads(self.redis.get("nyx_tok_" + self.token))
        self.assertFalse(stored["mustResetPassword"])

    def test_the_stored_password_is_replaced_and_the_flag_cleared(self):
        self.call("post", "/api/v1/cred/changepassword", self.token,
                  {"old_password": PASSWORD, "new_password": "Nouveau#Pass2026"})
        stored = self.es.docs["nyx_user"]["alice@example.com"]
        self.assertFalse(stored["mustResetPassword"])
        self.assertTrue(stored["password"].startswith("$pbkdf2-sha256$"))
        self.assertTrue(pbkdf2_sha256.verify("Nouveau#Pass2026", stored["password"]))

    def test_the_old_password_stops_working_after_the_change(self):
        self.call("post", "/api/v1/cred/changepassword", self.token,
                  {"old_password": PASSWORD, "new_password": "Nouveau#Pass2026"})
        code, body, _ = self.call("post", "/api/v1/cred/login",
                                  body={"login": "alice@example.com", "password": PASSWORD})
        self.assertEqual(body["error"], "Bad Credentials")
        code, body, _ = self.call("post", "/api/v1/cred/login",
                                  body={"login": "alice@example.com",
                                        "password": "Nouveau#Pass2026"})
        self.assertEqual(body["error"], "")

    def test_a_wrong_current_password_changes_nothing(self):
        before = self.es.docs["nyx_user"]["alice@example.com"]["password"]
        code, body, _ = self.call("post", "/api/v1/cred/changepassword", self.token,
                                  {"old_password": "faux", "new_password": "Nouveau#Pass2026"})
        self.assertEqual(body["error"], "wrongpassword")
        self.assertEqual(self.es.docs["nyx_user"]["alice@example.com"]["password"], before)
        self.assertEqual(self.es.written, [])

    def test_a_too_short_password_is_refused_by_the_policy(self):
        code, body, _ = self.call("post", "/api/v1/cred/changepassword", self.token,
                                  {"old_password": PASSWORD, "new_password": "court"})
        self.assertIn("caractères", body["error"])
        self.assertEqual(self.es.written, [])

    def test_a_missing_field_is_refused_before_any_work(self):
        code, body, _ = self.call("post", "/api/v1/cred/changepassword", self.token,
                                  {"old_password": PASSWORD})
        self.assertTrue(body["error"].startswith("MISSING_PARAM"))


@unittest.skipUnless(auth_routes is not None, "routes.auth non importable")
class LogoutTests(RouteTestCase):
    route_modules = (auth_routes, misc_routes)
    documents = {"nyx_user": {}}

    def setUp(self):
        super().setUp()
        self.token = self.login_as("alice@example.com", ["YUL"])

    def test_logout_revokes_the_token_everywhere(self):
        code, body, _ = self.call("get", "/api/v1/cred/logout", self.token)
        self.assertEqual(body["error"], "")
        self.assertNotIn("nyx_tok_" + self.token, self.redis.store)
        self.assertNotIn(self.token, self.state.tokens)
        code, body, _ = self.call("get", "/api/v1/status", self.token)
        self.assertEqual(body["error"], "UNKNOWN_TOKEN")

    def test_logout_announces_the_revocation_on_the_broker(self):
        self.call("get", "/api/v1/cred/logout", self.token)
        self.assertEqual(self.broker.sent_to("/topic/LOGOUT_EVENT"), [self.token])

    def test_an_unknown_token_is_refused(self):
        code, body, _ = self.call("get", "/api/v1/status", "00000000-0000-0000-0000-000000000000")
        self.assertEqual(body["error"], "UNKNOWN_TOKEN")


if __name__ == "__main__":
    unittest.main()
