"""Authentication routes: login, brute-force guard, first-login throttling, logout.

`routes/auth.py` was at 0 % coverage although it carries the only unauthenticated entry point
of the platform. The tests below exercise the real route (decorators included, hence
`token_required` and the guard) against a fake cluster and a fake Redis.
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
    """Two accounts: one plain and one administrator."""
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
        # "admin>other" is reserved for administrators: a plain account must land on its
        # own, that is, fail here since the password is alice's
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
        # the same token must pass immediately, including on another request
        for _ in range(3):
            code, body, _ = self.call("get", "/api/v1/status", self.token)
            self.assertEqual(body["error"], "", "le jeton doit etre debride sans reconnexion")
        # and the Redis copy must carry the cleared flag
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
        self.assertIn("characters", body["error"])
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


@unittest.skipUnless(auth_routes is not None, "routes.auth non importable")
class ResetPasswordTests(RouteTestCase):
    """An administrator resets a password: the result is provisional, never final."""

    route_modules = (auth_routes, misc_routes)

    def setUp(self):
        super().setUp()
        self.es.docs["nyx_user"] = users()
        self.admin = self.login_as("bob@example.com", ["admin"])
        self.plain = self.login_as("alice@example.com", ["YUL"])

    def test_an_admin_can_reset_and_the_new_password_is_provisional(self):
        code, body, _ = self.call("post", "/api/v1/cred/resetpassword", self.admin,
                                  {"login": "alice@example.com", "new_password": "Provisoire#2026"})
        self.assertEqual(body["error"], "")
        stored = self.es.docs["nyx_user"]["alice@example.com"]
        self.assertTrue(stored["mustResetPassword"],
                        "a password set by an administrator must stay provisional")
        self.assertTrue(pbkdf2_sha256.verify("Provisoire#2026", stored["password"]))

    def test_a_plain_account_cannot_reset_another_one(self):
        code, body, _ = self.call("post", "/api/v1/cred/resetpassword", self.plain,
                                  {"login": "alice@example.com", "new_password": "Provisoire#2026"})
        self.assertEqual(body["error"], "NO_PRIVILEGE")
        self.assertEqual(self.es.written, [])

    def test_resetting_an_unknown_account_says_so(self):
        code, body, _ = self.call("post", "/api/v1/cred/resetpassword", self.admin,
                                  {"login": "inconnu@example.com", "new_password": "Provisoire#2026"})
        self.assertEqual(body["error"], "usernotfound")
        self.assertEqual(self.es.written, [])

    def test_the_optional_queue_receives_the_new_password(self):
        self.call("post", "/api/v1/cred/resetpassword", self.admin,
                  {"login": "alice@example.com", "new_password": "Provisoire#2026",
                   "queue": "/topic/NOTIFY"})
        sent = self.broker.sent_to("/topic/NOTIFY")
        self.assertEqual(len(sent), 1)
        self.assertEqual(json.loads(sent[0])["newpassword"], "Provisoire#2026")


@unittest.skipUnless(auth_routes is not None, "routes.auth non importable")
class DoublePhaseTests(RouteTestCase):
    """Second factor by SMS: the code goes to the broker, never into the response."""

    route_modules = (auth_routes, misc_routes)

    def setUp(self):
        super().setUp()
        accounts = users()
        accounts["alice@example.com"].update({"doublePhase": True, "phone": "+32470000000"})
        self.es.docs["nyx_user"] = accounts

    def _login(self, **extra):
        payload = {"login": "alice@example.com", "password": PASSWORD}
        payload.update(extra)
        return self.call("post", "/api/v1/cred/login", body=payload)

    def test_the_first_call_sends_a_code_instead_of_a_session(self):
        code, body, _ = self._login()
        self.assertEqual(body["error"], "DoublePhase")
        sent = self.broker.sent_to("/topic/AUTH_SMS")
        self.assertEqual(len(sent), 1)
        message = json.loads(sent[0])
        self.assertEqual(message["phone"], "+32470000000")
        self.assertTrue(message["message"].startswith("Your access code is:"))
        self.assertIn("nyx_double_alice@example.com", self.redis.store,
                      "the code must be stored for the next call")

    def test_a_wrong_code_counts_as_a_failed_attempt(self):
        self._login()
        code, body, _ = self._login(doublecode="00000")
        self.assertEqual(body["error"], "ErrorDoublePhase")
        self.assertEqual(self.state.auth_failures_total, 1)
        self.assertEqual(int(self.redis.get("loginfail:user:alice@example.com")), 1)

    def test_the_right_code_opens_the_session(self):
        self._login()
        stored = self.redis.get("nyx_double_alice@example.com")
        good = stored.decode("ascii")
        code, body, _ = self._login(doublecode=good)
        self.assertEqual(body["error"], "")
        self.assertTrue(body["cred"]["token"])


@unittest.skipUnless(auth_routes is not None, "routes.auth non importable")
class ImpersonationTests(RouteTestCase):
    """`admin>user`: an administrator opens somebody else's session."""

    route_modules = (auth_routes, misc_routes)

    def setUp(self):
        super().setUp()
        self.es.docs["nyx_user"] = users()

    def test_an_admin_can_open_another_account(self):
        code, body, _ = self.call("post", "/api/v1/cred/login",
                                  body={"login": "bob@example.com>alice@example.com",
                                        "password": OTHER_PASSWORD})
        self.assertEqual(body["error"], "")
        self.assertEqual(body["cred"]["user"]["login"], "alice@example.com")
        self.assertEqual(body["cred"]["user"]["privileges"], ["YUL"])

    def test_an_unknown_target_is_refused(self):
        code, body, _ = self.call("post", "/api/v1/cred/login",
                                  body={"login": "bob@example.com>inconnu@example.com",
                                        "password": OTHER_PASSWORD})
        self.assertEqual(body["error"], "Unknown User")


if __name__ == "__main__":
    unittest.main()


@unittest.skipUnless(auth_routes is not None, "routes.auth non importable")
class PreferencesTests(RouteTestCase):
    """`/cred/preferences` : un reglage d'interface par compte, sur son propre document.

    Le theme vit a cote de `language` et `privileges` dans `nyx_user` : il suit donc le compte
    d'un navigateur a l'autre. La route n'ecrit que sur l'appelant — c'est ce que ces tests
    verifient, plutot que la valeur en base seule.
    """

    route_modules = (auth_routes, misc_routes)

    def setUp(self):
        super().setUp()
        self.es.docs["nyx_user"] = users()
        code, body, _ = self.call("post", "/api/v1/cred/login",
                                  body={"login": "alice@example.com", "password": PASSWORD})
        self.assertEqual(body["error"], "")
        self.token = body["cred"]["token"]

    def test_the_theme_lands_on_the_callers_own_document(self):
        before = dict(self.es.docs["nyx_user"]["alice@example.com"])
        code, body, _ = self.call("post", "/api/v1/cred/preferences", self.token,
                                  {"theme": "dark"})
        self.assertEqual(body["error"], "")
        self.assertEqual(code, 200)
        stored = self.es.docs["nyx_user"]["alice@example.com"]
        self.assertEqual(stored["theme"], "dark")
        # rien d'autre ne bouge : ni le mot de passe, ni les privileges, ni la langue
        for field in ("password", "privileges", "language", "login"):
            self.assertEqual(stored[field], before[field], field)

    def test_an_unknown_value_is_refused_before_any_write(self):
        for value in ("purple", "", None, 42, ["dark"]):
            code, body, _ = self.call("post", "/api/v1/cred/preferences", self.token,
                                      {"theme": value})
            self.assertEqual(body["error"], "invalid_value", repr(value))
            self.assertEqual(code, 400)
        self.assertEqual(self.es.written, [])

    def test_an_empty_body_saves_nothing(self):
        code, body, _ = self.call("post", "/api/v1/cred/preferences", self.token, {})
        self.assertEqual(body["error"], "nothing_to_save")
        self.assertEqual(self.es.written, [])

    def test_another_account_cannot_be_targeted(self):
        code, body, _ = self.call("post", "/api/v1/cred/preferences", self.token,
                                  {"theme": "dark", "login": "bob@example.com",
                                   "id": "bob@example.com"})
        self.assertEqual(body["error"], "")
        self.assertNotIn("theme", self.es.docs["nyx_user"]["bob@example.com"])
        self.assertEqual(self.es.docs["nyx_user"]["alice@example.com"]["theme"], "dark")

    def test_the_preference_comes_back_in_the_login_payload(self):
        self.call("post", "/api/v1/cred/preferences", self.token, {"theme": "auto"})
        code, body, _ = self.call("post", "/api/v1/cred/login",
                                  body={"login": "alice@example.com", "password": PASSWORD})
        self.assertEqual(body["cred"]["user"].get("theme"), "auto",
                         "la console lit le theme au login : il doit y figurer")

    def test_the_route_is_closed_without_a_token(self):
        code, body, _ = self.call("post", "/api/v1/cred/preferences", None, {"theme": "dark"})
        self.assertNotEqual(body.get("error"), "")
        self.assertEqual(self.es.written, [])
