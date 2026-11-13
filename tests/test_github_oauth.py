"""
Signing in with GitHub: the exchange, the state, and the account it lands on.

Two layers, for two kinds of mistake. `github_oauth` is tested with a fake session, because what
matters is how GitHub's answers are read (a JSON body, an error payload, an unverified address).
`/cred/login_github` is tested through the real Flask application and the route harness, because
what matters there is the wiring: which account is chosen, what is stored, and what is refused.

The rule the tests keep an eye on: **the GitHub token is used and dropped**. It is not the session
token, and it must not appear in Redis — a provider token in our cache would be a second secret
with a one-hour life that we cannot revoke.
"""

import json
import sys
import unittest
import urllib.parse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sources"))

try:
    import requests  # noqa: F401

    HAVE_DEPS = True
    DEP_MISSING = ""
except Exception as exc:  # pragma: no cover - depends on the machine running the tests
    HAVE_DEPS = False
    DEP_MISSING = type(exc).__name__ + ": " + str(exc)[:80]

from lib import github_oauth  # noqa: E402
from route_harness import RouteTestCase, HAVE_ROUTE_DEPS, DEPS_MISSING  # noqa: E402
from routes import auth as auth_routes  # noqa: E402
from config import settings  # noqa: E402


class FakeResponse:
    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


class FakeSession:
    """Records the calls, answers what the test decided, per URL."""

    def __init__(self, responses=None):
        self.responses = responses or {}
        self.calls = []

    def post(self, url, data=None, headers=None, timeout=None):
        self.calls.append(("post", url, data, headers))
        answer = self.responses.get(url)
        if isinstance(answer, Exception):
            raise answer
        return answer

    def get(self, url, headers=None, timeout=None):
        self.calls.append(("get", url, None, headers))
        answer = self.responses.get(url)
        if isinstance(answer, Exception):
            raise answer
        return answer


class AuthorizeUrlTests(unittest.TestCase):
    def test_the_url_carries_what_github_needs(self):
        url = github_oauth.authorize_url("abc123", "https://host/oauth", "state-1")

        self.assertTrue(url.startswith(github_oauth.GITHUB_AUTHORIZE + "?"))
        params = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
        self.assertEqual(params["client_id"], ["abc123"])
        self.assertEqual(params["redirect_uri"], ["https://host/oauth"])
        self.assertEqual(params["state"], ["state-1"])
        self.assertEqual(params["scope"], [github_oauth.SCOPE])

    def test_the_redirect_uri_is_encoded_as_one_parameter(self):
        url = github_oauth.authorize_url("abc", "https://host/oauth?x=1", "s")

        self.assertIn("redirect_uri=https%3A%2F%2Fhost%2Foauth%3Fx%3D1", url)

    def test_a_missing_piece_is_an_error_not_a_half_url(self):
        for args in (("", "https://host/oauth", "s"), ("id", "", "s"), ("id", "https://host/oauth", "")):
            with self.assertRaises(ValueError):
                github_oauth.authorize_url(*args)


class ExchangeCodeTests(unittest.TestCase):
    def test_the_token_is_read_from_the_json_body(self):
        session = FakeSession({github_oauth.GITHUB_TOKEN: FakeResponse(200, {"access_token": "gho_x"})})

        token = github_oauth.exchange_code(session, "id", "secret", "the-code", "https://host/oauth")

        self.assertEqual(token, "gho_x")
        method, url, data, headers = session.calls[0]
        self.assertEqual(url, github_oauth.GITHUB_TOKEN)
        self.assertEqual(data["client_secret"], "secret")
        self.assertEqual(data["code"], "the-code")
        self.assertEqual(data["redirect_uri"], "https://host/oauth")
        self.assertEqual(headers["Accept"], "application/json")

    def test_github_refusing_the_code_is_not_a_crash(self):
        session = FakeSession({github_oauth.GITHUB_TOKEN:
                               FakeResponse(200, {"error": "bad_verification_code"})})

        with self.assertRaises(github_oauth.GithubError):
            github_oauth.exchange_code(session, "id", "secret", "stale")

    def test_an_unreachable_github_is_not_a_crash(self):
        session = FakeSession({github_oauth.GITHUB_TOKEN: ConnectionError("down")})

        with self.assertRaises(github_oauth.GithubError):
            github_oauth.exchange_code(session, "id", "secret", "code")

    def test_a_body_that_is_not_json_is_not_a_crash(self):
        session = FakeSession({github_oauth.GITHUB_TOKEN: FakeResponse(500, ValueError("html"))})

        with self.assertRaises(github_oauth.GithubError):
            github_oauth.exchange_code(session, "id", "secret", "code")


class PickPrimaryEmailTests(unittest.TestCase):
    def test_the_primary_verified_address_wins(self):
        entries = [
            {"email": "secondary@example.com", "primary": False, "verified": True},
            {"email": "me@example.com", "primary": True, "verified": True},
        ]

        self.assertEqual(github_oauth.pick_primary_email(entries), "me@example.com")

    def test_any_verified_address_beats_an_unverified_one(self):
        entries = [
            {"email": "typed@example.com", "primary": True, "verified": False},
            {"email": "real@example.com", "primary": False, "verified": True},
        ]

        self.assertEqual(github_oauth.pick_primary_email(entries), "real@example.com")

    def test_an_unverified_address_alone_matches_nothing(self):
        # Matching on it would let anybody claim a NYX account by typing its address on GitHub.
        self.assertEqual(github_oauth.pick_primary_email(
            [{"email": "typed@example.com", "primary": True, "verified": False}]), "")

    def test_odd_answers_are_empty(self):
        for payload in (None, "", {}, [None], [{"email": ""}]):
            self.assertEqual(github_oauth.pick_primary_email(payload), "")


class FetchIdentityTests(unittest.TestCase):
    def test_the_login_and_the_primary_address(self):
        session = FakeSession({
            github_oauth.GITHUB_USER: FakeResponse(200, {"login": "snuids"}),
            github_oauth.GITHUB_EMAILS: FakeResponse(200, [{"email": "me@example.com",
                                                            "primary": True, "verified": True}]),
        })

        self.assertEqual(github_oauth.fetch_identity(session, "gho_x"), ("snuids", "me@example.com"))
        self.assertEqual(session.calls[0][3]["Authorization"], "Bearer gho_x")

    def test_a_profile_without_a_login_is_refused(self):
        session = FakeSession({github_oauth.GITHUB_USER: FakeResponse(200, {"name": "no login"})})

        with self.assertRaises(github_oauth.GithubError):
            github_oauth.fetch_identity(session, "gho_x")

    def test_a_refused_profile_is_refused(self):
        session = FakeSession({github_oauth.GITHUB_USER: FakeResponse(401, {"message": "Bad credentials"})})

        with self.assertRaises(github_oauth.GithubError):
            github_oauth.fetch_identity(session, "gho_x")

    def test_unreadable_emails_keep_the_login(self):
        # `user:email` may be missing from an older token: the sign-in still identifies somebody,
        # the account match just has the login to go on.
        session = FakeSession({
            github_oauth.GITHUB_USER: FakeResponse(200, {"login": "snuids"}),
            github_oauth.GITHUB_EMAILS: FakeResponse(403, {"message": "forbidden"}),
        })

        self.assertEqual(github_oauth.fetch_identity(session, "gho_x"), ("snuids", ""))


class CandidateTests(unittest.TestCase):
    def test_the_address_is_tried_before_the_login(self):
        self.assertEqual(github_oauth.candidates("snuids", "me@example.com"),
                         ["me@example.com", "snuids"])

    def test_the_same_value_once(self):
        self.assertEqual(github_oauth.candidates("me@example.com", "me@example.com"), ["me@example.com"])

    def test_nothing_usable(self):
        self.assertEqual(github_oauth.candidates("", None), [])


class SettingsOverride:
    """Points the three GitHub settings at a test value, and puts them back."""

    def configure_github(self, client_id="cid", secret="secret", redirect="https://host/oauth"):
        for name, value in (("GITHUB_CLIENT_ID", client_id),
                            ("GITHUB_CLIENT_SECRET", secret),
                            ("GITHUB_REDIRECT_URI", redirect)):
            saved = getattr(settings, name)
            setattr(settings, name, value)
            self.addCleanup(setattr, settings, name, saved)


@unittest.skipUnless(HAVE_ROUTE_DEPS and HAVE_DEPS, "route dependencies missing (" + DEPS_MISSING + ")")
class GithubLoginRouteTests(SettingsOverride, RouteTestCase):
    """`GET /cred/oauth/state` + `POST /cred/login_github` through the real application."""

    route_modules = (auth_routes,)
    documents = {"nyx_user": {"someone": {
        "login": "someone", "password": "$pbkdf2-sha256$29000$x$y", "privileges": ["admin"],
        "language": "en", "filters": [],
    }}}

    def setUp(self):
        super().setUp()
        self.configure_github()

    # -- helpers ---------------------------------------------------------
    def start_sign_in(self):
        status, payload, response = self.call("get", "/api/v1/cred/oauth/state")
        cookies = response.headers.getlist("Set-Cookie")
        state_value = urllib.parse.parse_qs(urllib.parse.urlparse(payload["url"]).query)["state"][0]
        return payload, state_value, cookies

    def sign_in(self, state_value, cookie=True, code="the-code"):
        if not cookie:
            # The test client keeps a cookie jar, so the cookie set by `start_sign_in` would be
            # sent anyway: "another browser" means dropping it from the jar first.
            self.client.delete_cookie("nyx_oauth_state")
        headers = {"Cookie": "nyx_oauth_state=" + state_value} if cookie else {}
        return self.call("post", "/api/v1/cred/login_github",
                         body={"code": code, "state": state_value}, headers=headers)

    def capture_finalize(self):
        captured = {}
        original = auth_routes.finalize_login

        def fake(usr, data, es, conn):
            captured["usr"] = usr
            captured["data"] = data
            return {"error": "", "cred": {"token": "session-token", "user": usr["_source"]}}

        auth_routes.finalize_login = fake
        self.addCleanup(setattr, auth_routes, "finalize_login", original)
        return captured

    def fake_github(self, login="someone", email="someone@example.com"):
        auth_routes.github_oauth.exchange_code = lambda *a, **k: "gho_secret_token"
        auth_routes.github_oauth.fetch_identity = lambda *a, **k: (login, email)
        self.addCleanup(setattr, auth_routes.github_oauth, "exchange_code",
                        github_oauth.exchange_code)
        self.addCleanup(setattr, auth_routes.github_oauth, "fetch_identity",
                        github_oauth.fetch_identity)

    # -- the state -------------------------------------------------------
    def test_the_state_endpoint_is_off_until_configured(self):
        settings.GITHUB_CLIENT_ID = ""
        status, payload, response = self.call("get", "/api/v1/cred/oauth/state")

        self.assertEqual(status, 200)
        self.assertEqual(payload["error"], "github login not configured")
        self.assertEqual(response.headers.getlist("Set-Cookie"), [])

    def test_the_state_endpoint_returns_the_url_and_binds_it_to_the_browser(self):
        payload, state_value, cookies = self.start_sign_in()

        self.assertEqual(payload["error"], "")
        params = urllib.parse.parse_qs(urllib.parse.urlparse(payload["url"]).query)
        self.assertEqual(params["client_id"], ["cid"])
        self.assertEqual(params["redirect_uri"], ["https://host/oauth"])
        self.assertEqual(params["state"], [state_value])

        self.assertEqual(len(cookies), 1)
        self.assertIn("nyx_oauth_state=" + state_value, cookies[0])
        self.assertIn("HttpOnly", cookies[0])
        self.assertIn("SameSite=Lax", cookies[0])
        self.assertIsNotNone(self.redis.store.get("nyx_oauth_state_" + state_value))
        self.assertEqual(self.redis.ttls.get("nyx_oauth_state_" + state_value), 600)

    # -- the callback ----------------------------------------------------
    def test_a_login_without_state_is_refused(self):
        status, payload, _ = self.call("post", "/api/v1/cred/login_github", body={"code": "x"})

        self.assertEqual(payload["error"], "Bad Credentials")

    def test_an_unknown_state_is_refused(self):
        captured = self.capture_finalize()

        _, payload, _ = self.sign_in("never-issued", cookie=False)

        self.assertEqual(payload["error"], "Bad Credentials")
        self.assertNotIn("usr", captured)
        self.assertEqual(self.state.auth_failures_total, 1)

    def test_a_state_issued_to_another_browser_is_refused(self):
        # The state exists in Redis, but this browser never received its cookie: that is the
        # login-CSRF shape (an attacker's code and a state fetched elsewhere).
        captured = self.capture_finalize()
        _, state_value, _ = self.start_sign_in()

        _, payload, _ = self.sign_in(state_value, cookie=False)

        self.assertEqual(payload["error"], "Bad Credentials")
        self.assertNotIn("usr", captured, "no session may be created")
        self.assertIsNone(self.redis.store.get("nyx_oauth_state_" + state_value),
                          "the state is consumed even when refused")

    def test_an_existing_account_signs_in(self):
        captured = self.capture_finalize()
        self.fake_github(login="someone", email="someone@example.com")
        _, state_value, _ = self.start_sign_in()

        _, payload, _ = self.sign_in(state_value)

        self.assertEqual(payload["error"], "")
        self.assertEqual(captured["usr"]["_source"]["login"], "someone")
        self.assertEqual(captured["data"]["login"], "someone")
        self.assertEqual(captured["data"]["app"], "console")
        for key, value in self.redis.store.items():
            self.assertNotIn("gho_secret_token", str(value),
                             "the GitHub token must not be stored")
            self.assertNotIn("gho_secret_token", str(key))

    def test_the_account_is_found_by_its_address_when_the_id_is_the_address(self):
        # Eight production accounts are indexed under their e-mail while `login` holds a short
        # name; the identity's address must still find them.
        self.es.docs["nyx_user"] = {"amarchand@icloud.com": {
            "login": "amarchand", "privileges": ["user"], "language": "fr", "filters": [],
        }}
        captured = self.capture_finalize()
        self.fake_github(login="snuids", email="amarchand@icloud.com")
        _, state_value, _ = self.start_sign_in()

        _, payload, _ = self.sign_in(state_value)

        self.assertEqual(payload["error"], "")
        self.assertEqual(captured["usr"]["_source"]["login"], "amarchand")

    def test_the_login_finds_the_account_when_the_address_is_unknown(self):
        # The GitHub profile hides its e-mail (or the scope is missing): the login is the only
        # identifier left, and `login.keyword` is what the password route uses too.
        captured = self.capture_finalize()
        self.fake_github(login="someone", email="")
        _, state_value, _ = self.start_sign_in()

        _, payload, _ = self.sign_in(state_value)

        self.assertEqual(payload["error"], "")
        self.assertEqual(captured["usr"]["_source"]["login"], "someone")

    def test_an_identity_without_an_account_is_refused_and_audited(self):
        self.es.docs["nyx_user"] = {}
        captured = self.capture_finalize()
        self.fake_github(login="stranger", email="stranger@example.com")
        _, state_value, _ = self.start_sign_in()

        _, payload, _ = self.sign_in(state_value)

        self.assertEqual(payload["error"], "Bad Credentials")
        self.assertNotIn("usr", captured, "only an existing account signs in")
        self.assertEqual(self.state.auth_failures_total, 1)
        # `record_auth_failure` tries `document=` (elasticsearch-py 8) before `body=`: the fake
        # client accepts both, so only the payloads that are documents count.
        audits = [entry for index, _id, entry in self.es.written
                  if index.startswith("nyx_authfail-") and isinstance(entry, dict)]
        self.assertEqual(len(audits), 1)
        self.assertEqual(audits[0]["reason"], "NO_ACCOUNT_GITHUB")

    # -- an identity nobody knows, and the setting that files it -------------
    def set_policy(self, value):
        saved = getattr(settings, "GITHUB_UNKNOWN_ACCOUNTS", "refuse")
        settings.GITHUB_UNKNOWN_ACCOUNTS = value
        self.addCleanup(setattr, settings, "GITHUB_UNKNOWN_ACCOUNTS", saved)

    def test_by_default_an_unknown_identity_signs_nobody_in(self):
        # the historical behaviour, kept: the setting is opt-in
        self.set_policy("refuse")
        self.es.docs["nyx_user"] = {}
        self.fake_github(login="stranger", email="stranger@example.com")
        _, state_value, _ = self.start_sign_in()

        _, payload, _ = self.sign_in(state_value)

        self.assertEqual(payload["error"], "Bad Credentials")
        self.assertEqual(self.es.docs["nyx_user"], {})

    def test_the_inactive_setting_files_a_disabled_account_without_privileges(self):
        self.set_policy("inactive")
        self.es.docs["nyx_user"] = {}
        self.fake_github(login="stranger", email="stranger@example.com")
        _, state_value, _ = self.start_sign_in()

        _, payload, _ = self.sign_in(state_value)

        # The account exists, and it cannot come in: the console shows the page that explains it.
        self.assertEqual(payload["error"], "ACCOUNT_INACTIVE")
        self.assertNotIn("cred", payload)

        document = self.es.docs["nyx_user"]["stranger@example.com"]
        self.assertIs(document["inactive"], True)
        self.assertEqual(document["privileges"], [])
        self.assertEqual(document["login"], "stranger")
        self.assertEqual(document["email"], "stranger@example.com")
        self.assertEqual(document["source"], "github")
        self.assertNotIn("password", document)

    def test_the_setting_is_read_without_its_case_or_spaces(self):
        self.set_policy("  INACTIVE  ")
        self.es.docs["nyx_user"] = {}
        self.fake_github(login="stranger", email="stranger@example.com")
        _, state_value, _ = self.start_sign_in()

        _, payload, _ = self.sign_in(state_value)

        self.assertEqual(payload["error"], "ACCOUNT_INACTIVE")
        self.assertIn("stranger@example.com", self.es.docs["nyx_user"])

    def test_a_misspelt_setting_refuses_rather_than_opening_the_door(self):
        self.set_policy("inactive!")
        self.es.docs["nyx_user"] = {}
        self.fake_github(login="stranger", email="stranger@example.com")
        _, state_value, _ = self.start_sign_in()

        _, payload, _ = self.sign_in(state_value)

        self.assertEqual(payload["error"], "Bad Credentials")
        self.assertEqual(self.es.docs["nyx_user"], {})

    def test_the_filed_account_signs_in_once_an_administrator_activates_it(self):
        self.set_policy("inactive")
        self.es.docs["nyx_user"] = {}
        self.fake_github(login="stranger", email="stranger@example.com")
        _, state_value, _ = self.start_sign_in()
        self.sign_in(state_value)
        self.assertIn("stranger@example.com", self.es.docs["nyx_user"])

        # the administrator ticks the switch off and grants a privilege
        document = self.es.docs["nyx_user"]["stranger@example.com"]
        document.pop("inactive")
        document["privileges"] = ["user"]

        _, state_value, _ = self.start_sign_in()
        _, payload, _ = self.sign_in(state_value)

        self.assertEqual(payload["error"], "")
        self.assertTrue(payload["cred"]["token"])
        self.assertEqual(payload["cred"]["user"]["privileges"], ["user"])

    def test_an_identity_that_already_has_an_account_is_untouched_by_the_setting(self):
        self.set_policy("inactive")
        self.fake_github(login="someone", email="someone@example.com")
        _, state_value, _ = self.start_sign_in()

        _, payload, _ = self.sign_in(state_value)

        self.assertEqual(payload["error"], "")
        self.assertNotIn("inactive", self.es.docs["nyx_user"]["someone"])

    def test_a_refused_exchange_is_a_login_failure(self):
        captured = self.capture_finalize()

        def refuse(*args, **kwargs):
            raise github_oauth.GithubError("bad_verification_code")

        auth_routes.github_oauth.exchange_code = refuse
        self.addCleanup(setattr, auth_routes.github_oauth, "exchange_code",
                        github_oauth.exchange_code)
        _, state_value, _ = self.start_sign_in()

        _, payload, _ = self.sign_in(state_value)

        self.assertEqual(payload["error"], "Bad Credentials")
        self.assertNotIn("usr", captured)
        self.assertEqual(self.state.auth_failures_total, 1)

    def test_a_state_is_single_use(self):
        self.fake_github()
        _, state_value, _ = self.start_sign_in()
        self.sign_in(state_value)

        self.capture_finalize()
        _, payload, _ = self.sign_in(state_value)

        self.assertEqual(payload["error"], "Bad Credentials")


if __name__ == "__main__":
    unittest.main()
