"""
Signing in with Microsoft Entra ID: the exchange, the claims, and the account it lands on.

Two layers, for two kinds of mistake. `entra_oauth` is tested with a fake session and hand-built
identity tokens, because what matters is how Entra's answers are read — a refusal, an ID token from
another directory, one issued for another application, one that answers another sign-in. The routes
are tested through the real Flask application and the route harness, because what matters there is
the wiring: which account is chosen, what is stored, and what is refused.

Two rules the tests keep an eye on:

* **the tokens are used and dropped** — neither the access token nor the ID token is the session
  token, and neither may appear in Redis: a provider token in our cache would be a second secret
  with a one-hour life that we cannot revoke;
* **the directory is the boundary** — an identity token whose `tid` is not the configured tenant
  signs nobody in, whatever its address claims.
"""

import base64
import json
import sys
import time
import unittest
import urllib.parse
import uuid as uuidlib
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sources"))

try:
    import requests  # noqa: F401

    HAVE_DEPS = True
    DEP_MISSING = ""
except Exception as exc:  # pragma: no cover - depends on the machine running the tests
    HAVE_DEPS = False
    DEP_MISSING = type(exc).__name__ + ": " + str(exc)[:80]

from lib import entra_oauth  # noqa: E402
from route_harness import RouteTestCase, HAVE_ROUTE_DEPS, DEPS_MISSING  # noqa: E402
from routes import auth as auth_routes  # noqa: E402
from config import settings  # noqa: E402

TENANT = "11111111-2222-3333-4444-555555555555"
OTHER_TENANT = "99999999-8888-7777-6666-555555555555"


def id_token(claims):
    """An unsigned JWT carrying `claims` — what the token endpoint answers inside its JSON body."""
    def segment(payload):
        raw = json.dumps(payload).encode("utf-8")
        return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")

    return segment({"alg": "none", "typ": "JWT"}) + "." + segment(claims) + ".signature"


def claims_for(**overrides):
    """The claims of a well-formed identity token for the test directory."""
    claims = {
        "aud": "the-client-id",
        "tid": TENANT,
        "iss": "https://login.microsoftonline.com/" + TENANT + "/v2.0",
        "exp": int(time.time()) + 3600,
        "nbf": int(time.time()) - 60,
        "oid": "00000000-1111-2222-3333-444444444444",
        "preferred_username": "arnaud.marchand@contoso.com",
        "name": "Arnaud Marchand",
        "given_name": "Arnaud",
        "family_name": "Marchand",
        "nonce": "the-nonce",
    }
    claims.update(overrides)
    return claims


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
        answer = None
        for known, decided in self.responses.items():
            if url.startswith(known):
                answer = decided
                break
        if isinstance(answer, Exception):
            raise answer
        return answer

    def get(self, url, headers=None, timeout=None):
        self.calls.append(("get", url, None, headers))
        answer = self.responses.get(url)
        if isinstance(answer, Exception):
            raise answer
        return answer


def token_url(tenant=TENANT):
    return entra_oauth.TOKEN.format(tenant=tenant)


class ConfigurationTests(unittest.TestCase):
    def test_a_directory_id_is_the_tenant(self):
        self.assertTrue(entra_oauth.is_tenant_id(TENANT))
        self.assertTrue(entra_oauth.is_tenant_id("  " + TENANT.upper() + " "))

    def test_a_domain_name_is_not(self):
        for value in ("contoso.com", "contoso.onmicrosoft.com", "common", "", None, "1234"):
            with self.subTest(value=value):
                self.assertFalse(entra_oauth.is_tenant_id(value))

    def test_the_four_settings_make_the_sign_in(self):
        self.assertTrue(entra_oauth.configured(TENANT, "cid", "secret", "https://host/oauth"))

    def test_a_domain_name_leaves_the_sign_in_unconfigured(self):
        # Showing the button and then refusing the token would look like a wrong password.
        self.assertFalse(entra_oauth.configured("contoso.com", "cid", "secret", "https://host/oauth"))

    def test_any_missing_setting_leaves_it_unconfigured(self):
        for args in (("", "cid", "secret", "https://host/oauth"),
                     (TENANT, "", "secret", "https://host/oauth"),
                     (TENANT, "cid", "", "https://host/oauth"),
                     (TENANT, "cid", "secret", "")):
            with self.subTest(args=args):
                self.assertFalse(entra_oauth.configured(*args))


class AuthorizeUrlTests(unittest.TestCase):
    def test_the_url_goes_to_the_directory_and_carries_what_entra_needs(self):
        url = entra_oauth.authorize_url(TENANT, "cid", "https://host/oauth", "state-1", "nonce-1")

        self.assertTrue(url.startswith("https://login.microsoftonline.com/" + TENANT
                                       + "/oauth2/v2.0/authorize?"))
        params = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
        self.assertEqual(params["client_id"], ["cid"])
        self.assertEqual(params["response_type"], ["code"])
        self.assertEqual(params["redirect_uri"], ["https://host/oauth"])
        self.assertEqual(params["scope"], [entra_oauth.SCOPE])
        self.assertEqual(params["state"], ["state-1"])
        self.assertEqual(params["nonce"], ["nonce-1"])
        self.assertEqual(params["prompt"], ["select_account"],
                         "silently reusing the browser's Microsoft session signs in the wrong person")

    def test_the_redirect_uri_is_encoded_as_one_parameter(self):
        url = entra_oauth.authorize_url(TENANT, "cid", "https://host/oauth?x=1", "s", "n")

        self.assertIn("redirect_uri=https%3A%2F%2Fhost%2Foauth%3Fx%3D1", url)

    def test_a_missing_piece_is_an_error_not_a_half_url(self):
        for args in ((TENANT, "", "https://host/oauth", "s", "n"),
                     (TENANT, "cid", "", "s", "n"),
                     (TENANT, "cid", "https://host/oauth", "", "n"),
                     (TENANT, "cid", "https://host/oauth", "s", "")):
            with self.subTest(args=args):
                with self.assertRaises(ValueError):
                    entra_oauth.authorize_url(*args)

    def test_a_domain_name_never_builds_a_url(self):
        with self.assertRaises(ValueError):
            entra_oauth.authorize_url("contoso.com", "cid", "https://host/oauth", "s", "n")


class ReadClaimsTests(unittest.TestCase):
    def test_the_claims_of_a_well_formed_token(self):
        claims = entra_oauth.read_claims(id_token(claims_for()), client_id="the-client-id",
                                         tenant=TENANT, nonce="the-nonce")

        self.assertEqual(claims["preferred_username"], "arnaud.marchand@contoso.com")

    def test_a_token_for_another_application_is_refused(self):
        with self.assertRaises(entra_oauth.EntraError):
            entra_oauth.read_claims(id_token(claims_for(aud="another-app")),
                                    client_id="the-client-id", tenant=TENANT)

    def test_a_token_from_another_directory_is_refused(self):
        # Without this check, any Entra tenant could present a user holding one of our addresses.
        with self.assertRaises(entra_oauth.EntraError):
            entra_oauth.read_claims(id_token(claims_for(tid=OTHER_TENANT,
                                                        iss="https://login.microsoftonline.com/"
                                                            + OTHER_TENANT + "/v2.0")),
                                    client_id="the-client-id", tenant=TENANT)

    def test_an_issuer_that_disagrees_with_the_directory_is_refused(self):
        with self.assertRaises(entra_oauth.EntraError):
            entra_oauth.read_claims(id_token(claims_for(iss="https://login.microsoftonline.com/"
                                                            + OTHER_TENANT + "/v2.0")),
                                    client_id="the-client-id", tenant=TENANT)

    def test_an_expired_token_is_refused(self):
        with self.assertRaises(entra_oauth.EntraError):
            entra_oauth.read_claims(id_token(claims_for(exp=int(time.time()) - 3600)),
                                    client_id="the-client-id", tenant=TENANT)

    def test_a_token_not_valid_yet_is_refused(self):
        with self.assertRaises(entra_oauth.EntraError):
            entra_oauth.read_claims(id_token(claims_for(nbf=int(time.time()) + 3600)),
                                    client_id="the-client-id", tenant=TENANT)

    def test_a_token_that_answers_another_sign_in_is_refused(self):
        with self.assertRaises(entra_oauth.EntraError):
            entra_oauth.read_claims(id_token(claims_for(nonce="another-nonce")),
                                    client_id="the-client-id", tenant=TENANT, nonce="the-nonce")

    def test_a_token_without_a_nonce_is_refused_when_one_was_asked(self):
        claims = claims_for()
        claims.pop("nonce")
        with self.assertRaises(entra_oauth.EntraError):
            entra_oauth.read_claims(id_token(claims), client_id="the-client-id",
                                    tenant=TENANT, nonce="the-nonce")

    def test_something_that_is_not_a_jwt_is_refused(self):
        for value in ("", "not-a-token", "one.segment"):
            with self.subTest(value=value):
                with self.assertRaises(entra_oauth.EntraError):
                    entra_oauth.read_claims(value)

    def test_a_payload_that_is_not_json_is_refused(self):
        broken = base64.urlsafe_b64encode(b"not json").decode("ascii").rstrip("=")

        with self.assertRaises(entra_oauth.EntraError):
            entra_oauth.read_claims("header." + broken + ".signature")


class ProfileTests(unittest.TestCase):
    def test_the_address_and_the_short_name(self):
        profile = entra_oauth.profile_from_claims(claims_for())

        self.assertEqual(profile["email"], "arnaud.marchand@contoso.com")
        self.assertEqual(profile["login"], "arnaud.marchand")
        self.assertEqual(profile["upn"], "arnaud.marchand@contoso.com")
        self.assertEqual(profile["source"], "entra")

    def test_the_email_claim_wins_over_the_sign_in_name(self):
        profile = entra_oauth.profile_from_claims(claims_for(email="amarchand@icloud.com"))

        self.assertEqual(profile["email"], "amarchand@icloud.com")
        self.assertEqual(profile["login"], "arnaud.marchand",
                         "the short name still comes from the directory's sign-in name")

    def test_the_sign_in_name_stands_in_when_there_is_no_email_claim(self):
        profile = entra_oauth.profile_from_claims(claims_for(preferred_username="am@contoso.com"))

        self.assertEqual(profile["email"], "am@contoso.com")

    def test_an_identity_without_an_address_is_refused(self):
        # Matching on a derived short name would let it land on an account it never proved.
        with self.assertRaises(entra_oauth.EntraError):
            entra_oauth.profile_from_claims(claims_for(preferred_username="arnaud"))

    def test_the_name_fields_are_read(self):
        profile = entra_oauth.profile_from_claims(claims_for())

        self.assertEqual((profile["firstname"], profile["lastname"]), ("Arnaud", "Marchand"))
        self.assertEqual(profile["name"], "Arnaud Marchand")

    def test_a_token_with_only_a_display_name_is_split(self):
        claims = claims_for(name="Jean de La Fontaine")
        claims.pop("given_name")
        claims.pop("family_name")

        profile = entra_oauth.profile_from_claims(claims)

        self.assertEqual((profile["firstname"], profile["lastname"]), ("Jean", "de La Fontaine"))

    def test_the_profile_has_no_phone_because_entra_gives_none(self):
        self.assertNotIn("phone", entra_oauth.profile_from_claims(claims_for()))

    def test_the_directory_ids_travel_with_the_profile(self):
        profile = entra_oauth.profile_from_claims(claims_for())

        self.assertEqual(profile["tid"], TENANT)
        self.assertEqual(profile["oid"], "00000000-1111-2222-3333-444444444444")


class CandidateTests(unittest.TestCase):
    def test_the_addresses_of_the_identity_and_nothing_else(self):
        profile = {"email": "a@contoso.com", "upn": "a@contoso.com", "login": "a"}

        self.assertEqual(entra_oauth.candidates(profile), ["a@contoso.com"],
                         "a short name must never be tried: the account may not hold it")

    def test_the_sign_in_name_is_tried_after_the_address(self):
        profile = {"email": "a@contoso.com", "upn": "a@contoso.onmicrosoft.com"}

        self.assertEqual(entra_oauth.candidates(profile),
                         ["a@contoso.com", "a@contoso.onmicrosoft.com"])

    def test_the_same_address_once_whatever_its_case(self):
        profile = {"email": "A@contoso.com", "upn": "a@contoso.com"}

        self.assertEqual(entra_oauth.candidates(profile), ["A@contoso.com"])

    def test_nothing_usable(self):
        self.assertEqual(entra_oauth.candidates({}), [])


class ExchangeCodeTests(unittest.TestCase):
    def test_the_identity_is_read_from_the_body_and_the_tokens_stay_inside(self):
        session = FakeSession({token_url(): FakeResponse(200, {
            "access_token": "at-secret", "id_token": id_token(claims_for())})})

        profile = entra_oauth.exchange_code(session, TENANT, "the-client-id", "secret",
                                            "the-code", "https://host/oauth", nonce="the-nonce")

        self.assertEqual(profile["email"], "arnaud.marchand@contoso.com")
        self.assertNotIn("at-secret", json.dumps(profile),
                         "the provider token must not reach the caller")
        method, url, data, _headers = session.calls[0]
        self.assertEqual(url, token_url())
        self.assertEqual(data["grant_type"], "authorization_code")
        self.assertEqual(data["client_id"], "the-client-id")
        self.assertEqual(data["client_secret"], "secret")
        self.assertEqual(data["code"], "the-code")
        self.assertEqual(data["redirect_uri"], "https://host/oauth")

    def test_entra_refusing_the_code_is_not_a_crash(self):
        session = FakeSession({token_url(): FakeResponse(400, {
            "error": "invalid_grant", "error_description": "AADSTS70008: expired"})})

        with self.assertRaises(entra_oauth.EntraError):
            entra_oauth.exchange_code(session, TENANT, "cid", "secret", "stale")

    def test_a_body_without_an_identity_token_is_refused(self):
        session = FakeSession({token_url(): FakeResponse(200, {"access_token": "at"})})

        with self.assertRaises(entra_oauth.EntraError):
            entra_oauth.exchange_code(session, TENANT, "cid", "secret", "code")

    def test_an_unreachable_entra_is_not_a_crash(self):
        session = FakeSession({token_url(): ConnectionError("down")})

        with self.assertRaises(entra_oauth.EntraError):
            entra_oauth.exchange_code(session, TENANT, "cid", "secret", "code")

    def test_a_body_that_is_not_json_is_not_a_crash(self):
        session = FakeSession({token_url(): FakeResponse(500, ValueError("html"))})

        with self.assertRaises(entra_oauth.EntraError):
            entra_oauth.exchange_code(session, TENANT, "cid", "secret", "code")

    def test_a_refused_token_never_becomes_a_profile(self):
        session = FakeSession({token_url(): FakeResponse(200, {
            "id_token": id_token(claims_for(aud="another-app"))})})

        with self.assertRaises(entra_oauth.EntraError):
            entra_oauth.exchange_code(session, TENANT, "the-client-id", "secret", "code",
                                      nonce="the-nonce")


class SettingsOverride:
    """Points the Entra and GitHub settings at test values, and puts them back."""

    def configure_entra(self, tenant=TENANT, client_id="the-client-id", secret="the-secret",
                        redirect="https://host/oauth"):
        for name, value in (("ENTRA_TENANT_ID", tenant),
                            ("ENTRA_CLIENT_ID", client_id),
                            ("ENTRA_CLIENT_SECRET", secret),
                            ("ENTRA_REDIRECT_URI", redirect)):
            saved = getattr(settings, name)
            setattr(settings, name, value)
            self.addCleanup(setattr, settings, name, saved)

    def configure_github(self, client_id="gh-client-id", secret="gh-secret",
                         redirect="https://host/oauth"):
        for name, value in (("GITHUB_CLIENT_ID", client_id),
                            ("GITHUB_CLIENT_SECRET", secret),
                            ("GITHUB_REDIRECT_URI", redirect)):
            saved = getattr(settings, name)
            setattr(settings, name, value)
            self.addCleanup(setattr, settings, name, saved)


@unittest.skipUnless(HAVE_ROUTE_DEPS and HAVE_DEPS,
                     "route dependencies missing (" + DEPS_MISSING + ")")
class EntraLoginRouteTests(SettingsOverride, RouteTestCase):
    """`GET /cred/oauth/state?provider=entra` + `POST /cred/login_sso` + `/cred/login_entra`."""

    route_modules = (auth_routes,)
    documents = {"nyx_user": {
        "someone": {
            "login": "someone", "password": "$pbkdf2-sha256$29000$x$y", "privileges": ["admin"],
            "language": "en", "filters": [],
        },
        # The production shape: the document id is the address, `login` the short name.
        "arnaud.marchand@contoso.com": {
            "login": "amarchand", "privileges": ["user"], "language": "fr", "filters": [],
        },
    }}

    def setUp(self):
        super().setUp()
        self.configure_entra()
        self.configure_github()

    # -- helpers ---------------------------------------------------------
    def start_sign_in(self, provider="entra"):
        query = "/api/v1/cred/oauth/state" + ("?provider=" + provider if provider else "")
        status, payload, response = self.call("get", query)
        state_value = urllib.parse.parse_qs(
            urllib.parse.urlparse(payload.get("url") or "").query).get("state", [""])[0]
        return payload, state_value, response.headers.getlist("Set-Cookie")

    def sign_in(self, state_value, path="/api/v1/cred/login_sso", cookie=True, code="the-code"):
        if not cookie:
            # The test client keeps a cookie jar, so the cookie set by `start_sign_in` would be
            # sent anyway: "another browser" means dropping it from the jar first.
            self.client.delete_cookie("nyx_oauth_state")
        headers = {"Cookie": "nyx_oauth_state=" + state_value} if cookie else {}
        return self.call("post", path, body={"code": code, "state": state_value}, headers=headers)

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

    def fake_entra(self, profile=None):
        """Replaces the exchange: what is under test here is the wiring, not the module."""
        profile = profile or entra_oauth.profile_from_claims(claims_for())
        original = entra_oauth.exchange_code
        auth_routes.entra_oauth.exchange_code = lambda *a, **k: dict(profile)
        self.addCleanup(setattr, entra_oauth, "exchange_code", original)

    def set_policy(self, value):
        saved = getattr(settings, "ENTRA_UNKNOWN_ACCOUNTS", "refuse")
        settings.ENTRA_UNKNOWN_ACCOUNTS = value
        self.addCleanup(setattr, settings, "ENTRA_UNKNOWN_ACCOUNTS", saved)

    def audits(self, reason=None):
        entries = [entry for index, _id, entry in self.es.written
                   if index.startswith("nyx_authfail-") and isinstance(entry, dict)]
        if reason is not None:
            entries = [entry for entry in entries if entry.get("reason") == reason]
        return entries

    # -- the state -------------------------------------------------------
    def test_the_state_endpoint_is_off_until_configured(self):
        settings.ENTRA_TENANT_ID = "contoso.com"

        status, payload, response = self.call("get", "/api/v1/cred/oauth/state?provider=entra")

        self.assertEqual(status, 200)
        self.assertEqual(payload["error"], "entra login not configured")
        self.assertEqual(response.headers.getlist("Set-Cookie"), [])

    def test_github_stays_the_default_provider(self):
        payload, _state, cookies = self.start_sign_in(provider="")

        self.assertEqual(payload["error"], "")
        self.assertIn("github.com", payload["url"])
        self.assertEqual(payload["provider"], "github")
        self.assertEqual(len(cookies), 1)

    def test_an_unknown_provider_is_refused(self):
        status, payload, response = self.call("get", "/api/v1/cred/oauth/state?provider=okta")

        self.assertEqual(payload["error"], "unknown sign-in provider")
        self.assertEqual(response.headers.getlist("Set-Cookie"), [])

    def test_the_state_endpoint_returns_the_url_and_binds_it_to_the_browser(self):
        payload, state_value, cookies = self.start_sign_in()

        self.assertEqual(payload["error"], "")
        self.assertEqual(payload["provider"], "entra")
        params = urllib.parse.parse_qs(urllib.parse.urlparse(payload["url"]).query)
        self.assertEqual(params["client_id"], ["the-client-id"])
        self.assertEqual(params["redirect_uri"], ["https://host/oauth"])
        self.assertEqual(params["state"], [state_value])
        self.assertTrue(params["nonce"][0], "the nonce is what a replayed token cannot answer")

        self.assertEqual(len(cookies), 1)
        self.assertIn("nyx_oauth_state=" + state_value, cookies[0])
        self.assertIn("HttpOnly", cookies[0])
        self.assertIn("SameSite=Lax", cookies[0])
        self.assertIsNotNone(self.redis.store.get("nyx_oauth_state_" + state_value))
        self.assertEqual(self.redis.ttls.get("nyx_oauth_state_" + state_value), 600)

    def test_the_state_record_carries_the_provider_and_the_nonce(self):
        _payload, state_value, _cookies = self.start_sign_in()

        record = json.loads(self.redis.store["nyx_oauth_state_" + state_value])

        self.assertEqual(record["provider"], "entra")
        self.assertTrue(record["nonce"])

    # -- the callback ----------------------------------------------------
    def test_a_login_without_state_is_refused(self):
        status, payload, _ = self.call("post", "/api/v1/cred/login_sso", body={"code": "x"})

        self.assertEqual(payload["error"], "Bad Credentials")

    def test_an_unknown_state_is_refused(self):
        captured = self.capture_finalize()
        self.fake_entra()

        _status, payload, _ = self.sign_in("never-issued", cookie=False)

        self.assertEqual(payload["error"], "Bad Credentials")
        self.assertNotIn("usr", captured)
        self.assertEqual(self.state.auth_failures_total, 1)

    def test_a_state_issued_to_another_browser_is_refused(self):
        captured = self.capture_finalize()
        self.fake_entra()
        _payload, state_value, _cookies = self.start_sign_in()

        _status, payload, _ = self.sign_in(state_value, cookie=False)

        self.assertEqual(payload["error"], "Bad Credentials")
        self.assertNotIn("usr", captured, "no session may be created")
        self.assertIsNone(self.redis.store.get("nyx_oauth_state_" + state_value),
                          "the state is consumed even when refused")

    def test_a_state_posted_to_the_other_callback_is_refused(self):
        # `/cred/login_entra` given a state issued for GitHub: the browser is not doing what the
        # route says, and the exchange would fail with a message nobody could act on.
        captured = self.capture_finalize()
        _payload, state_value, _cookies = self.start_sign_in(provider="github")

        _status, payload, _ = self.sign_in(state_value, path="/api/v1/cred/login_entra")

        self.assertEqual(payload["error"], "Bad Credentials")
        self.assertNotIn("usr", captured)
        self.assertEqual(len(self.audits("OAUTH_PROVIDER")), 1)

    def test_an_existing_account_signs_in_through_the_common_callback(self):
        captured = self.capture_finalize()
        self.fake_entra()
        _payload, state_value, _cookies = self.start_sign_in()

        _status, payload, _ = self.sign_in(state_value)

        self.assertEqual(payload["error"], "")
        self.assertEqual(captured["data"]["login"], "amarchand")
        self.assertEqual(captured["data"]["app"], "console")
        for key, value in self.redis.store.items():
            self.assertNotIn("at-secret", str(value), "the provider token must not be stored")
            self.assertNotIn("at-secret", str(key))

    def test_the_account_is_found_by_its_address(self):
        captured = self.capture_finalize()
        self.fake_entra()
        _payload, state_value, _cookies = self.start_sign_in()

        _status, payload, _ = self.sign_in(state_value, path="/api/v1/cred/login_entra")

        self.assertEqual(payload["error"], "")
        self.assertEqual(captured["usr"]["_source"]["login"], "amarchand")

    def test_the_sign_in_name_finds_an_account_indexed_under_the_other_address(self):
        # The `email` claim and the sign-in name can differ (a primary SMTP address, a UPN from
        # another domain); both are directory-vouched, so either may hold the account.
        self.es.docs["nyx_user"] = {"arnaud@contoso.onmicrosoft.com": {
            "login": "arnaud", "privileges": ["user"], "language": "fr", "filters": []}}
        profile = entra_oauth.profile_from_claims(claims_for(
            email="amarchand@icloud.com", preferred_username="arnaud@contoso.onmicrosoft.com"))
        captured = self.capture_finalize()
        self.fake_entra(profile)
        _payload, state_value, _cookies = self.start_sign_in()

        _status, payload, _ = self.sign_in(state_value)

        self.assertEqual(payload["error"], "")
        self.assertEqual(captured["usr"]["_source"]["login"], "arnaud")

    def test_an_identity_without_an_account_is_refused_and_audited(self):
        self.es.docs["nyx_user"] = {}
        captured = self.capture_finalize()
        self.fake_entra()
        _payload, state_value, _cookies = self.start_sign_in()

        _status, payload, _ = self.sign_in(state_value)

        self.assertEqual(payload["error"], "Bad Credentials")
        self.assertNotIn("usr", captured, "only an existing account signs in")
        self.assertEqual(self.state.auth_failures_total, 1)
        self.assertEqual(len(self.audits("NO_ACCOUNT_ENTRA")), 1)

    def test_the_inactive_setting_files_a_disabled_account_without_privileges(self):
        self.set_policy("inactive")
        self.es.docs["nyx_user"] = {}
        self.fake_entra()
        _payload, state_value, _cookies = self.start_sign_in()

        _status, payload, _ = self.sign_in(state_value)

        self.assertEqual(payload["error"], "ACCOUNT_INACTIVE")
        self.assertNotIn("cred", payload)

        document = self.es.docs["nyx_user"]["arnaud.marchand@contoso.com"]
        self.assertIs(document["inactive"], True)
        self.assertEqual(document["privileges"], [])
        self.assertEqual(document["login"], "arnaud.marchand")
        self.assertEqual(document["source"], "entra")
        self.assertEqual(document["firstname"], "Arnaud")
        self.assertEqual(document["lastname"], "Marchand")
        self.assertNotIn("password", document)
        self.assertNotIn("phone", document)

    def test_the_filed_account_signs_in_once_an_administrator_activates_it(self):
        self.set_policy("inactive")
        self.es.docs["nyx_user"] = {}
        self.fake_entra()
        _payload, state_value, _cookies = self.start_sign_in()
        self.sign_in(state_value)
        self.assertIn("arnaud.marchand@contoso.com", self.es.docs["nyx_user"])

        document = self.es.docs["nyx_user"]["arnaud.marchand@contoso.com"]
        document.pop("inactive")
        document["privileges"] = ["user"]

        _payload, state_value, _cookies = self.start_sign_in()
        _status, payload, _ = self.sign_in(state_value)

        self.assertEqual(payload["error"], "")
        self.assertTrue(payload["cred"]["token"])
        self.assertEqual(payload["cred"]["user"]["privileges"], ["user"])

    def test_a_refused_exchange_is_a_login_failure(self):
        captured = self.capture_finalize()
        # The original is read *before* the patch: reading it afterwards hands back the refusal,
        # the restore becomes a no-op, and every later test in the file runs against it.
        original = entra_oauth.exchange_code

        def refuse(*args, **kwargs):
            raise entra_oauth.EntraError("invalid_grant")

        auth_routes.entra_oauth.exchange_code = refuse
        self.addCleanup(setattr, auth_routes.entra_oauth, "exchange_code", original)
        _payload, state_value, _cookies = self.start_sign_in()

        _status, payload, _ = self.sign_in(state_value)

        self.assertEqual(payload["error"], "Bad Credentials")
        self.assertNotIn("usr", captured)
        self.assertEqual(self.state.auth_failures_total, 1)
        self.assertEqual(len(self.audits("ENTRA_EXCHANGE")), 1)

    def test_a_state_is_single_use(self):
        self.fake_entra()
        _payload, state_value, _cookies = self.start_sign_in()
        self.sign_in(state_value)

        self.capture_finalize()
        _status, payload, _ = self.sign_in(state_value)

        self.assertEqual(payload["error"], "Bad Credentials")

    def test_a_state_issued_before_entra_existed_is_read_as_github(self):
        # `"1"` is what the state route wrote until this release: an open sign-in must not break.
        # The originals are read *before* the first patch — reading them afterwards hands back the
        # lambda, the restore becomes a no-op and every later test runs against the fake.
        original = {name: getattr(auth_routes.github_oauth, name)
                    for name in ("exchange_code", "fetch_profile")}
        auth_routes.github_oauth.exchange_code = lambda *a, **k: "gho_x"
        auth_routes.github_oauth.fetch_profile = lambda *a, **k: {
            "login": "someone", "email": "someone@example.com", "name": "",
            "firstname": "", "lastname": "", "public_email": ""}
        for name, function in original.items():
            self.addCleanup(setattr, auth_routes.github_oauth, name, function)

        state_value = "legacy-" + str(uuidlib.uuid4())
        self.redis.set("nyx_oauth_state_" + state_value, "1", 600)
        # The cookie comes from the jar: Werkzeug rebuilds the `Cookie` header from it, so a header
        # set by hand would be dropped.
        self.client.set_cookie("nyx_oauth_state", state_value)
        self.capture_finalize()

        _status, payload, _ = self.call("post", "/api/v1/cred/login_sso",
                                        body={"code": "the-code", "state": state_value})

        self.assertEqual(payload["error"], "")


if __name__ == "__main__":
    unittest.main()
