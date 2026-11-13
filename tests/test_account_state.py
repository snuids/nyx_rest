"""The `inactive` flag, from the sign-in to every request that follows.

The flag was only read once, when the token was minted: an account disabled by an administrator kept
its session for the day Redis holds the token, which is exactly the window the switch is meant to
close. Two rules are pinned here:

* a request carrying the token of a disabled account is refused with `ACCOUNT_INACTIVE` and the
  token is dropped — the console turns that into the page that explains it;
* the document is read through a one-minute Redis cache, so the API does not take an Elasticsearch
  round trip per call — and the answer is dropped the moment a route writes an account, so an
  administrator's decision takes effect on the next request, not in a minute.
"""

import json
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sources"))

from route_harness import RouteTestCase, HAVE_ROUTE_DEPS          # noqa: E402

if HAVE_ROUTE_DEPS:
    import routes.auth as auth_routes                            # noqa: E402
    import routes.data as data_routes                            # noqa: E402
    from lib import account_state                                # noqa: E402
else:                                                            # pragma: no cover
    auth_routes = data_routes = account_state = None


@unittest.skipUnless(HAVE_ROUTE_DEPS, "route dependencies missing")
class FlagReadingTests(unittest.TestCase):
    """What the flag means, on the three shapes a document may take."""

    def test_an_absent_field_is_an_active_account(self):
        self.assertFalse(account_state.is_inactive({"_source": {"login": "alice"}}))
        self.assertFalse(account_state.is_inactive({"login": "alice"}))

    def test_an_explicit_false_is_active(self):
        self.assertFalse(account_state.is_inactive({"_source": {"inactive": False}}))

    def test_the_word_or_the_number_also_disables(self):
        for value in (True, "true", "TRUE", " true ", 1, "1", "yes"):
            with self.subTest(value=value):
                self.assertTrue(account_state.is_inactive({"_source": {"inactive": value}}))

    def test_a_word_that_is_not_yes_stays_active(self):
        for value in ("false", "no", "0", ""):
            with self.subTest(value=value):
                self.assertFalse(account_state.is_inactive({"_source": {"inactive": value}}))


@unittest.skipUnless(HAVE_ROUTE_DEPS, "route dependencies missing")
class DisabledSessionTests(RouteTestCase):
    """A session opened before the account was disabled ends at the next request."""

    route_modules = (auth_routes,)
    documents = {"nyx_user": {"alice@example.com": {
        "login": "alice@example.com", "privileges": ["user"], "language": "en",
        "filters": [], "inactive": False,
    }}}

    def setUp(self):
        super().setUp()
        self.token = self.login_as("alice@example.com", ["user"],
                                   docid="alice@example.com")

    def ask(self, token=None):
        # any route behind `token_required`: saving a preference touches nothing else
        return self.call("post", "/api/v1/cred/preferences", token or self.token,
                         {"theme": "dark"})

    def read_count(self):
        return sum(1 for call in self.es.calls
                   if call[0] == "get" and call[1] == "nyx_user")

    # -- the cut ----------------------------------------------------------
    def test_an_active_account_keeps_its_session(self):
        code, payload, _ = self.ask()

        self.assertEqual(code, 200)
        self.assertEqual(payload["error"], "")

    def test_the_session_ends_once_the_document_says_disabled(self):
        self.es.docs["nyx_user"]["alice@example.com"]["inactive"] = True
        account_state.forget("alice@example.com")

        code, payload, _ = self.ask()

        self.assertEqual(code, 401, "a cut session is a 401, not a 200 carrying a word")
        self.assertEqual(payload["error"], "ACCOUNT_INACTIVE")

    def test_the_token_is_dropped_so_it_cannot_be_replayed(self):
        self.es.docs["nyx_user"]["alice@example.com"]["inactive"] = True
        account_state.forget("alice@example.com")

        self.ask()

        self.assertNotIn(self.token, self.state.tokens)
        self.assertIsNone(self.redis.store.get("nyx_tok_" + self.token))

    def test_the_calls_that_arrive_after_the_cut_still_hear_the_reason(self):
        # the console makes several calls at once: the ones landing after the drop must not read
        # "unknown token", which the console turns into "lost session, try again"
        self.es.docs["nyx_user"]["alice@example.com"]["inactive"] = True
        account_state.forget("alice@example.com")

        self.ask()                          # cuts the session
        code, payload, _ = self.ask()       # another call, same dead token

        self.assertEqual((code, payload["error"]), (401, "ACCOUNT_INACTIVE"))
        self.assertEqual(self.redis.ttls.get("nyx_tokcut_" + self.token), 300)

    def test_the_reason_expires(self):
        # five minutes later the token is simply unknown: the console is on the page by then
        self.es.docs["nyx_user"]["alice@example.com"]["inactive"] = True
        account_state.forget("alice@example.com")
        self.ask()
        del self.redis.store["nyx_tokcut_" + self.token]

        code, payload, _ = self.ask()

        self.assertEqual((code, payload["error"]), (401, "UNKNOWN_TOKEN"))

    def test_a_token_that_never_existed_still_says_unknown(self):
        code, payload, _ = self.ask("never-issued")

        self.assertEqual((code, payload["error"]), (401, "UNKNOWN_TOKEN"))

    def test_a_cut_session_is_not_told_apart_from_a_lost_token_by_accident(self):
        # the console reads the body, the status is for everything else: both must say the truth
        self.es.docs["nyx_user"]["alice@example.com"]["inactive"] = True
        account_state.forget("alice@example.com")

        code, payload, _ = self.ask()

        self.assertEqual((code, payload["error"]), (401, "ACCOUNT_INACTIVE"))
        self.assertNotEqual(payload["error"], "UNKNOWN_TOKEN")

    # -- the cache --------------------------------------------------------
    def test_the_document_is_read_once_a_minute_not_once_a_request(self):
        self.ask()
        first = self.read_count()
        self.ask()
        self.ask()

        self.assertEqual(first, 1)
        self.assertEqual(self.read_count(), 1, "the cache answers the next requests")
        self.assertEqual(self.redis.ttls.get("nyx_accountstate_alice@example.com"), 60)

    def test_forgetting_the_answer_makes_the_next_request_read_again(self):
        self.ask()
        account_state.forget("alice@example.com")
        self.ask()

        self.assertEqual(self.read_count(), 2)

    def test_the_cache_carries_the_decision_not_a_copy_of_the_document(self):
        self.ask()
        cached = self.redis.store.get("nyx_accountstate_alice@example.com")

        self.assertIn(cached, ("active", b"active"))

    # -- the failure modes ------------------------------------------------
    def test_an_unreadable_document_keeps_the_session(self):
        # an outage must not lock the platform out: the check closes a door, it does not guard it
        account_state.forget("alice@example.com")
        self.es.get_error = RuntimeError("cluster down")

        code, payload, _ = self.ask()

        self.assertEqual(code, 200)
        self.assertEqual(payload["error"], "")

    def test_a_session_without_a_document_is_left_alone(self):
        # the AD path builds its payload from the directory, there is no flag to read
        token = self.login_as("ad-user", ["user"])

        code, payload, _ = self.ask(token)

        self.assertNotEqual(payload.get("error"), "ACCOUNT_INACTIVE")


@unittest.skipUnless(HAVE_ROUTE_DEPS, "route dependencies missing")
class WrittenAccountTests(RouteTestCase):
    """The console's own write drops the cached answer, so the switch is immediate."""

    route_modules = (data_routes, auth_routes)
    documents = {"nyx_user": {"alice@example.com": {
        "login": "alice@example.com", "privileges": ["user"], "language": "en",
        "filters": [], "password": "$pbkdf2-sha256$29000$x$y",
    }}}

    def setUp(self):
        super().setUp()
        self.admin = self.login_as("root@example.com", ["admin"])
        # the cache holds "active" for Alice, as any request of hers would have left it
        self.redis.set("nyx_accountstate_alice@example.com", "active", 60)

    def test_disabling_an_account_from_the_console_drops_the_cached_answer(self):
        _, payload, _ = self.call("post", "/api/v1/generic/nyx_user/alice@example.com",
                                  self.admin,
                                  {"login": "alice@example.com", "inactive": True,
                                   "privileges": ["user"]})

        self.assertEqual(payload["error"], "")
        self.assertIsNone(self.redis.store.get("nyx_accountstate_alice@example.com"),
                          "the next request must read the document, not a minute-old answer")

    def test_deleting_an_account_drops_it_too(self):
        self.call("delete", "/api/v1/generic/nyx_user/alice@example.com", self.admin)

        self.assertIsNone(self.redis.store.get("nyx_accountstate_alice@example.com"))

    def test_a_write_on_another_index_leaves_the_cache_alone(self):
        self.call("post", "/api/v1/generic/nyx_app/rec-1", self.admin,
                  {"title": "Forms", "config": {}})

        self.assertEqual(self.redis.store.get("nyx_accountstate_alice@example.com"), "active",
                         "only an account write may invalidate an account state")
