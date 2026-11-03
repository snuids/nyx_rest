"""Editing an account must never touch its password.

The console saves a user by posting the whole document back to `/generic/nyx_user/<id>` — the
same route that creates one. So changing a language, a phone number or a privilege sends the
document again, and the route used to decide "no usable password was supplied" whenever the
field was empty *or* already a hash: it then hashed a fresh random password nobody knows and
raised `mustResetPassword`. An administrator editing a colleague's language locked them out.

These tests pin the distinction: creating an account is not updating one.
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
    from passlib.hash import pbkdf2_sha256                 # noqa: E402
    PASSWORD = "Correct#Pass2026"
    STORED = pbkdf2_sha256.hash(PASSWORD)
else:                                                      # pragma: no cover
    data_routes = pbkdf2_sha256 = None
    PASSWORD = STORED = ""


def accounts():
    return {
        "alice@example.com": {
            "login": "alice@example.com",
            "privileges": ["YUL"],
            "language": "fr",
            "password": STORED,
            "mustResetPassword": False,
        },
        "bob@example.com": {
            "login": "bob@example.com",
            "privileges": ["admin"],
            "language": "fr",
            "password": STORED,
            "mustResetPassword": False,
        },
    }


@unittest.skipUnless(data_routes is not None, "routes.data non importable")
class UpdateAccountTests(RouteTestCase):
    route_modules = (data_routes,)

    def setUp(self):
        super().setUp()
        self.es.docs["nyx_user"] = accounts()
        self.admin = self.login_as("bob@example.com", ["admin"])

    def save(self, document, target="alice@example.com"):
        return self.call("post", "/api/v1/generic/nyx_user/" + target, self.admin, document)

    def stored(self, target="alice@example.com"):
        return self.es.docs["nyx_user"][target]

    def test_changing_the_language_keeps_the_password_when_the_hash_is_sent_back(self):
        document = dict(self.stored())
        document["language"] = "nl"
        code, body, _ = self.save(document)

        self.assertEqual(body["error"], "")
        self.assertEqual(self.stored()["language"], "nl")
        self.assertEqual(self.stored()["password"], STORED,
                         "the hash that comes back from the interface is the stored one, not a "
                         "new secret to mint")
        self.assertFalse(self.stored()["mustResetPassword"])
        self.assertTrue(pbkdf2_sha256.verify(PASSWORD, self.stored()["password"]),
                        "the account must still open with the password the owner knows")

    def test_changing_the_language_keeps_the_password_when_the_field_is_absent(self):
        document = {k: v for k, v in self.stored().items() if k != "password"}
        document["language"] = "en"
        code, body, _ = self.save(document)

        self.assertEqual(body["error"], "")
        self.assertEqual(self.stored()["password"], STORED)
        self.assertFalse(self.stored()["mustResetPassword"])

    def test_an_edit_cannot_clear_a_forced_password_change(self):
        self.es.docs["nyx_user"]["alice@example.com"]["mustResetPassword"] = True
        document = dict(self.stored())
        document["language"] = "nl"
        self.save(document)

        self.assertTrue(self.stored()["mustResetPassword"],
                        "only a successful password change may lift the flag")

    def test_a_new_clear_password_is_hashed(self):
        document = dict(self.stored())
        document["password"] = "Nouveau#Pass2026"
        code, body, _ = self.save(document)

        self.assertEqual(body["error"], "")
        self.assertTrue(self.stored()["password"].startswith("$pbkdf2-sha256$"))
        self.assertTrue(pbkdf2_sha256.verify("Nouveau#Pass2026", self.stored()["password"]))
        self.assertNotIn("Nouveau#Pass2026", json.dumps(self.stored()))

    def test_creating_an_account_without_a_password_still_closes_it(self):
        code, body, _ = self.save({"login": "neuf@example.com", "privileges": ["YUL"]},
                                  target="neuf@example.com")
        self.assertEqual(body["error"], "")
        self.assertTrue(self.stored("neuf@example.com")["mustResetPassword"],
                        "a brand new account with no password must open on nothing")

    def test_a_failed_read_refuses_the_write_instead_of_resetting_the_password(self):
        document = dict(self.stored())
        document["language"] = "nl"
        self.es.get_error = ConnectionError("cluster unavailable")

        code, body, _ = self.save(document)

        self.assertNotEqual(body["error"], "", "a save that could not read must not claim success")
        self.assertEqual(self.stored()["language"], "fr", "nothing may be written")
        self.assertEqual(self.stored()["password"], STORED,
                         "an unreadable account is not a new account: the hash must survive")
        self.assertFalse(self.stored()["mustResetPassword"])
        self.assertEqual(self.es.written_on("nyx_user"), [],
                         "the write must not reach the cluster at all")

    def test_saving_by_login_finds_the_account_indexed_under_another_identifier(self):
        # carol signs in as `carol` but is indexed as carol@example.com
        self.es.docs["nyx_user"]["carol@example.com"] = dict(
            login="carol", privileges=["YUL"], language="fr",
            password=STORED, mustResetPassword=False)
        self.es.foreign_hits["nyx_user"] = [
            {"_id": "carol@example.com", "_index": "nyx_user",
             "_source": dict(self.es.docs["nyx_user"]["carol@example.com"])}
        ]

        code, body, _ = self.save({"login": "carol", "privileges": ["YUL"], "language": "nl"},
                                  target="carol")

        self.assertEqual(body["error"], "")
        written = dict(self.es.written_on("nyx_user"))["carol"]
        self.assertEqual(written["password"], STORED,
                         "the account exists under its e-mail: its hash is the stored one")
        self.assertFalse(written["mustResetPassword"])


if __name__ == "__main__":
    unittest.main()
