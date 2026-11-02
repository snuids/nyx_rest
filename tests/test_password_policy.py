"""Password policy: provisional passwords and throttled access.

These rules replace the shared hash the interface sent for every new account. They are tested
without Flask or Redis: they are pure functions.
"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sources"))

import password_policy as policy


class TemporaryPasswordTest(unittest.TestCase):
    def test_a_generated_password_is_long_and_url_safe(self):
        mot = policy.generate_temporary_password()
        self.assertGreaterEqual(len(mot), 24)
        self.assertRegex(mot, r"^[A-Za-z0-9_-]+$", "le mot de passe doit être recopiable sans ambiguïté")

    def test_two_accounts_never_share_a_password(self):
        """The defect being fixed: one hash common to every account."""
        mots = {policy.generate_temporary_password() for _ in range(200)}
        self.assertEqual(len(mots), 200)


class AccessBeforeChangeTest(unittest.TestCase):
    def test_a_normal_account_is_not_restricted(self):
        self.assertFalse(policy.must_change_password({"login": "bob"}, "/api/v1/generic/nyx_app"))
        self.assertFalse(policy.must_change_password({}, "/api/v1/generic/nyx_app"))
        self.assertFalse(policy.must_change_password(None, "/api/v1/generic/nyx_app"))

    def test_a_provisional_password_only_opens_the_change_route(self):
        usr = {"login": "bob", "mustResetPassword": True}
        self.assertTrue(policy.must_change_password(usr, "/api/v1/generic/nyx_app"))
        self.assertTrue(policy.must_change_password(usr, "/api/v1/config"))
        self.assertTrue(policy.must_change_password(usr, "/api/v1/generic_search/nyx_user-*"))

    def test_the_change_route_and_logout_stay_open(self):
        usr = {"login": "bob", "mustResetPassword": True}
        for chemin in ("/api/v1/cred/changepassword",
                       "/api/v1/cred/changepassword/",
                       "/api/v1/cred/logout",
                       "/api/v1/healthz",
                       "/api/v1/metrics"):
            self.assertFalse(policy.must_change_password(usr, chemin), chemin)

    def test_a_false_flag_does_not_restrict(self):
        """After the change, the value is False and not absent: it must open the API."""
        self.assertFalse(policy.must_change_password({"login": "bob", "mustResetPassword": False},
                                                     "/api/v1/generic/nyx_app"))


class NewPasswordTest(unittest.TestCase):
    def test_a_short_password_is_refused(self):
        self.assertIn("12", policy.check_new_password("court"))

    def test_an_empty_password_is_refused(self):
        self.assertIn("12", policy.check_new_password(""))
        self.assertIn("12", policy.check_new_password(None))

    def test_the_login_itself_is_refused(self):
        self.assertIn("identifiant", policy.check_new_password("amarchand@icloud.com",
                                                               "amarchand@icloud.com"))

    def test_a_correct_password_passes(self):
        self.assertEqual(policy.check_new_password("cheval-pile-agrafe-42", "amarchand"), "")


if __name__ == "__main__":
    unittest.main()
