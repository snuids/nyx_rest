"""Reading `nyx_user`: an ordinary account only sees its own record.

The index carries the pbkdf2 hashes of every account, and `can_use_indice` only receives an
index name — it therefore cannot tell *which* document is requested. Two checks are needed
and must refuse the same thing, otherwise the restriction is bypassed through the other
route:

* `can_use_indice` adds a `bool.filter` clause ("own account") to every search or
  aggregation on the index, hence to `generic_search`, `queryFilter` and the exports;
* `can_read_nyx_user` guards the direct read `GET /generic/nyx_user/<id>`.

An `admin` account is not restricted: the user administration screen needs it.
"""

import sys
import unittest
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sources"))

import state
from es_helpers import (
    can_read_nyx_user,
    can_use_indice,
    is_admin,
    is_nyx_user_indice,
    nyx_user_read_filter,
    own_account_ids,
    requires_admin_for_write,
)


def rule(pattern, **fields):
    return {"_source": {"indicepattern": pattern, **fields}}


def user(login="bob@example.com", privileges=(), filters=(), **extra):
    return {"login": login, "privileges": list(privileges),
            "filters": list(filters), **extra}


def filters_of(query):
    return (query.get("bool") or {}).get("filter") or []


class NyxUserIndexDetectionTests(unittest.TestCase):
    def test_the_index_and_its_reindex_alias_are_recognised(self):
        # production serves an alias: naming `reindexed-nyx_user-v8` in generic_search
        # must fall under the same rule as `nyx_user`
        self.assertTrue(is_nyx_user_indice("nyx_user"))
        self.assertTrue(is_nyx_user_indice("reindexed-nyx_user-v8"))

    def test_a_neighbouring_index_is_not_recognised(self):
        for name in ("nyx_user_history", "nyx_users", "nyx_userfoo", "not_nyx_user", ""):
            self.assertFalse(is_nyx_user_indice(name), name)

    def test_a_wildcard_that_designates_nyx_user_is_recognised(self):
        # the interface lists accounts through `generic_search/nyx_user*`: without that
        # form, the restriction was bypassed with a single asterisk
        for name in ("nyx_user*", "nyx_us*", "nyx_*", "*", "?yx_user", "reindexed-nyx_user*"):
            self.assertTrue(is_nyx_user_indice(name), name)

    def test_a_wildcard_on_another_family_is_left_alone(self):
        # four production screens read a family this way
        for name in ("nyx_apicalls*", "nyx_energy*", "nyx_alert*", "airport_udropstatus*",
                     "deliveries-*"):
            self.assertFalse(is_nyx_user_indice(name), name)

    def test_a_missing_index_name_is_not_a_crash(self):
        self.assertFalse(is_nyx_user_indice(None))


class OwnAccountIdentityTests(unittest.TestCase):
    def test_the_login_and_the_document_id_are_both_kept(self):
        # `_id` carries the e-mail and `login` the short name on eight accounts: either of
        # the two is enough to read one's own record, so both are kept
        self.assertEqual(own_account_ids({"id": "bob@example.com", "login": "bob"}),
                         ["bob@example.com", "bob"])

    def test_a_token_without_identity_yields_no_ids(self):
        self.assertEqual(own_account_ids({}), [])
        self.assertEqual(own_account_ids(None), [])
        self.assertEqual(own_account_ids({"login": "", "id": None}), [])

    def test_the_filter_matches_the_id_or_the_login(self):
        clause = nyx_user_read_filter({"id": "bob@example.com", "login": "bob"})
        should = clause["bool"]["should"]
        self.assertIn({"ids": {"values": ["bob@example.com", "bob"]}}, should)
        self.assertIn({"terms": {"login.keyword": ["bob@example.com", "bob"]}}, should)
        self.assertEqual(clause["bool"]["minimum_should_match"], 1)

    def test_no_filter_without_identity(self):
        self.assertIsNone(nyx_user_read_filter({}))


class WriteRestrictionTests(unittest.TestCase):
    """Indices that carry rights are read-only for a non-admin."""

    def test_the_account_and_configuration_indices_are_protected(self):
        for name in ("nyx_user", "reindexed-nyx_user-v8", "nyx_app", "nyx_privilege",
                     "nyx_privileges", "nyx_privilege-history"):
            self.assertTrue(requires_admin_for_write(name), name)

    def test_the_access_rule_and_filter_catalogues_are_protected(self):
        # nyx_indice says who accesses which index, nyx_filter who sees which rows:
        # rewriting them would amount to granting oneself rights without touching
        # nyx_privilege.
        for name in ("nyx_indice", "reindexed-nyx_indice-v8", "nyx_filter",
                     "reindexed-nyx_filter", "nyx_indice*", "nyx_filter-2026"):
            self.assertTrue(requires_admin_for_write(name), name)

    def test_data_indices_are_not_protected(self):
        # a business app must stay editable by its users
        for name in ("airport_alert-2026.11", "nyx_energy-2026.11", "nyx_lambda",
                     "nyx_user_history", "deliveries-2026", "nyx_indice_history",
                     "nyx_filters_business"):
            self.assertFalse(requires_admin_for_write(name), name)

    def test_the_guard_does_not_crash_on_a_missing_name(self):
        self.assertFalse(requires_admin_for_write(None))
        self.assertFalse(requires_admin_for_write(""))

    def test_only_the_admin_privilege_counts(self):
        self.assertTrue(is_admin({"privileges": ["admin"]}))
        self.assertFalse(is_admin({"privileges": ["YUL", "report"]}))
        self.assertFalse(is_admin({}))
        self.assertFalse(is_admin(None))


class SearchRestrictionTests(unittest.TestCase):
    def setUp(self):
        state.last_indices_refresh = datetime.now()
        state.indices_refresh_seconds = 60
        state.indices = []

    def test_a_non_admin_reading_nyx_user_is_restricted_to_their_own_account(self):
        allowed, query, _ = can_use_indice("nyx_user", user(), None)
        self.assertTrue(allowed)
        self.assertEqual(filters_of(query), [nyx_user_read_filter(user())])

    def test_the_restriction_also_covers_the_reindex_alias(self):
        allowed, query, _ = can_use_indice("reindexed-nyx_user-v8", user(), None)
        self.assertTrue(allowed)
        self.assertEqual(len(filters_of(query)), 1)

    def test_the_restriction_also_covers_the_wildcard_form(self):
        # `generic_search/nyx_user*`: the form the interface uses to list the accounts, and
        # the one that let the 21 hashes through
        allowed, query, _ = can_use_indice("nyx_user*", user(), None)
        self.assertTrue(allowed)
        self.assertEqual(len(filters_of(query)), 1)

    def test_an_admin_keeps_the_whole_index(self):
        # without that exception, the account administration screen would only display
        # oneself
        allowed, query, _ = can_use_indice("nyx_user", user(privileges=["admin"]), None)
        self.assertTrue(allowed)
        self.assertEqual(filters_of(query), [])

    def test_a_token_without_identity_is_refused_rather_than_broadened(self):
        # fail closed: an incomplete token must not hand over the whole index
        allowed, _, _ = can_use_indice("nyx_user", {"privileges": ["YUL"]}, None)
        self.assertFalse(allowed)

    def test_another_index_is_left_untouched(self):
        allowed, query, _ = can_use_indice("deliveries-2026", user(), None)
        self.assertTrue(allowed)
        self.assertEqual(filters_of(query), [])

    def test_the_restriction_survives_the_row_filter_branch(self):
        # the `filtercolumn` branch rewrites the query_string then returns: the filter added
        # before the rule loop must therefore be carried by `bool.filter`, not by the string
        state.indices = [rule("nyx_user", filtercolumn="site")]
        allowed, query, _ = can_use_indice("nyx_user", user(filters=["A"]), None)
        self.assertTrue(allowed)
        self.assertIn("site:A", query["bool"]["must"][0]["query_string"]["query"])
        self.assertEqual(len(filters_of(query)), 1)

    def test_the_restriction_survives_a_user_query(self):
        query = {"bool": {"must": [{"query_string": {"query": "privileges:admin",
                                                     "default_field": "*"}}]}}
        allowed, result, _ = can_use_indice("nyx_user", user(), query)
        self.assertTrue(allowed)
        self.assertEqual(result["bool"]["must"][0]["query_string"]["query"], "privileges:admin")
        self.assertEqual(len(filters_of(result)), 1, "le filtre doit être and-é, pas substitué")

    def test_an_existing_filter_list_is_kept_and_completed(self):
        query = {"bool": {"must": [{"match_all": {}}],
                          "filter": [{"term": {"active": True}}]}}
        allowed, result, _ = can_use_indice("nyx_user", user(), query)
        self.assertTrue(allowed)
        self.assertEqual(len(filters_of(result)), 2)


class DirectReadTests(unittest.TestCase):
    def test_a_non_admin_reads_their_own_document(self):
        u = {"id": "bob@example.com", "login": "bob", "privileges": ["YUL"]}
        self.assertTrue(can_read_nyx_user(u, "bob@example.com"))
        self.assertTrue(can_read_nyx_user(u, "bob"))

    def test_a_non_admin_cannot_read_another_account(self):
        u = {"id": "bob@example.com", "login": "bob", "privileges": ["YUL"]}
        self.assertFalse(can_read_nyx_user(u, "alice@example.com"))
        self.assertFalse(can_read_nyx_user(u, ""))

    def test_an_admin_reads_any_account(self):
        u = {"id": "root@example.com", "login": "root", "privileges": ["admin"]}
        self.assertTrue(can_read_nyx_user(u, "alice@example.com"))

    def test_no_user_reads_nothing(self):
        self.assertFalse(can_read_nyx_user(None, "alice@example.com"))
        self.assertFalse(can_read_nyx_user({}, "alice@example.com"))


if __name__ == "__main__":
    unittest.main()
