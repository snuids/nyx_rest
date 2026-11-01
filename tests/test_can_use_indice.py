"""
Tests for the access control applied to every data query.

`can_use_indice` is the only thing that decides whether a user may read an index, and which rows
of it: a regression here is a data leak, not an inconvenience. The rules come from the `nyx_indice`
documents, matched against the index name with a regular expression.

`es_helpers` only imports `state` and the standard library, so these tests run everywhere.
"""

import sys
import unittest
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sources"))

import state
from es_helpers import can_use_indice


def rule(pattern, **fields):
    return {"_source": {"indicepattern": pattern, **fields}}


def user(privileges=(), filters=()):
    return {"login": "tester", "privileges": list(privileges), "filters": list(filters)}


class CanUseIndiceTests(unittest.TestCase):
    def setUp(self):
        # refresh_indices() must not reach for a cluster during the tests
        state.last_indices_refresh = datetime.now()
        state.indices_refresh_seconds = 60
        state.indices = []

    # -- default behaviour -------------------------------------------------

    def test_an_index_with_no_rule_is_readable_by_everyone(self):
        allowed, query, privilege_column = can_use_indice("deliveries-2026", user(["ops"]), None)
        self.assertTrue(allowed)
        self.assertIsNone(privilege_column)
        self.assertEqual(query["bool"]["must"][0]["query_string"]["query"], "*")

    def test_a_query_without_a_query_string_gets_one(self):
        query = {"bool": {"must": [{"match_all": {}}]}}
        allowed, result, _ = can_use_indice("anything", user(["ops"]), query)
        self.assertTrue(allowed)
        self.assertEqual(result["bool"]["must"][0]["query_string"]["query"], "*")
        self.assertEqual(result["bool"]["must"][1], {"match_all": {}})

    # -- privileges -------------------------------------------------------

    def test_a_rule_with_privileges_denies_a_user_outside_them(self):
        state.indices = [rule("deliveries.*", privileges=["deliveries"])]
        allowed, _, _ = can_use_indice("deliveries-2026", user(["other"]), None)
        self.assertFalse(allowed)

    def test_a_matching_privilege_is_allowed(self):
        state.indices = [rule("deliveries.*", privileges=["deliveries", "admin"])]
        allowed, _, _ = can_use_indice("deliveries-2026", user(["deliveries"]), None)
        self.assertTrue(allowed)

    def test_an_empty_privilege_list_locks_the_index(self):
        # `privileges: []` is not the same as no `privileges` key: the rule denies everyone.
        # Only an empty *string* is treated as "not configured" by can_use_indice, so an index is
        # locked intentionally with an empty list — a plausible accident too, hence this test.
        state.indices = [rule("deliveries.*", privileges=[])]
        self.assertFalse(can_use_indice("deliveries-2026", user(["admin"]), None)[0])
        self.assertFalse(can_use_indice("deliveries-2026", user([]), None)[0])

    # -- row filtering ----------------------------------------------------

    def test_user_filters_are_and_ed_onto_the_existing_query(self):
        state.indices = [rule("deliveries.*", filtercolumn="site")]
        query = {
            "bool": {
                "must": [{"query_string": {"query": "status:open", "default_field": "*"}}]
            }
        }
        allowed, result, _ = can_use_indice("deliveries-2026", user(["ops"], ["A", "B"]), query)
        self.assertTrue(allowed)
        self.assertEqual(
            result["bool"]["must"][0]["query_string"]["query"],
            "status:open AND (site:A OR site:B)",
        )

    def test_user_filters_are_appended_to_a_wildcard_query(self):
        state.indices = [rule("deliveries.*", filtercolumn="site")]
        query = {"bool": {"must": [{"query_string": {"query": "*", "default_field": "*"}}]}}
        allowed, result, _ = can_use_indice("deliveries-2026", user(["ops"], ["A"]), query)
        self.assertTrue(allowed)
        # only an *empty* query is replaced; a wildcard is kept and and-ed, which Elasticsearch
        # accepts and which keeps the operator's intent visible in the log
        self.assertEqual(result["bool"]["must"][0]["query_string"]["query"], "* AND (site:A)")

    def test_a_user_without_filters_is_not_filtered_even_if_the_rule_has_a_column(self):
        state.indices = [rule("deliveries.*", filtercolumn="site")]
        query = {"bool": {"must": [{"query_string": {"query": "*", "default_field": "*"}}]}}
        allowed, result, _ = can_use_indice("deliveries-2026", user(["ops"], []), query)
        self.assertTrue(allowed)
        self.assertEqual(result["bool"]["must"][0]["query_string"]["query"], "*")

    def test_the_privilege_column_is_returned_for_post_filtering(self):
        state.indices = [rule("docs.*", privilegecolumn="visible_to")]
        allowed, _, privilege_column = can_use_indice("docs-2026", user(["ops"]), None)
        self.assertTrue(allowed)
        self.assertEqual(privilege_column, "visible_to")

    def test_the_privilege_column_is_returned_even_when_the_rule_denies(self):
        state.indices = [rule("docs.*", privileges=["other"], privilegecolumn="visible_to")]
        allowed, _, privilege_column = can_use_indice("docs-2026", user(["ops"]), None)
        self.assertFalse(allowed)
        self.assertEqual(privilege_column, "visible_to")

    # -- matching semantics ----------------------------------------------

    def test_the_pattern_is_a_substring_match_not_an_equality(self):
        # documents this footgun: "app" also covers "myapp_prod" and "apps-archive"
        state.indices = [rule("app", privileges=["only-this"])]
        self.assertFalse(can_use_indice("myapp_prod", user(["ops"]), None)[0])
        self.assertFalse(can_use_indice("apps-archive", user(["ops"]), None)[0])

    def test_a_regex_pattern_restricts_the_match(self):
        state.indices = [rule("^deliveries.*", privileges=["deliveries"])]
        self.assertTrue(can_use_indice("deliveries-2026", user(["deliveries"]), None)[0])
        self.assertTrue(can_use_indice("other-2026", user(["deliveries"]), None)[0])

    def test_the_first_matching_rule_wins(self):
        # the loop returns on the first rule whose pattern matches, so a later, stricter rule
        # on the same index is never evaluated: this is the current contract, pinned here
        state.indices = [
            rule("deliveries.*", filtercolumn="site"),
            rule("deliveries-2026", privileges=["other"]),
        ]
        query = {"bool": {"must": [{"query_string": {"query": "*", "default_field": "*"}}]}}
        allowed, result, _ = can_use_indice("deliveries-2026", user(["ops"], ["A"]), query)
        self.assertTrue(allowed)
        self.assertEqual(result["bool"]["must"][0]["query_string"]["query"], "* AND (site:A)")

    def test_a_non_matching_pattern_leaves_the_query_alone(self):
        state.indices = [rule("^archived-.*", privileges=["never"])]
        query = {"bool": {"must": [{"query_string": {"query": "status:open", "default_field": "*"}}]}}
        allowed, result, _ = can_use_indice("deliveries-2026", user(["ops"]), query)
        self.assertTrue(allowed)
        self.assertEqual(result["bool"]["must"][0]["query_string"]["query"], "status:open")


if __name__ == "__main__":
    unittest.main()
