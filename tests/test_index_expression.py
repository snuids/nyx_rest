"""Tests for how an index expression is read.

Elasticsearch accepts a comma-separated list wherever a single index name is accepted. The access
rule used to decide on the whole string, so `nyx_app,nyx_user` looked like "not the account index"
and the own-record filter was skipped — on a query that did return every account, hashes included.
These tests pin the spelling down, because the failure is silent: the query succeeds, and it
returns what it should not.
"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sources"))

from lib.index_expression import expression_reaches  # noqa: E402

# `es_helpers` imports `state`, which needs the runtime dependencies (cachetools, elasticsearch…):
# the account-index check is skipped where they are absent, like the rest of the suite.
try:
    from es_helpers import is_nyx_user_indice  # noqa: E402
except ImportError:  # pragma: no cover - depends on the environment
    is_nyx_user_indice = None


class IndexExpressionTest(unittest.TestCase):
    def test_exact_name(self):
        self.assertTrue(expression_reaches("nyx_user", "nyx_user"))

    def test_reindexed_alias(self):
        self.assertTrue(expression_reaches("reindexed-nyx_user-v8", "reindexed-nyx_user"))
        self.assertTrue(expression_reaches("nyx_user.2026", "nyx_user"))

    def test_patterns(self):
        self.assertTrue(expression_reaches("*", "nyx_user"))
        self.assertTrue(expression_reaches("nyx_*", "nyx_user"))
        self.assertTrue(expression_reaches("nyx_user*", "nyx_user"))
        self.assertFalse(expression_reaches("nyx_apicalls*", "nyx_user"))

    def test_case_and_whitespace_insensitive(self):
        self.assertTrue(expression_reaches(" NYX_USER ", "nyx_user"))

    def test_unrelated_name(self):
        self.assertFalse(expression_reaches("nyx_app", "nyx_user"))
        self.assertFalse(expression_reaches("nyx_user2", "nyx_user"))

    def test_comma_list_reaches_when_one_part_does(self):
        # The case that mattered: a list is not a name, and one part is enough.
        self.assertTrue(expression_reaches("nyx_app,nyx_user", "nyx_user"))
        self.assertTrue(expression_reaches("nyx_user,nyx_app", "nyx_user"))
        self.assertTrue(expression_reaches("nyx_app, *", "nyx_user"))
        self.assertTrue(expression_reaches("nyx_app,nyx_user*", "nyx_user"))
        self.assertTrue(expression_reaches("nyx_app, reindexed-nyx_user-v8", "reindexed-nyx_user"))

    def test_comma_list_without_the_root(self):
        self.assertFalse(expression_reaches("nyx_app,nyx_apicalls", "nyx_user"))

    def test_empty(self):
        for value in ("", None, "  ", ","):
            self.assertFalse(expression_reaches(value, "nyx_user"))


@unittest.skipUnless(is_nyx_user_indice is not None, "runtime dependencies absent")
class AccountIndexTest(unittest.TestCase):
    def test_account_index_detection_follows(self):
        # `is_nyx_user_indice` is what the read filter hangs on: it must see the list too.
        self.assertTrue(is_nyx_user_indice("nyx_app,nyx_user"))
        self.assertTrue(is_nyx_user_indice("reindexed-nyx_user-v8"))
        self.assertFalse(is_nyx_user_indice("nyx_app,nyx_apicalls"))


if __name__ == "__main__":
    unittest.main()
