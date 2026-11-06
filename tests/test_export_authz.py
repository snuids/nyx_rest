"""Tests for the access control the queue worker applies before exporting.

The worker reads Elasticsearch directly, outside the API: the check it does on its own is the only
thing between a message on `/queue/REST_LOAD_DATA` and a CSV of whatever index it names. Two
failure modes are tested here, because both are silent in production: exporting an index the
account may not read, and exporting the account index at all — it has no row filter, so the only
sane answer for a non-administrator is no.
"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sources"))

from lib.export_authz import kibana_indices, load_rules, may_export  # noqa: E402


def rule(pattern, privileges="", **fields):
    return {"indicepattern": pattern, "privileges": privileges, **fields}


RULES = [
    rule("^nyx_apicalls", "A1"),
    rule("^nyx_energy$", "E1,E2"),
    rule("^airport_", ""),          # open index: no privilege declared
]

ENGINEER = {"id": "alice@example.com", "privileges": ["E1"]}
ADMIN = {"id": "root@example.com", "privileges": ["admin"]}


class MayExportTest(unittest.TestCase):
    def test_administrator_exports_anything(self):
        self.assertTrue(may_export("nyx_user", ADMIN, RULES))
        self.assertTrue(may_export("nyx_apicalls-2026.11", ADMIN, RULES))
        self.assertTrue(may_export("anything", ADMIN, None))

    def test_account_index_refused_except_for_administrators(self):
        # No row filter exists on this path: a read of one's own record is not an export.
        self.assertFalse(may_export("nyx_user", ENGINEER, RULES))
        self.assertFalse(may_export("reindexed-nyx_user-v8", ENGINEER, RULES))
        self.assertFalse(may_export("nyx_app,nyx_user", ENGINEER, RULES))

    def test_missing_privilege_refuses(self):
        self.assertFalse(may_export("nyx_apicalls-2026.11", ENGINEER, RULES))

    def test_present_privilege_allows(self):
        self.assertTrue(may_export("nyx_energy", ENGINEER, RULES))

    def test_index_without_declared_privileges_is_open(self):
        self.assertTrue(may_export("airport_udropstatus", ENGINEER, RULES))

    def test_unknown_rules_refuse(self):
        # Failing closed matters: a rule set that cannot be read must not grant anything.
        self.assertFalse(may_export("nyx_energy", ENGINEER, None))

    def test_unknown_indice_or_user_refuses(self):
        self.assertFalse(may_export("", ENGINEER, RULES))
        self.assertFalse(may_export("nyx_energy", None, RULES))
        self.assertFalse(may_export("nyx_energy", {}, RULES))

    def test_invalid_pattern_does_not_grant(self):
        # A malformed rule must be ignored, not treated as "no rule matched, therefore allowed".
        self.assertFalse(may_export("nyx_secret", ENGINEER, [rule("(unclosed", "A1")]))


class KibanaIndicesTest(unittest.TestCase):
    def test_reads_every_index_line_of_an_msearch_body(self):
        body = 'nyx_energy,nyx_app\n{"query":{"match_all":{}}}\nnyx_user\n{"size":0}'
        self.assertEqual(kibana_indices(body), ["nyx_energy", "nyx_app", "nyx_user"])

    def test_body_without_an_index_line_yields_nothing(self):
        # Nothing to authorise means nothing is exported: the worker refuses an empty list.
        self.assertEqual(kibana_indices('{"query":{"match_all":{}}}'), [])

    def test_missing_body(self):
        for value in (None, ""):
            self.assertEqual(kibana_indices(value), [])


class FakeClient:
    """Minimal stand-in for the OpenSearch client: only `search` is used."""

    def __init__(self, hits=None, error=None):
        self.hits = hits
        self.error = error
        self.calls = []

    def search(self, index=None, body=None):
        self.calls.append((index, body))
        if self.error:
            raise self.error
        return {"hits": {"hits": [{"_source": source} for source in self.hits or []]}}


class LoadRulesTest(unittest.TestCase):
    def test_reads_the_index_rules(self):
        client = FakeClient(hits=[rule("^nyx_energy$", "E1")])
        rules = load_rules(client)
        self.assertEqual(len(rules), 1)
        self.assertEqual(client.calls[0][0], "nyx_indice")

    def test_unreadable_rules_return_none(self):
        self.assertIsNone(load_rules(FakeClient(error=RuntimeError("cluster down"))))


if __name__ == "__main__":
    unittest.main()
