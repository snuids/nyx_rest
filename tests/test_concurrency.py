"""`lib/concurrency`: reading the `If-Match` of a write.

The parsing is tested on its own because the route tests can only show the shapes the console
happens to send. What matters here is the boundary: everything that is not a version must be
refused rather than quietly turned into "no precondition", since a check silently disabled is
worse than no check at all.
"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sources"))

from lib.concurrency import (                                    # noqa: E402
    WILDCARD, BadPrecondition, conflict_body, etag_for, is_conflict, parse_precondition,
    write_condition,
)


class ParsePreconditionTests(unittest.TestCase):
    def test_no_header_and_no_parameter_is_no_precondition(self):
        self.assertEqual(parse_precondition(None, None, None), (None, None))
        self.assertEqual(parse_precondition("", "", ""), (None, None))
        self.assertEqual(parse_precondition("   ", None, None), (None, None))
        self.assertEqual(write_condition(parse_precondition(None, None, None)), {},
                         "un appelant qui n'affirme rien doit garder le comportement historique")

    def test_a_quoted_pair(self):
        self.assertEqual(parse_precondition('"12-1"'), (12, 1))
        self.assertEqual(write_condition((12, 1)), {"if_seq_no": 12, "if_primary_term": 1})

    def test_a_bare_sequence_defaults_the_primary_term(self):
        # what a hand-written curl sends, and what an ETag looks like to most clients
        self.assertEqual(parse_precondition('"12"'), (12, 1))
        self.assertEqual(parse_precondition("12"), (12, 1))

    def test_a_weak_validator_is_accepted_for_its_number(self):
        self.assertEqual(parse_precondition('W/"7-2"'), (7, 2))

    def test_a_wildcard_means_the_document_must_exist(self):
        self.assertEqual(parse_precondition("*"), (WILDCARD, None))
        self.assertEqual(parse_precondition('"*"'), (WILDCARD, None))
        self.assertEqual(write_condition((WILDCARD, None)), {},
                         "le cluster n'a pas de drapeau 'doit exister' : la route lit d'abord")

    def test_the_query_string_covers_a_client_that_cannot_set_a_header(self):
        self.assertEqual(parse_precondition(None, "4", None), (4, 1))
        self.assertEqual(parse_precondition(None, "4", "2"), (4, 2))

    def test_the_header_wins_over_the_query_string(self):
        self.assertEqual(parse_precondition('"9-1"', "4", "2"), (9, 1))

    def test_an_unreadable_value_is_refused(self):
        for bad in ("abc", "1-2-3", "x-1", "-1", "1-0", "1.5"):
            with self.assertRaises(BadPrecondition, msg=bad):
                parse_precondition(bad)
        for bad in (("abc", None), ("1", "zero")):
            with self.assertRaises(BadPrecondition, msg=str(bad)):
                parse_precondition(None, bad[0], bad[1])
        # a primary term alone asserts nothing: without a sequence there is no version to
        # compare, and refusing would break a caller that only ever sends nothing
        self.assertEqual(parse_precondition(None, None, "abc"), (None, None))

    def test_zero_is_a_version(self):
        # the first write of a document carries `_seq_no: 0`
        self.assertEqual(parse_precondition('"0-1"'), (0, 1))

    def test_a_negative_number_in_the_query_string_is_refused(self):
        with self.assertRaises(BadPrecondition):
            parse_precondition(None, "-1", "1")


class EtagTests(unittest.TestCase):
    def test_the_etag_and_the_header_agree(self):
        self.assertEqual(etag_for(3, 1), '"3-1"')
        self.assertEqual(parse_precondition(etag_for(3, 1)), (3, 1),
                         "ce que la lecture sert doit etre ce que l'ecriture accepte")


class IsConflictTests(unittest.TestCase):
    def test_a_status_code_or_the_opensearch_name(self):
        class WithStatus(Exception):
            status_code = 409

        class ConflictError(Exception):
            pass

        class Outage(Exception):
            status_code = 503

        self.assertTrue(is_conflict(WithStatus()))
        self.assertTrue(is_conflict(ConflictError()))
        self.assertFalse(is_conflict(Outage()))
        self.assertFalse(is_conflict(Exception("boom")))


class ConflictBodyTests(unittest.TestCase):
    """What a refused write answers. The hash rule is pinned here, not only in the route."""

    def _es(self, document=None, error=None):
        class FakeES:
            def get(self, index=None, id=None):
                if error is not None:
                    raise error
                if document is None:
                    raise LookupError("missing")

                return document
        return FakeES()

    def test_the_winning_document_comes_back_with_its_version(self):
        doc = {"_id": "1", "_index": "rows", "_seq_no": 4, "_primary_term": 1,
               "_source": {"site": "A"}}
        body, tag = conflict_body(self._es(doc), "rows", "1")
        self.assertEqual(body["error"], "conflict")
        self.assertEqual(body["data"]["_source"]["site"], "A")
        self.assertEqual(body["seq_no"], 4)
        self.assertEqual(tag, '"4-1"')

    def test_the_password_hash_is_stripped_whatever_the_index(self):
        doc = {"_id": "a@b.c", "_index": "nyx_user", "_seq_no": 2, "_primary_term": 1,
               "_source": {"login": "a@b.c", "password": "$pbkdf2-sha256$29000$x"}}
        body, _ = conflict_body(self._es(doc), "nyx_user", "a@b.c")
        self.assertEqual(body["data"]["_source"]["password"], "")
        self.assertIn("a@b.c", body["data"]["_source"]["login"])

    def test_an_unreadable_document_still_answers(self):
        # the write was refused anyway: the caller must not lose that answer to a second
        # failure, but it gets no version to retry with
        body, tag = conflict_body(self._es(None), "rows", "1")
        self.assertEqual(body["error"], "conflict")
        self.assertIsNone(body["data"])
        self.assertIsNone(tag)

    def test_a_document_without_a_version_answers_without_an_etag(self):
        # a client cannot retry with a version it was not given
        doc = {"_id": "1", "_index": "rows", "_source": {"site": "A"}}
        body, tag = conflict_body(self._es(doc), "rows", "1")
        self.assertEqual(body["error"], "conflict")
        self.assertIsNone(tag)
        self.assertEqual(body["data"]["_source"]["site"], "A")


if __name__ == "__main__":
    unittest.main()
