"""Aggregation of the counters between workers: pure functions, tested without Redis."""

import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sources"))

import metrics_aggregate as agg


class AggregateTest(unittest.TestCase):
    def test_three_workers_are_added_up(self):
        now = 1000
        entries = [(str(pid), json.dumps({"at": now, "requests_total": n, "errors_total": 0}))
                   for pid, n in ((7, 10), (9, 5), (11, 2))]
        totals = agg.aggregate(entries, now=now)
        self.assertEqual(totals["requests_total"], 17)
        self.assertEqual(totals["workers"], 3)

    def test_a_silent_worker_drops_out_of_the_total(self):
        """A redeployed worker must not freeze its last figures in the sum."""
        now = 1000
        entries = [
            ("7", json.dumps({"at": now, "requests_total": 10})),
            ("9", json.dumps({"at": now - 120, "requests_total": 500})),
        ]
        totals = agg.aggregate(entries, now=now)
        self.assertEqual(totals["requests_total"], 10)
        self.assertEqual(totals["workers"], 1)

    def test_an_unreadable_entry_is_ignored(self):
        now = 1000
        totals = agg.aggregate([("7", "pas du json"), ("9", json.dumps({"at": now, "requests_total": 3}))],
                               now=now)
        self.assertEqual(totals["requests_total"], 3)

    def test_missing_counters_count_as_zero(self):
        now = 1000
        totals = agg.aggregate([("7", json.dumps({"at": now, "requests_total": 4}))], now=now)
        self.assertEqual(totals["errors_total"], 0)
        self.assertEqual(totals["auth_blocked_total"], 0)

    def test_no_worker_means_zero_not_a_crash(self):
        totals = agg.aggregate([], now=1000)
        self.assertEqual(totals["workers"], 0)
        self.assertEqual(totals["requests_total"], 0)

    def test_publish_and_read_agree(self):
        class Conn(dict):
            def hset(self, key, field, value):
                self[(key, field)] = value

            def expire(self, key, ttl):
                pass

            def hgetall(self, key):
                return {k[1]: v for k, v in self.items() if k[0] == key}

        conn = Conn()
        agg.publish(conn, 7, {"requests_total": 4, "errors_total": 1}, now=1000)
        agg.publish(conn, 9, {"requests_total": 6, "errors_total": 0}, now=1000)
        totals = agg.read_aggregate(conn, now=1000)
        self.assertEqual(totals["requests_total"], 10)
        self.assertEqual(totals["errors_total"], 1)
        self.assertEqual(totals["workers"], 2)

    def test_read_returns_none_when_redis_is_down(self):
        class Down:
            def hgetall(self, key):
                raise ConnectionError("redis is down")

        self.assertIsNone(agg.read_aggregate(Down(), now=1000))


if __name__ == "__main__":
    unittest.main()
