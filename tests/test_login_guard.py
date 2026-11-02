"""Tests for the login guard: no dependency on Redis, nor on Flask."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sources"))

import login_guard as guard


class FakeRedis:
    """Strictly what the guard needs from the Redis API, with a controllable clock."""

    def __init__(self):
        self.values = {}
        self.clock = 1_000.0

    def _expired(self, key):
        deadline = self.values.get(key, (None, None))[1]
        return deadline is not None and self.clock >= deadline

    def get(self, key):
        if key not in self.values or self._expired(key):
            self.values.pop(key, None)
            return None
        return self.values[key][0]

    def setex(self, key, seconds, value):
        self.values[key] = (value, self.clock + seconds)
        return True

    def incr(self, key):
        current = int(self.get(key) or 0) + 1
        deadline = self.values.get(key, (None, None))[1]
        self.values[key] = (current, deadline)
        return current

    def expire(self, key, seconds):
        if key in self.values:
            self.values[key] = (self.values[key][0], self.clock + seconds)
        return True

    def ttl(self, key):
        if key not in self.values or self._expired(key):
            self.values.pop(key, None)
            return -2
        return int(self.values[key][1] - self.clock)

    def delete(self, *keys):
        for key in keys:
            self.values.pop(key, None)
        return len(keys)

    def advance(self, seconds):
        self.clock += seconds


class BrokenRedis:
    """A Redis that goes down: the guard must let through, not close the door."""

    def __getattr__(self, name):
        def boom(*args, **kwargs):
            raise ConnectionError("redis is down")
        return boom


class GuardTest(unittest.TestCase):
    def setUp(self):
        self.redis = FakeRedis()

    def fail_times(self, count, login="amarchand@icloud.com", ip="203.0.113.9"):
        last = 0
        for _ in range(count):
            last = guard.register_failure(self.redis, login, ip)
        return last

    def test_a_handful_of_attempts_is_not_punished(self):
        for expected in range(guard.MAX_ATTEMPTS - 1):
            wait = guard.register_failure(self.redis, "amarchand@icloud.com", "203.0.113.9")
            self.assertEqual(wait, 0, f"gel prématuré après {expected + 1} échec(s)")

    def test_the_first_freeze_arrives_at_the_threshold(self):
        wait = self.fail_times(guard.MAX_ATTEMPTS)
        self.assertGreater(wait, 0)
        self.assertLessEqual(wait, guard.WINDOW)

    def test_the_freeze_lifts_when_the_window_expires(self):
        self.fail_times(guard.MAX_ATTEMPTS)
        self.assertGreater(guard.retry_after(self.redis, "amarchand@icloud.com", "203.0.113.9"), 0)
        self.redis.advance(guard.WINDOW + 1)
        self.assertEqual(guard.retry_after(self.redis, "amarchand@icloud.com", "203.0.113.9"), 0)

    def test_ten_failures_freeze_the_account_for_five_minutes(self):
        self.fail_times(10)
        self.assertEqual(guard.retry_after(self.redis, "amarchand@icloud.com", "203.0.113.9"), 300)

    def test_twenty_failures_freeze_the_account_for_an_hour(self):
        self.fail_times(20)
        self.assertEqual(guard.retry_after(self.redis, "amarchand@icloud.com", "203.0.113.9"), 3600)

    def test_a_frozen_account_stays_frozen_after_the_counter_expires(self):
        self.fail_times(20)
        self.redis.advance(guard.WINDOW + 1)   # the window falls back…
        remaining = guard.retry_after(self.redis, "amarchand@icloud.com", "203.0.113.9")
        self.assertGreater(remaining, 3000, "le gel d'une heure doit survivre à la fenêtre de 5 min")
        self.assertLessEqual(remaining, 3600)

    def test_a_success_wipes_the_counters(self):
        self.fail_times(guard.MAX_ATTEMPTS)
        guard.reset(self.redis, "amarchand@icloud.com", "203.0.113.9")
        self.assertEqual(guard.retry_after(self.redis, "amarchand@icloud.com", "203.0.113.9"), 0)

    def test_the_ip_counter_catches_username_spraying(self):
        """Changing login on every attempt must not make the attack free."""
        for attempt in range(guard.MAX_ATTEMPTS):
            guard.register_failure(self.redis, f"victime{attempt}@icloud.com", "198.51.100.7")
        wait = guard.retry_after(self.redis, "encore-un-autre@icloud.com", "198.51.100.7")
        self.assertGreater(wait, 0)
        # … while another source is not penalised
        self.assertEqual(guard.retry_after(self.redis, "voisin@icloud.com", "198.51.100.8"), 0)

    def test_an_unknown_login_is_counted_too(self):
        """Otherwise the attacker enumerates the existing accounts for free."""
        self.fail_times(guard.MAX_ATTEMPTS, login="inconnu@icloud.com")
        self.assertGreater(guard.retry_after(self.redis, "inconnu@icloud.com", "203.0.113.9"), 0)

    def test_logins_are_case_insensitive(self):
        self.fail_times(guard.MAX_ATTEMPTS, login="Amarchand@iCloud.com")
        self.assertGreater(guard.retry_after(self.redis, "amarchand@icloud.com", "203.0.113.9"), 0)

    def test_redis_down_means_open_door_not_locked_out(self):
        self.assertEqual(guard.retry_after(BrokenRedis(), "amarchand@icloud.com", "203.0.113.9"), 0)
        self.assertEqual(guard.register_failure(BrokenRedis(), "a@b.c", "203.0.113.9"), 0)
        guard.reset(BrokenRedis(), "a@b.c", "203.0.113.9")   # does not raise

    def test_client_ip_prefers_the_real_ip_header(self):
        self.assertEqual(
            guard.client_ip({"X-Real-IP": "203.0.113.9", "X-Forwarded-For": "10.0.0.1, 203.0.113.9"}, "172.18.0.5"),
            "203.0.113.9",
        )
        self.assertEqual(guard.client_ip({"X-Forwarded-For": "203.0.113.9, 10.0.0.1"}, "172.18.0.5"), "203.0.113.9")
        self.assertEqual(guard.client_ip({}, "172.18.0.5"), "172.18.0.5")
        self.assertEqual(guard.client_ip({}, None), "unknown")

    def test_the_free_attempt_count_is_the_documented_one(self):
        self.assertEqual(guard.MAX_ATTEMPTS, 5)
        self.assertEqual(guard.lock_seconds(4), 0)
        self.assertGreater(guard.lock_seconds(5), 0)

    def test_a_failure_costs_a_visible_floor_of_time(self):
        """The response floor prevents telling an existing account from an unknown one."""
        import time
        start = time.monotonic()
        guard.delay()
        elapsed = time.monotonic() - start
        self.assertGreaterEqual(elapsed, guard.MIN_FAILURE_DELAY)

    def test_both_failure_cases_share_one_message(self):
        """An unknown account and a wrong password must be indistinguishable."""
        self.assertEqual(guard.failure_message(), "Bad Credentials")


if __name__ == "__main__":
    unittest.main()
