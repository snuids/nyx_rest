import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sources"))

from auth.double_phase import verify_double_code


class FakeRedis:
    def __init__(self, codes=None):
        self.codes = codes or {}

    def get(self, key):
        return self.codes.get(key)

    def delete(self, key):
        self.codes.pop(key, None)


class DoublePhaseTests(unittest.TestCase):
    def test_missing_or_expired_code_cannot_be_submitted_as_none(self):
        redisserver = FakeRedis()
        self.assertFalse(verify_double_code(redisserver, "alice", "None"))
        self.assertFalse(verify_double_code(redisserver, "alice", None))

    def test_correct_code_is_single_use(self):
        redisserver = FakeRedis({"nyx_double_alice": b"12345"})
        self.assertTrue(verify_double_code(redisserver, "alice", "12345"))
        self.assertFalse(verify_double_code(redisserver, "alice", "12345"))

    def test_wrong_code_is_rejected_and_invalidated(self):
        redisserver = FakeRedis({"nyx_double_alice": b"12345"})
        self.assertFalse(verify_double_code(redisserver, "alice", "99999"))
        self.assertFalse(verify_double_code(redisserver, "alice", "12345"))


if __name__ == "__main__":
    unittest.main()
