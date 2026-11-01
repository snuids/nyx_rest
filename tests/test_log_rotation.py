import logging
import os
import shutil
import sys
import tempfile
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sources"))

from log_setup import MultiProcessTimedRotatingFileHandler  # noqa: E402


class MultiProcessRotationTest(unittest.TestCase):
    """Three gunicorn workers share one log file; only one of them can rename it."""

    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.path = os.path.join(self.dir, "nyx_rest_api.log")
        self.addCleanup(shutil.rmtree, self.dir, True)

    def _handler(self):
        handler = MultiProcessTimedRotatingFileHandler(self.path, when="d", interval=1, backupCount=5)
        handler.setFormatter(logging.Formatter("%(message)s"))
        self.addCleanup(handler.close)
        return handler

    def _logger(self, handler, name):
        logger = logging.getLogger(name)
        logger.handlers = []
        logger.propagate = False
        logger.setLevel(logging.INFO)
        logger.addHandler(handler)
        return logger

    def _archives(self):
        return sorted(f for f in os.listdir(self.dir) if f.startswith("nyx_rest_api.log.2"))

    def _read(self, name):
        with open(os.path.join(self.dir, name), encoding="utf-8") as handle:
            return handle.read()

    def _reach_boundary(self, *handlers, seconds_back=3600):
        """Emulate midnight: the deadline has just passed and the file was last written before."""
        boundary = int(time.time()) - 1
        for handler in handlers:
            handler.rolloverAt = boundary
        when = time.time() - seconds_back
        os.utime(self.path, (when, when))

    def test_the_loser_does_not_raise(self):
        first, second = self._handler(), self._handler()
        a, b = self._logger(first, "rotation.a1"), self._logger(second, "rotation.b1")
        a.info("before-a")
        b.info("before-b")
        self._reach_boundary(first, second)

        first.doRollover()  # the worker that arrives first
        second.doRollover()  # used to raise FileNotFoundError here

    def test_the_loser_does_not_destroy_the_archive(self):
        """The standard handler deletes the archive the winner just created, then rotates an empty file over it."""
        first, second = self._handler(), self._handler()
        a, b = self._logger(first, "rotation.a2"), self._logger(second, "rotation.b2")
        a.info("before-a")
        b.info("before-b")
        self._reach_boundary(first, second)

        first.doRollover()
        archives = self._archives()
        self.assertEqual(len(archives), 1, archives)

        second.doRollover()

        self.assertEqual(self._archives(), archives, "the archive was replaced by an empty file")
        content = self._read(archives[0])
        self.assertIn("before-a", content)
        self.assertIn("before-b", content)

    def test_the_loser_writes_to_the_live_file_afterwards(self):
        first, second = self._handler(), self._handler()
        b = self._logger(second, "rotation.b3")
        b.info("before-b")
        self._reach_boundary(first, second)

        first.doRollover()
        second.doRollover()
        b.info("after-b")

        live = self._read("nyx_rest_api.log")
        self.assertIn("after-b", live, "the losing worker kept writing to the rotated file")
        self.assertNotIn("before-b", live)
        self.assertIn("before-b", self._read(self._archives()[0]))

    def test_the_loser_does_not_retry_on_every_record(self):
        first, second = self._handler(), self._handler()
        self._reach_boundary(first, second)

        first.doRollover()
        second.doRollover()

        self.assertGreater(
            second.rolloverAt,
            int(time.time()),
            "rolloverAt must move forward, otherwise every record retries the rename",
        )

    def test_an_ordinary_rollover_still_rotates(self):
        handler = self._handler()
        logger = self._logger(handler, "rotation.single")
        logger.info("first-day")
        self._reach_boundary(handler)

        handler.doRollover()
        logger.info("second-day")

        self.assertEqual(len(self._archives()), 1)
        self.assertIn("first-day", self._read(self._archives()[0]))
        self.assertIn("second-day", self._read("nyx_rest_api.log"))


if __name__ == "__main__":
    unittest.main()
