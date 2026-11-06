"""Tests for the destinations `/sendmessage` may publish to.

The broker is the platform's internal bus. Publishing on it used to require nothing but a valid
token and a destination string, so an ordinary account could write on the queues the workers
consume — and `/queue/REST_LOAD_DATA` exports whatever index the message names. The allowlist is
therefore a data-access control, not a convenience.
"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sources"))

from lib.amq_destinations import (  # noqa: E402
    LAMBDA_COMMAND,
    LOAD_DATA,
    PLATFORM_DESTINATIONS,
    REPORTRUNNER_COMMAND,
    is_allowed_destination,
)

USER = {"id": "alice@example.com", "privileges": ["A1"]}
ADMIN = {"id": "root@example.com", "privileges": ["admin"]}


class DestinationTest(unittest.TestCase):
    def test_platform_destinations_allowed_for_ordinary_accounts(self):
        for destination in PLATFORM_DESTINATIONS:
            self.assertTrue(is_allowed_destination(destination, USER), destination)

    def test_the_two_the_interface_really_publishes_to(self):
        # ReportEditor and LambdaEditor use these; removing them from the list breaks the product.
        self.assertIn(REPORTRUNNER_COMMAND, PLATFORM_DESTINATIONS)
        self.assertIn(LAMBDA_COMMAND, PLATFORM_DESTINATIONS)
        self.assertTrue(is_allowed_destination(REPORTRUNNER_COMMAND, USER))
        self.assertTrue(is_allowed_destination(LAMBDA_COMMAND, USER))

    def test_arbitrary_destination_refused(self):
        for destination in (
            "/queue/NYX_ADMIN",
            "/topic/homeassistant.detectors",
            "/queue/REST_LOAD_DATA_SUFFIX",
            "/queue/rest_load_data",
            "REST_LOAD_DATA",
        ):
            self.assertFalse(is_allowed_destination(destination, USER), destination)

    def test_administrators_publish_anywhere(self):
        # The administration screen offers a free-form destination: keep it, for them.
        self.assertTrue(is_allowed_destination("/queue/ANYTHING", ADMIN))
        self.assertTrue(is_allowed_destination("/topic/NYX_LAMBDA_COMMAND", ADMIN))

    def test_empty_or_missing_destination_refused(self):
        for destination in ("", "   ", None):
            self.assertFalse(is_allowed_destination(destination, USER))

    def test_whitespace_is_not_a_bypass(self):
        self.assertTrue(is_allowed_destination("  " + LOAD_DATA + "  ", USER))

    def test_user_without_privileges_is_still_limited(self):
        self.assertTrue(is_allowed_destination(LOAD_DATA, {"id": "x@example.com"}))
        self.assertFalse(is_allowed_destination("/queue/OTHER", {"id": "x@example.com"}))


if __name__ == "__main__":
    unittest.main()
