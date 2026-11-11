"""The version the API announces is the version of the release.

`/api/v1/config` answers `state.VERSION`, and the container image is tagged from the version written
in the merge commit. The two are written by hand, in two files, at different moments — and on
11 November 2026 they went out of step: the image `v3.22.30` was built and deployed announcing
`3.22.29`, because the changelog entry was added and the constant forgotten. Nothing failed; an
operator would just have been told the wrong build was running.

This test reads both, without importing the API module (which starts a cluster client at import
time): the constant from `state.py`, and the last dated line of the changelog in
`nyx_rest_api_plus.py`.
"""

import os
import re
import unittest


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STATE = os.path.join(ROOT, "sources", "state.py")
CHANGELOG = os.path.join(ROOT, "sources", "nyx_rest_api_plus.py")

VERSION_PATTERN = r"^[0-9]+\.[0-9]+\.[0-9]+$"


def read(path):
    with open(path, encoding="utf-8") as handle:
        return handle.read()


def announced_version():
    """`VERSION` as `state.py` declares it."""
    match = re.search(r'VERSION\s*=\s*"([^"]+)"', read(STATE))
    if not match:
        raise AssertionError("state.py declares no VERSION")
    return match.group(1)


def last_changelog_version():
    """The version of the last dated entry in the changelog."""
    entries = re.findall(r"^v([0-9]+\.[0-9]+\.[0-9]+)\s+\d{2}/\w{3}/\d{4}", read(CHANGELOG), re.M)
    if not entries:
        raise AssertionError("the changelog in nyx_rest_api_plus.py has no dated entry")
    return entries[-1]


class VersionTests(unittest.TestCase):
    def test_the_announced_version_is_the_last_changelog_entry(self):
        self.assertEqual(
            announced_version(),
            last_changelog_version(),
            "the changelog's last entry and state.VERSION disagree: bump both, or neither",
        )

    def test_the_version_looks_like_a_release(self):
        self.assertRegex(announced_version(), VERSION_PATTERN)


if __name__ == "__main__":
    unittest.main()
