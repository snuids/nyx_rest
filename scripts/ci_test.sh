#!/bin/sh
# Runs the nyx_rest test suite under coverage, inside the image the CI has just built.
# POSIX sh: the base image is python:3.11-alpine, without bash.
#
# Two lies this script stops:
#   * `unittest` exits 0 even when it skips tests, and the route tests skip themselves when a
#     dependency is missing — a missing import used to disable a third of the suite in silence;
#   * nothing measured how much code the tests actually exercised.
#
# The coverage floor is the level reached on 2026-11-02 (43 % measured). Raise it as tests
# land, never lower it.
set -eu

FLOOR=40

python -m pip install --quiet -r sources/requirements-test.txt
python -m coverage erase

# No pipeline here: we want unittest's exit status, not tee's.
status=0
python -m coverage run --source=sources -m unittest discover -s tests > /tmp/suite.log 2>&1 || status=$?
cat /tmp/suite.log

if grep -q 'skipped=' /tmp/suite.log; then
    echo '::error::Tests were skipped although the dependencies are present: this is a regression, not an incomplete environment.'
    exit 1
fi

if [ "$status" -ne 0 ]; then
    exit "$status"
fi

python -m coverage report --fail-under="$FLOOR"
