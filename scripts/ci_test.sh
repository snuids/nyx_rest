#!/bin/sh
# Runs the nyx_rest test suite under coverage, inside the image the CI has just built.
# POSIX sh: the base image is python:3.11-alpine, without bash.
#
# Two lies this script stops:
#   * `unittest` exits 0 even when it skips tests, and the route tests skip themselves when a
#     dependency is missing — a missing import used to disable a third of the suite in silence;
#   * nothing measured how much code the tests actually exercised.
#
# The coverage floor is the level actually measured, never a wish: 43 % on 2026-11-02, then 48 %
# on 2026-11-06 once `tests/test_routes_access.py` covered the publication and index-rule
# boundaries — the two places where a routing mistake is a data leak — then 53 % on 2026-11-08
# once `tests/test_kibana_export.py` held the dashboard export. Raise it as tests land, never
# lower it.
set -eu

FLOOR=53

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

# The percentage, alone, for the badge the README points at. Written where the workflow can
# read it: the repository is mounted at /work.
python -m coverage report --format=total > .coverage-total