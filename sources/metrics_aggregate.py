"""Aggregation of the counters across gunicorn workers.

Each worker holds its counters in memory: ``state.requests_total`` is three different things
depending on which one answers the scrape. Prometheus reuses its HTTP connection, so it always
queries the same worker and sees only a third of the traffic.

Each worker therefore publishes its counters into a Redis hash (``HSET nyx_counters <pid>``), and
``/metrics`` adds up what every worker published recently. A dead worker disappears from the sum
after ``STALE_AFTER`` seconds: without that, a redeployment would leave its last figures frozen in
the total forever.

The aggregation is a pure function, testable without Redis.
"""

import json
import time

HASH_KEY = "nyx_counters"
STALE_AFTER = 30        # a worker silent for longer than this is considered dead
PUBLISH_TTL = 120       # the whole hash expires if nobody publishes any more

COUNTERS = (
    "requests_total",
    "errors_total",
    "auth_failures_total",
    "auth_blocked_total",
    "active_tokens",
)


def snapshot(state):
    """Values to publish for this worker."""
    return {name: int(getattr(state, name, 0) or 0) for name in COUNTERS}


def publish(conn, pid, counters, now=None):
    """Writes this worker's state into Redis. Silent on failure: the metric is not a
    vital function of the API."""
    try:
        payload = {"at": int(now if now is not None else time.time())}
        payload.update({name: int(value) for name, value in counters.items()})
        conn.hset(HASH_KEY, str(pid), json.dumps(payload))
        conn.expire(HASH_KEY, PUBLISH_TTL)
    except Exception:
        pass


def aggregate(entries, now=None, stale_after=STALE_AFTER):
    """Sums the counters of the live workers.

    ``entries``: list of ``(field, raw value)`` pairs as returned by ``HGETALL``, or of
    dictionaries already decoded. Entries too old or unreadable are ignored.
    """
    now = int(now if now is not None else time.time())
    totals = {name: 0 for name in COUNTERS}
    workers = 0
    for entry in entries or []:
        payload = entry
        if isinstance(entry, (list, tuple)) and len(entry) == 2:
            try:
                payload = json.loads(entry[1])
            except (TypeError, ValueError):
                continue
        if not isinstance(payload, dict):
            continue
        published_at = int(payload.get("at") or 0)
        if now - published_at > stale_after:
            continue          # dead worker: its figures must not freeze in the total
        workers += 1
        for name in COUNTERS:
            try:
                totals[name] += int(payload.get(name) or 0)
            except (TypeError, ValueError):
                pass
    totals["workers"] = workers
    return totals


def read_aggregate(conn, now=None, stale_after=STALE_AFTER):
    """Reads the Redis hash then aggregates. ``None`` if Redis is unreachable."""
    try:
        raw = conn.hgetall(HASH_KEY)
    except Exception:
        return None
    if not raw:
        return None
    entries = []
    for key, value in raw.items():
        entries.append((key, value))
    totals = aggregate(entries, now=now, stale_after=stale_after)
    if totals["workers"] == 0:
        return None
    return totals
