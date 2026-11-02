"""
Liveness and metrics.

``GET /api/v1/healthz`` answers without a token: a probe cannot hold one, and a token expires.
``GET /api/v1/metrics`` exposes a small Prometheus text document (no extra dependency).

Both are unauthenticated on purpose, so they must stay free of anything sensitive: a version, a
status word, and counters. No hostname, no credential, no user.
"""

import json
import logging
import os

from flask import Response
from flask_restx import Resource
from datetime import datetime, timezone

import state
import metrics_aggregate

logger = logging.getLogger()

# A broker that has not published a life sign for this long is considered stale. The API
# publishes one every 5 seconds (see es_helpers.handleAPICalls).
LIFESIGN_MAX_AGE_SECONDS = 30


def _check_elasticsearch():
    try:
        info = state.es.info(request_timeout=3)
        version = (info or {}).get("version", {}).get("number", "")
        return True, {"version": version}
    except Exception as error:
        logger.warning("healthz: elasticsearch unreachable (%s)", type(error).__name__)
        return False, {"error": type(error).__name__}


def _check_redis():
    try:
        state.redisserver.ping()
        return True, {}
    except Exception as error:
        logger.warning("healthz: redis unreachable (%s)", type(error).__name__)
        return False, {"error": type(error).__name__}


def _check_broker():
    if state.conn is None:
        return False, {"error": "not configured"}
    if state.last_lifesign is None:
        return False, {"error": "no life sign yet"}
    age = (datetime.now() - state.last_lifesign).total_seconds()
    return age < LIFESIGN_MAX_AGE_SECONDS, {"lastLifeSignSeconds": round(age, 1)}


def collect_health():
    """Return the health payload and the HTTP status that goes with it."""
    checks = {}
    for name, check in (
        ("elasticsearch", _check_elasticsearch),
        ("redis", _check_redis),
        ("broker", _check_broker),
    ):
        up, detail = check()
        checks[name] = {"status": "up" if up else "down", **detail}

    healthy = all(check["status"] == "up" for check in checks.values())
    payload = {
        "status": "ok" if healthy else "degraded",
        "version": state.VERSION,
        "elasticVersion": state.elkversion,
        "checks": checks,
        "time": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
    }
    return payload, (200 if healthy else 503)


def _metric(lines, name, value, help_text=None, metric_type="gauge", labels=None):
    if help_text:
        lines.append(f"# HELP {name} {help_text}")
        lines.append(f"# TYPE {name} {metric_type}")
    if labels:
        rendered = ",".join(
            f'{key}="{str(val).replace(chr(92), chr(92) * 2).replace(chr(34), chr(92) + chr(34))}"'
            for key, val in labels.items()
        )
        lines.append(f"{name}{{{rendered}}} {value}")
    else:
        lines.append(f"{name} {value}")


def render_metrics():
    """Prometheus text exposition, version 0.0.4."""
    es_up, _ = _check_elasticsearch()
    redis_up, _ = _check_redis()
    broker_up, broker_detail = _check_broker()

    lines = []
    _metric(lines, "nyx_up", 1, "Always 1 when the API answers this endpoint")
    _metric(
        lines,
        "nyx_info",
        1,
        "Version of the API and of the cluster it talks to",
        labels={"version": state.VERSION, "elastic_version": state.elkversion},
    )
    _metric(lines, "nyx_elasticsearch_up", int(es_up), "1 when Elasticsearch answers")
    _metric(lines, "nyx_redis_up", int(redis_up), "1 when Redis answers")
    _metric(lines, "nyx_broker_up", int(broker_up), "1 when the broker life sign is fresh")
    if "lastLifeSignSeconds" in broker_detail:
        _metric(
            lines,
            "nyx_broker_life_sign_age_seconds",
            broker_detail["lastLifeSignSeconds"],
            "Age of the last life sign published on the broker",
        )
    # The counters live in the process: three gunicorn workers, hence three values. Each
    # worker publishes its own into Redis, and we expose the sum — without a label, a single
    # accurate series. A label per worker was not enough: Prometheus reuses its HTTP
    # connection and never queries anything but the same worker, so it saw only a third of
    # the traffic.
    # Fallback on the local counters if Redis does not answer: a partial value is better
    # than a missing metric.
    totals = metrics_aggregate.read_aggregate(state.redisserver) or {}
    local = metrics_aggregate.snapshot(state)
    _metric(lines, "nyx_requests_total", totals.get("requests_total", local["requests_total"]),
            "Authenticated calls served", "counter")
    _metric(lines, "nyx_errors_total", totals.get("errors_total", local["errors_total"]),
            "Calls answered with an error", "counter")
    _metric(lines, "nyx_active_tokens", totals.get("active_tokens", local["active_tokens"]),
            "Tokens currently cached")
    # Two distinct counters: a wrong password (legitimate attempt or attack) and an attempt
    # turned away by the guard (attack in progress). Confusing them would make both signals
    # unreadable.
    _metric(lines, "nyx_auth_failures_total", totals.get("auth_failures_total", local["auth_failures_total"]),
            "Rejected credentials", "counter")
    _metric(lines, "nyx_auth_blocked_total", totals.get("auth_blocked_total", local["auth_blocked_total"]),
            "Login attempts refused by the brute-force guard", "counter")
    _metric(lines, "nyx_counters_workers", totals.get("workers", 0),
            "Workers whose counters are included in this scrape")

    return "\n".join(lines) + "\n"


def register(app, api, name_space):

    @name_space.route("/healthz")
    @api.doc(description="Liveness probe, without a token. Answers 503 when a dependency is down.")
    class healthz(Resource):
        def get(self):
            payload, status = collect_health()
            response = Response(json.dumps(payload), status=status, mimetype="application/json")
            response.headers["Cache-Control"] = "no-store"
            return response

    @name_space.route("/metrics")
    @api.doc(description="Prometheus metrics for this API worker.")
    class metrics(Resource):
        def get(self):
            return Response(
                render_metrics(),
                mimetype="text/plain; version=0.0.4; charset=utf-8",
                headers={"Cache-Control": "no-store"},
            )
