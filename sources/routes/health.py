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
    # Les compteurs vivent dans le processus : trois workers gunicorn, donc trois valeurs
    # différentes. Sans label, chaque scrape tombe sur un worker au hasard et la série fait
    # des dents de scie (34, 28, 31, 39…) que Prometheus interprète comme des remises à zéro :
    # rate() et increase() renvoient n'importe quoi — c'est ce qui a fait sonner l'alerte de
    # taux d'erreur à 6 % pour zéro erreur réelle le 01/11. Le label worker rend chaque série
    # monotone, et sum(rate(...)) au niveau de la règle redevient juste.
    worker = {"worker": str(os.getpid())}
    _metric(lines, "nyx_requests_total", state.requests_total, "Authenticated calls served by this worker", "counter", labels=worker)
    _metric(lines, "nyx_errors_total", state.errors_total, "Calls answered with an error", "counter", labels=worker)
    _metric(lines, "nyx_active_tokens", len(state.tokens), "Tokens currently cached by this worker", labels=worker)
    # Deux compteurs distincts : un mot de passe faux (tentative légitime ou attaque) et une
    # tentative écartée par le garde (attaque en cours). Les confondre rendrait les deux
    # signaux illisibles.
    _metric(lines, "nyx_auth_failures_total", state.auth_failures_total, "Rejected credentials", "counter", labels=worker)
    _metric(lines, "nyx_auth_blocked_total", state.auth_blocked_total, "Login attempts refused by the brute-force guard", "counter", labels=worker)

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
