"""
Configuration bundle routes: export and import the platform design.

``GET  /config/export``  returns the design documents of the instance as one JSON bundle.
``POST /config/import``  compares a bundle with the instance and, when applied, writes it.

Import is a dry run unless ``?apply=1`` is passed: the default answer is the plan (what would be
created, updated or deleted), which is what a promotion pipeline should review before applying.
"""

import json
import logging
from datetime import datetime, timezone

from flask import request
from flask_restx import Resource, fields

import state
from middleware import token_required
from config_bundle import (
    EXPORT_INDICES,
    build_bundle,
    build_bulk_lines,
    compute_plan,
    validate_bundle,
)

logger = logging.getLogger()


def _search_all(index, size=10000):
    """All documents of an index, ignoring an index that does not exist yet."""
    body = {"size": size, "query": {"match_all": {}}}
    try:
        if state.elkversion >= 7:
            response = state.es.search(index=index, body=body)
        else:
            response = state.es.search(index=index, doc_type="doc", body=body)
    except Exception:
        logger.info("configuration bundle: index %s not readable, treated as empty", index)
        return []
    return response.get("hits", {}).get("hits", [])


def _flag(name, default=False):
    value = request.args.get(name)
    if value is None:
        return default
    return str(value).lower() in ("1", "true", "yes", "on")


def register(app, api, name_space):

    exportParams = {
        "token": "A valid admin token",
        "indices": "Optional comma separated subset of " + ",".join(EXPORT_INDICES),
    }

    @name_space.route("/config/export")
    @api.doc(description="Export the platform configuration as a bundle.", params=exportParams)
    class configExport(Resource):
        @token_required("admin")
        def get(self, user=None):
            requested = request.args.get("indices", "")
            if requested:
                wanted = [index.strip() for index in requested.split(",") if index.strip()]
                unknown = [index for index in wanted if index not in EXPORT_INDICES]
                if unknown:
                    return {"error": "UNKNOWN_INDEX", "details": unknown, "bundle": None}
                indices = wanted
            else:
                indices = list(EXPORT_INDICES)

            documents = {index: _search_all(index) for index in indices}
            bundle = build_bundle(
                documents,
                api_version=state.VERSION,
                elastic_version=state.elkversion,
                instance=request.host,
            )
            counts = {index: len(docs) for index, docs in bundle["indices"].items()}
            logger.info("configuration bundle exported: %s", counts)
            return {"error": "", "counts": counts, "bundle": bundle}

    importModel = api.model(
        "config_import_model",
        {
            "format": fields.String(description="nyx-config-bundle", required=True),
            "formatVersion": fields.Integer(description="Bundle format version", required=True),
            "indices": fields.Raw(description="{index: [{_id, _source}]}", required=True),
        },
    )

    @name_space.route("/config/import")
    @api.doc(
        description="Compare a configuration bundle with this instance and optionally apply it.",
        params={
            "token": "A valid admin token",
            "apply": "1 to write the changes; absent or 0 returns the plan only",
            "prune": "1 to delete documents that the bundle does not contain",
        },
    )
    class configImport(Resource):
        @token_required("admin")
        @api.expect(importModel)
        def post(self, user=None):
            apply_changes = _flag("apply", False)
            prune = _flag("prune", False)

            try:
                bundle = json.loads(request.data.decode("utf-8"))
            except Exception:
                return {"error": "UNABLE_TO_DECODE_BODY"}

            errors = validate_bundle(bundle)
            if errors:
                return {"error": "INVALID_BUNDLE", "details": errors}

            indices = sorted(bundle["indices"])
            current = {index: _search_all(index) for index in indices}
            plan = compute_plan(bundle, current, prune=prune)

            logger.info(
                "configuration bundle plan: %s (apply=%s, prune=%s)", plan["summary"], apply_changes, prune
            )

            if not apply_changes:
                return {
                    "error": "",
                    "applied": False,
                    "dry_run": True,
                    "prune": prune,
                    "summary": plan["summary"],
                    "changes": plan["changes"],
                }

            body, operations = build_bulk_lines(plan["changes"], bundle, state.elkversion)
            if operations == 0:
                return {
                    "error": "",
                    "applied": False,
                    "dry_run": False,
                    "prune": prune,
                    "summary": plan["summary"],
                    "changes": plan["changes"],
                }

            try:
                result = state.es.bulk(body)
            except Exception:
                logger.error("configuration bundle: bulk failed", exc_info=True)
                return {"error": "APPLY_FAILED", "details": ["bulk request rejected"]}

            failures = [
                {
                    "index": item.get("index", {}).get("_index"),
                    "id": item.get("index", {}).get("_id") or item.get("delete", {}).get("_id"),
                    "reason": (item.get("index") or item.get("delete") or {}).get("error"),
                }
                for item in result.get("items", [])
                if item.get("index", {}).get("error") or item.get("delete", {}).get("error")
            ]

            try:
                state.es.indices.refresh(index=",".join(indices))
            except Exception:
                logger.warning("configuration bundle: refresh failed, changes are still queued")

            if failures:
                logger.error("configuration bundle: %s operations failed", len(failures))
                return {"error": "APPLY_FAILED", "details": failures, "summary": plan["summary"]}

            return {
                "error": "",
                "applied": True,
                "dry_run": False,
                "prune": prune,
                "operations": operations,
                "summary": plan["summary"],
                "changes": plan["changes"],
                "appliedAt": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            }
