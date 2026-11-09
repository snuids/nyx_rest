"""
Configuration bundle: serialise the platform design of an instance into one JSON document and
plan its import on another instance.

"Platform design" is everything that is configuration rather than data or secrets:

* ``nyx_app``          the screens (menus, columns, query filters, dashboards)
* ``nyx_indice``       the access rules applied to data indices
* ``nyx_privilege``    the privilege catalogue
* ``nyx_filter``       the filter catalogue
* ``nyx_translation``  the menu/UI translations
* ``nyx_datasource``   the named queries used by Vega graphs, forms and exports

Deliberately excluded, because they are per-instance state or carry secrets:
``nyx_user`` (password hashes), ``nyx_apikey``, ``nyx_config`` (instance stylesheet and password
rules), ``nyx_reportdef`` / ``nyx_reportperiodic`` / ``nyx_reporttask`` (may embed credentials and
paths), ``nyx_lambda``, and every runtime index (``nyx_apicalls-*``, ``nyx_module_info-*``, …).

This module is pure Python on purpose: the routes and the tests import it without Flask, the
Elasticsearch client or a live cluster.
"""

import json
from datetime import datetime, timezone

BUNDLE_FORMAT = "nyx-config-bundle"
BUNDLE_VERSION = 1

EXPORT_INDICES = (
    "nyx_app",
    "nyx_indice",
    "nyx_privilege",
    "nyx_filter",
    "nyx_translation",
    "nyx_datasource",
)

FORBIDDEN_INDICES = (
    "nyx_user",
    "nyx_apikey",
    "nyx_config",
    "nyx_reportdef",
    "nyx_reportperiodic",
    "nyx_reporttask",
    "nyx_lambda",
    "nyx_lambdalog",
)

# Fields that a screen may carry at read time but that are not part of its definition.
_VOLATILE_FIELDS = ("rec_id", "_id", "_index", "_type", "_score")


def clean_source(source):
    """Return the document without the fields the API adds when it hands it out."""
    if not isinstance(source, dict):
        return {}
    return {k: v for k, v in source.items() if k not in _VOLATILE_FIELDS}


def canonical(document):
    """Stable string form of a document, used to detect real changes."""
    return json.dumps(clean_source(document), sort_keys=True, default=str)


def build_bundle(documents_by_index, api_version="", elastic_version=None, instance=""):
    """Assemble the bundle from ``{index: [{"_id": ..., "_source": {...}}, ...]}``."""
    indices = {}
    for index in sorted(documents_by_index):
        documents = []
        for hit in documents_by_index[index]:
            document_id = hit.get("_id")
            if document_id is None:
                continue
            documents.append(
                {"_id": str(document_id), "_source": clean_source(hit.get("_source", {}))}
            )
        documents.sort(key=lambda d: d["_id"])
        indices[index] = documents

    return {
        "format": BUNDLE_FORMAT,
        "formatVersion": BUNDLE_VERSION,
        "platform": {
            "apiVersion": api_version,
            "elasticVersion": elastic_version,
            "instance": instance,
            "generatedAt": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        },
        "indices": indices,
    }


def validate_bundle(bundle):
    """Return the list of problems that make a bundle unusable (empty list == valid)."""
    errors = []
    if not isinstance(bundle, dict):
        return ["bundle is not a JSON object"]
    if bundle.get("format") != BUNDLE_FORMAT:
        errors.append(f"unsupported format {bundle.get('format')!r}, expected {BUNDLE_FORMAT!r}")
    version = bundle.get("formatVersion")
    if version is None:
        errors.append("formatVersion is missing")
    elif not isinstance(version, int) or version < 1:
        errors.append(f"unsupported formatVersion {version!r}")
    elif version > BUNDLE_VERSION:
        errors.append(
            f"formatVersion {version} is newer than this API ({BUNDLE_VERSION}), refusing to import"
        )

    indices = bundle.get("indices")
    if not isinstance(indices, dict) or not indices:
        errors.append("indices must be a non-empty object")
        return errors

    for index, documents in indices.items():
        if index in FORBIDDEN_INDICES:
            errors.append(f"{index} is not portable (secrets or instance state)")
            continue
        if index not in EXPORT_INDICES:
            errors.append(f"{index} is not part of the configuration bundle")
            continue
        if not isinstance(documents, list):
            errors.append(f"{index}: documents must be a list")
            continue
        seen = set()
        for position, document in enumerate(documents):
            if not isinstance(document, dict):
                errors.append(f"{index}[{position}] is not an object")
                continue
            document_id = document.get("_id")
            if not isinstance(document_id, str) or document_id == "":
                errors.append(f"{index}[{position}] has no valid _id")
                continue
            if document_id in seen:
                errors.append(f"{index}: duplicate _id {document_id!r}")
            seen.add(document_id)
            if not isinstance(document.get("_source"), dict):
                errors.append(f"{index}/{document_id}: _source must be an object")
    return errors


def compute_plan(bundle, current_by_index, prune=False):
    """
    Compare a bundle with what the target instance holds.

    ``current_by_index`` is ``{index: [{"_id": ..., "_source": {...}}, ...]}`` for the indices the
    bundle touches. Returns ``{"changes": [...], "summary": {...}}`` where each change is
    ``{index, id, action, fieldsChanged}`` and action is one of ``create``, ``update``,
    ``unchanged``, ``delete``.
    """
    changes = []

    for index in sorted(bundle.get("indices", {})):
        wanted = {document["_id"]: document for document in bundle["indices"][index]}
        existing = {
            str(hit.get("_id")): hit for hit in current_by_index.get(index, []) if hit.get("_id") is not None
        }

        for document_id in sorted(wanted):
            wanted_source = clean_source(wanted[document_id].get("_source", {}))
            if document_id not in existing:
                changes.append(
                    {
                        "index": index,
                        "id": document_id,
                        "action": "create",
                        "fieldsChanged": sorted(wanted_source.keys()),
                    }
                )
                continue

            existing_source = clean_source(existing[document_id].get("_source", {}))
            if canonical(existing_source) == canonical(wanted_source):
                changes.append(
                    {"index": index, "id": document_id, "action": "unchanged", "fieldsChanged": []}
                )
                continue

            changed_fields = sorted(
                field
                for field in set(existing_source) | set(wanted_source)
                if canonical({field: existing_source.get(field)})
                != canonical({field: wanted_source.get(field)})
            )
            changes.append(
                {
                    "index": index,
                    "id": document_id,
                    "action": "update",
                    "fieldsChanged": changed_fields,
                }
            )

        if prune:
            for document_id in sorted(set(existing) - set(wanted)):
                changes.append(
                    {"index": index, "id": document_id, "action": "delete", "fieldsChanged": []}
                )

    return {"changes": changes, "summary": summarise(changes)}


def summarise(changes):
    summary = {"create": 0, "update": 0, "unchanged": 0, "delete": 0}
    for change in changes:
        action = change.get("action")
        if action in summary:
            summary[action] += 1
    summary["total"] = len(changes)
    summary["pending"] = summary["create"] + summary["update"] + summary["delete"]
    return summary


def build_bulk_lines(changes, bundle):
    """
    Turn the writable part of a plan into an Elasticsearch bulk payload.

    Returns ``(body, operations)`` where ``body`` is a newline-terminated string and
    ``operations`` the number of index/delete operations it contains.
    """
    documents = {
        (index, document["_id"]): document["_source"]
        for index, entries in bundle.get("indices", {}).items()
        for document in entries
    }

    lines = []
    operations = 0
    for change in changes:
        action = change.get("action")
        if action not in ("create", "update", "delete"):
            continue
        meta = {"_index": change["index"], "_id": change["id"]}

        if action == "delete":
            lines.append(json.dumps({"delete": meta}))
            operations += 1
            continue

        source = documents.get((change["index"], change["id"]))
        if source is None:
            continue
        lines.append(json.dumps({"index": meta}))
        lines.append(json.dumps(source))
        operations += 1

    if not lines:
        return "", 0
    return "\n".join(lines) + "\n", operations


def describe_plan(plan):
    """Human-readable rendering of a plan, used by the CLI."""
    lines = []
    for change in plan.get("changes", []):
        if change["action"] == "unchanged":
            continue
        suffix = ""
        if change.get("fieldsChanged"):
            fields = ", ".join(change["fieldsChanged"][:6])
            more = "" if len(change["fieldsChanged"]) <= 6 else ", …"
            suffix = f"  ({fields}{more})"
        lines.append(f"{change['action']:<9} {change['index']:<18} {change['id']}{suffix}")
    if not lines:
        lines.append("nothing to do: the target already matches the bundle")
    return "\n".join(lines)
