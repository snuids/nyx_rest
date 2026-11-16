"""Conditional writes: `If-Match` on `/api/v1/generic/<index>/<object>`.

Two operators editing the same record overwrote each other in silence: the route indexed
whatever the last browser had loaded, so the second save erased the first one's work with no
trace. The writer now asserts the version it read and the cluster refuses the write when the
document has moved on (`if_seq_no` / `if_primary_term`, native to Elasticsearch/OpenSearch).

The precondition is **optional**, and stays optional: the console's other editors, the
workers, the mobile clients and the third-party integrations post without it, and making the
header mandatory would have turned every one of them into a 428 overnight. A caller that
sends nothing keeps the old last-write-wins behaviour; a caller that sends the version it
read either succeeds or is told which document won, with its own content, to reload.

The header format is the ETag the GET route serves: `"<seq_no>-<primary_term>"`. A bare
`"<seq_no>"` is accepted (the primary term defaults to 1) because that is what a hand-written
curl sends and what most HTTP clients expect an ETag to look like. `*` means "the document
must exist". The same values are read from `?seq_no=`/`?primary_term=` when no header is
present: the console opens some downloads with `window.open()`, which carries no custom
header, and a proxy that strips `If-Match` must not silently turn the check off.
"""

from es_read import fetch_document

#: `If-Match: *` — the document must exist, whatever its version.
WILDCARD = "*"


class BadPrecondition(ValueError):
    """The precondition cannot be read. Guessing a version would be worse than refusing."""


def _numbers(value):
    """`12-1` or `12` -> `(12, 1)`. Anything else is a precondition we cannot honour."""
    parts = str(value).strip().split("-")
    if len(parts) > 2:
        raise BadPrecondition("expected <seq_no>[-<primary_term>], got " + str(value))
    try:
        seq_no = int(parts[0])
        primary_term = int(parts[1]) if len(parts) == 2 else 1
    except (TypeError, ValueError):
        raise BadPrecondition("not a version: " + str(value))
    if seq_no < 0 or primary_term < 1:
        raise BadPrecondition("not a version: " + str(value))
    return seq_no, primary_term


def parse_precondition(if_match=None, seq_no=None, primary_term=None):
    """Reads the precondition of a write.

    Returns `(seq_no, primary_term)`, `(WILDCARD, None)` when the document must merely
    exist, or `(None, None)` when the caller asserted nothing — the historical behaviour.
    """
    raw = (if_match or "").strip()
    if raw.startswith("W/"):            # weak validator: we can only compare the number
        raw = raw[2:].strip()
    if raw.startswith('"') and raw.endswith('"') and len(raw) >= 2:
        raw = raw[1:-1].strip()
    if raw == WILDCARD:
        return WILDCARD, None
    if raw:
        return _numbers(raw)

    # No header: the query string is the fallback the console uses for its downloads.
    if seq_no is None or str(seq_no).strip() == "":
        return None, None
    if primary_term is None or str(primary_term).strip() == "":
        primary_term = 1
    try:
        seq_no, primary_term = int(seq_no), int(primary_term)
    except (TypeError, ValueError):
        raise BadPrecondition("not a version: " + str(seq_no) + "-" + str(primary_term))
    if seq_no < 0 or primary_term < 1:
        raise BadPrecondition("not a version: " + str(seq_no) + "-" + str(primary_term))
    return seq_no, primary_term


def write_condition(precondition):
    """The keyword arguments `es.index()` / `es.delete()` need, empty when unasserted."""
    seq_no, primary_term = precondition
    if seq_no is None or seq_no == WILDCARD:
        return {}
    return {"if_seq_no": seq_no, "if_primary_term": primary_term}


def etag_for(seq_no, primary_term):
    """The `ETag` a GET hands out, and the `If-Match` a writer sends back."""
    return '"%s-%s"' % (seq_no, primary_term)


def is_conflict(exc):
    """True for a failed precondition, and not for an outage.

    `opensearch-py` raises `ConflictError` (409) when `if_seq_no` no longer matches; the
    name is checked as well because the clients do not all expose the same class.
    """
    return (getattr(exc, "status_code", None) == 409
            or type(exc).__name__ == "ConflictError")


def conflict_body(es, index, document_id):
    """What a refused write answers: the document that won, so the caller can decide.

    The password hash is stripped whatever the index: an error body is copied into bug
    reports and browser consoles, and a hash is never the answer to "who changed this row".
    """
    current = fetch_document(es, index, document_id)[1]

    body = {"error": "conflict", "data": None, "seq_no": None, "primary_term": None}
    if not current:
        return body, None
    source = dict(current.get("_source") or {})
    if "password" in source:
        source["password"] = ""
    body["data"] = dict(current)
    body["data"]["_source"] = source
    body["seq_no"] = current.get("_seq_no")
    body["primary_term"] = current.get("_primary_term", 1)
    if body["seq_no"] is None:
        return body, None
    return body, etag_for(body["seq_no"], body["primary_term"])
