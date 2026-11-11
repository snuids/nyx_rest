"""
Report submission: /report/generate and /report/regenerate.

Before this module the console built the whole report job itself and handed it to `/sendmessage`:
the definition, the privileges, and **the user's own credentials**, token included. `/sendmessage`
publishes on the platform's internal bus, so that was an open door — which is why it was closed by
an allow-list that only administrators may go through. The consequence was the other way round: an
account holding `reporteditor` saw the Generate button, filled the dialog, and got
`403 destination not allowed` with nothing on screen to say so.

These two routes are the door that was missing. The console sends **an identifier and parameter
values**, nothing else:

    POST /api/v1/report/generate     {"report": "<nyx_reportdef id>",
                                      "parameters": [{"name": "...", "value": ...}],
                                      "task": {"mailingList": [...], "mailSubject": "...",
                                               "mailTemplate": "..."}}
    POST /api/v1/report/regenerate   {"task": "<reporttask id>", "index": "nyx_reporttask-2026.11"}

and the server rebuilds the job from what it trusts: the definition as it is **stored** (not as the
caller sent it, so `exec` and `jasper` paths cannot be redirected), the session as it is
**authenticated** (not as the caller claimed), and an identifier it generates. Regenerating re-runs
the definition snapshot that the task already carries, so the parameters of the previous run are
kept; only the requester (or an administrator) may do it.

Submission stays unaffected by the destination allow-list: `/sendmessage` remains closed to
non-administrators, and this module publishes `/queue/NYX_REPORT_STEP1` on its own authority, after
its own checks.
"""

import json
import logging
import uuid

from flask import request
from flask_restx import Resource, fields

import state
from middleware import token_required

logger = logging.getLogger()

REPORTDEF_INDEX = "nyx_reportdef"
TASK_INDEX_PATTERN = "nyx_reporttask-*"
STEP1 = "/queue/NYX_REPORT_STEP1"

# What the flow may carry in `task`; anything else the caller sends is dropped.
TASK_LIST_FIELDS = ("mailingList", "mailingListCc", "mailingListCci")
TASK_TEXT_FIELDS = ("mailSubject", "mailTemplate")
MAX_RECIPIENTS = 200
MAX_TEXT = 20000


def _is_admin(user):
    return "admin" in ((user or {}).get("privileges") or [])


def may_run(user, definition):
    """An empty `privileges` list means everyone; otherwise one match is enough."""
    required = (definition or {}).get("privileges") or []
    if not required:
        return True
    return _is_admin(user) or any(priv in required for priv in (user.get("privileges") or []))


def sanitise_task(task):
    """Keep the mail fields the flow understands, with sane types and bounds."""
    task = task if isinstance(task, dict) else {}
    cleaned = {}
    for name in TASK_LIST_FIELDS:
        value = task.get(name) or []
        if not isinstance(value, list):
            value = []
        cleaned[name] = [str(item)[:320] for item in value][:MAX_RECIPIENTS]
    for name in TASK_TEXT_FIELDS:
        cleaned[name] = str(task.get(name) or "")[:MAX_TEXT]
    return cleaned


def merge_parameters(definition, submitted):
    """Apply the submitted *values* onto the stored definition.

    Returns `(parameters, error)`. Only the values cross the boundary: a name that is not in the
    definition is refused rather than added, so a caller cannot inject a parameter the report author
    never defined.
    """
    stored = []
    for param in (definition or {}).get("parameters") or []:
        stored.append(dict(param))

    if submitted is None:
        return stored, ""

    if not isinstance(submitted, list):
        return stored, "parameters must be a list"

    by_name = {}
    for item in submitted:
        if not isinstance(item, dict) or "name" not in item:
            return stored, "each parameter needs a name"
        by_name[str(item["name"])] = item.get("value")

    known = {param.get("name") for param in stored}
    unknown = sorted(name for name in by_name if name not in known)
    if unknown:
        return stored, "unknown parameter(s): " + ", ".join(unknown)

    for param in stored:
        if param.get("name") in by_name:
            param["value"] = by_name[param["name"]]
    return stored, ""


def build_job(report_id, token, user, report, task):
    """The job handed to Node-RED: identifier, session, definition, mail settings."""
    return {
        "id": report_id,
        "creds": {"token": token, "user": user},
        "report": report,
        "privileges": (user or {}).get("privileges") or [],
        "task": sanitise_task(task),
    }


def new_report_id():
    return "id_" + str(uuid.uuid4())


def load_definition(report_id):
    """The stored definition, or `None` when the index has no such document."""
    try:
        hit = state.es.get(index=REPORTDEF_INDEX, id=report_id)
    except Exception:
        return None
    return hit.get("_source") if isinstance(hit, dict) else None


def load_task(task_id, index=None):
    """The stored task document and its index, or `(None, None)`."""
    body = {"query": {"ids": {"values": [str(task_id)]}}, "size": 1}
    try:
        result = state.es.search(index=index or TASK_INDEX_PATTERN, body=body)
    except Exception:
        return None, None
    hits = ((result or {}).get("hits") or {}).get("hits") or []
    if not hits:
        return None, None
    return hits[0].get("_source"), hits[0].get("_index")


def register(api, name_space):

    # -----------------------------------------------------------------------
    # /report/generate
    # -----------------------------------------------------------------------
    generate_model = api.model('report_generate_model', {
        'report': fields.String(description="The id of the report definition.", required=True),
        'parameters': fields.List(fields.Raw(), description="Values, by parameter name."),
        'task': fields.Raw(description="Mail recipients, subject and body."),
    })

    @name_space.route('/report/generate')
    class reportGenerate(Resource):
        @token_required()
        @api.doc(description="Submit a report definition for generation.",
                 params={'token': 'A valid token'})
        @api.expect(generate_model)
        def post(self, user=None):
            req = request.get_json(silent=True) or {}
            report_id = str(req.get("report") or "").strip()
            if not report_id:
                return {'error': "report is required"}, 400

            definition = load_definition(report_id)
            if definition is None:
                logger.warning("Report generation asked for an unknown definition: %s", report_id)
                return {'error': "unknown report"}, 404

            if not may_run(user, definition):
                logger.warning("Report %s refused to %s (privileges %s)",
                               report_id, (user or {}).get("login"), definition.get("privileges"))
                return {'error': "forbidden"}, 403

            parameters, error = merge_parameters(definition, req.get("parameters"))
            if error:
                return {'error': error}, 400

            report = dict(definition)
            report["parameters"] = parameters

            job_id = new_report_id()
            token = request.args.get("token") or (request.headers.get("Authorization") or "")[7:].strip()
            job = build_job(job_id, token, user, report, req.get("task"))
            state.conn.send_message(STEP1, json.dumps(job))
            logger.info("Report %s submitted by %s as %s", report_id, (user or {}).get("login"), job_id)
            return {'error': "", 'id': job_id}

    # -----------------------------------------------------------------------
    # /report/regenerate
    # -----------------------------------------------------------------------
    regenerate_model = api.model('report_regenerate_model', {
        'task': fields.String(description="The id of the report task to run again.", required=True),
        'index': fields.String(description="Its monthly index, when known."),
    })

    @name_space.route('/report/regenerate')
    class reportRegenerate(Resource):
        @token_required()
        @api.doc(description="Run an existing report task again, as the caller.",
                 params={'token': 'A valid token'})
        @api.expect(regenerate_model)
        def post(self, user=None):
            req = request.get_json(silent=True) or {}
            task_id = str(req.get("task") or "").strip()
            if not task_id:
                return {'error': "task is required"}, 400

            task_doc, task_index = load_task(task_id, req.get("index"))
            if task_doc is None:
                return {'error': "unknown task"}, 404

            # The task carries the definition as it was when it ran: regenerating replays that
            # snapshot rather than the current definition, so the previous parameters are kept.
            report = task_doc.get("report") or {}
            requester = ((task_doc.get("creds") or {}).get("user") or {}).get("id")
            login = (user or {}).get("id") or (user or {}).get("login")
            if requester and requester != login and not _is_admin(user):
                logger.warning("Regeneration of %s refused to %s (owned by %s)",
                               task_id, login, requester)
                return {'error': "forbidden"}, 403

            if not may_run(user, report):
                return {'error': "forbidden"}, 403

            job_id = new_report_id()
            token = request.args.get("token") or (request.headers.get("Authorization") or "")[7:].strip()
            job = build_job(job_id, token, user, report, task_doc.get("task"))
            state.conn.send_message(STEP1, json.dumps(job))
            logger.info("Task %s regenerated by %s as %s (from %s)",
                        task_id, login, job_id, task_index)
            return {'error': "", 'id': job_id}
