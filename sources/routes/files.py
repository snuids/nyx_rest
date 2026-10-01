"""
File-system routes: listdir, files (download/upload), reloadconfig,
streamfile, and upload endpoint. Also contains file utility helpers.
"""

import os
import json
import base64
import binascii
import string
import random
import logging
from pathlib import Path
from zipfile import ZipFile

import flask
from cachetools import cached, TTLCache
from flask import request, send_file
from flask_restx import Resource, fields

import state
from config import settings
from middleware import token_required, check_post_parameters
from helpers.disk_helper import (
    list_dir, get_all_file_paths, is_path_within_roots, resolve_under_root,
    normalize_log_path,
    can_access_file_app, can_access_logs,
)

logger = logging.getLogger()


# ---------------------------------------------------------------------------
# File utility helpers
# ---------------------------------------------------------------------------

def retrieve_app_info(rec_id, user):
    try:
        if state.elkversion >= 7:
            app = state.es.get(index="nyx_app", id=rec_id)
        else:
            app = state.es.get(index="nyx_app", doc_type="doc", id=rec_id)

        if app['_source']['type'] == 'file-system' and can_access_file_app(app['_source'], user):
            regex = ''
            if 'regex' in app['_source']['config']:
                regex = app['_source']['config']['regex']
            return app['_source']['config']['rootpath'], regex

    except Exception as e:
        logger.error("Unable to retrive root path of the app")
        logger.error(e)

    return None, None


@cached(cache=TTLCache(maxsize=1, ttl=300))
def _discover_app_roots(es, elkversion):
    """Return file-system apps; privilege checks are done per request."""
    roots = []
    try:
        if elkversion >= 7:
            res = es.search(index="nyx_app", body={"size": 1000})
        else:
            res = es.search(index="nyx_app", body={"size": 1000}, doc_type="doc")

        for hit in res["hits"]["hits"]:
            source = hit.get("_source", {})
            if source.get("type") == "file-system":
                root = source.get("config", {}).get("rootpath")
                if root:
                    roots.append((root, source))
    except Exception:
        logger.error("Unable to discover file-system app roots", exc_info=True)

    return roots


def get_allowed_stream_roots(user):
    """Roots under which /streamfile is allowed to read.

    App roots require access to that app. Configured roots are admin-only,
    except /logs, which is also available to users with the logs privilege.
    """
    configured = [r.strip() for r in settings.STREAM_ALLOWED_ROOTS.split(",") if r.strip()]
    roots = []
    if "/logs" in configured and can_access_logs(user):
        roots.append("/logs")
    if "admin" in user.get("privileges", []):
        roots += configured
    roots += [root for root, app in _discover_app_roots(state.es, state.elkversion)
              if can_access_file_app(app, user)]
    return roots


def randomString(stringLength):
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for i in range(stringLength))


def read_last_bytes(file_path, num_bytes):
    with open(file_path, 'rb') as f:
        try:
            f.seek(-num_bytes, os.SEEK_END)
            return f.read()
        except IOError:
            return f.read()
        return f.read()


# ---------------------------------------------------------------------------
# Route registration
# ---------------------------------------------------------------------------

def register(app, api, name_space):

    listdirAPI = api.model('listdir_model', {
        'rec_id': fields.String(description="The application rec_id.", required=True),
        'path': fields.String(description="The relative path of the application.", required=True),
    })

    @name_space.route('/listdir')
    class listDir(Resource):
        @token_required()
        @check_post_parameters("rec_id", "path")
        @api.doc(description="List files and directories in a directory.",
                 params={'token': 'A valid token'})
        @api.expect(listdirAPI)
        def post(self, user=None):
            req = json.loads(request.data.decode("utf-8"))
            path = req['path']

            if str(req['rec_id']) == '-1':
                if not can_access_logs(user):
                    return {'error': "not allowed"}
                prepath = "/logs"
                relative_path = normalize_log_path(path)
                regex = r".*\.log$"
            else:
                prepath, regex = retrieve_app_info(req['rec_id'], user)
                relative_path = "" if path == "/" else path

            if prepath is None:
                return {'error': "unknown app"}

            try:
                dirpath = resolve_under_root(prepath, relative_path)
            except ValueError:
                return {'error': "not allowed"}

            return list_dir(dirpath, path, regex, prepath)

    filesPostAPI = api.model('files_post_model', {
        'data': fields.String(description="A file in base64 format", required=True),
    })

    @name_space.route('/files')
    class files(Resource):
        @token_required()
        @api.doc(description="Download a file or a list of file.",
                 params={
                     'token': 'A valid token',
                     'rec_id': 'The application rec_id',
                     'path': 'the relative path inside the app',
                     'files': 'A file or a list of file (comma separated). (GET, DELETE)',
                 })
        def get(self, user=None):
            rec_id = request.args["rec_id"]
            path = request.args["path"]
            files_list = request.args["files"].split(',')

            logger.info(f"path    : {path}")

            if rec_id == '-1':
                if not can_access_logs(user):
                    return {'error': "not allowed"}
                prepath = "/logs"
                relative_path = normalize_log_path(path)
                regex = r".*\.log$"
            else:
                prepath, regex = retrieve_app_info(rec_id, user)
                relative_path = "" if path == "/" else path

            if prepath is None:
                return {'error': "unknown app"}

            try:
                dirpath = resolve_under_root(prepath, relative_path)
            except ValueError:
                return {'error': "not allowed"}

            if not files_list or not all(files_list):
                return {'error': 'error in file format'}

            # Older logs clients send the selected file itself as `path`.
            if rec_id == '-1' and os.path.isfile(dirpath):
                return flask.send_file(dirpath, download_name=os.path.basename(dirpath))

            filepaths_list = []
            for fil in files_list:
                try:
                    objpath = resolve_under_root(dirpath, fil)
                except ValueError:
                    return {'error': "not allowed"}

                if len(files_list) == 1 and os.path.isfile(objpath):
                    return flask.send_file(objpath, download_name=os.path.basename(fil))
                if os.path.isfile(objpath):
                    filepaths_list.append(objpath)
                elif os.path.isdir(objpath):
                    filepaths_list += get_all_file_paths(objpath, dirpath)

            zip_file_name = f"{randomString(10)}.zip"
            Path("./zip_folder").mkdir(parents=True, exist_ok=True)
            zip_path = os.path.abspath(f"./zip_folder/{zip_file_name}")

            try:
                with ZipFile(zip_path, 'w') as zip_:
                    for file in filepaths_list:
                        if not is_path_within_roots(file, [dirpath]):
                            return {'error': "not allowed"}
                        zip_.write(file, os.path.join('.', os.path.relpath(file, dirpath)))

                ret = send_file(zip_path, download_name=os.path.basename(files_list[0]))
                ret.content_type = 'zipfile'
                return ret
            finally:
                if os.path.exists(zip_path):
                    os.remove(zip_path)

        @token_required()
        @api.expect(filesPostAPI)
        def post(self, user=None):
            rec_id = request.args["rec_id"]
            path = request.args["path"]

            req = json.loads(request.data.decode("utf-8"))
            files_list = req['files']

            prepath, regex = retrieve_app_info(rec_id, user)

            if prepath is None:
                return {'error': "unknown app"}

            try:
                dirpath = resolve_under_root(prepath, "" if path == "/" else path)
            except ValueError:
                return {'error': "not allowed"}
            if not files_list:
                return {'error': 'error in file format'}

            try:
                uploads = [(resolve_under_root(dirpath, item['file_name']), item['data'])
                           for item in files_list]
            except (KeyError, TypeError, ValueError):
                return {'error': "not allowed"}

            for filepath, encoded_data in uploads:
                try:
                    file_data = base64.b64decode(encoded_data)
                    with open(filepath, "wb") as new_file:
                        new_file.write(file_data)
                except (OSError, ValueError, TypeError, binascii.Error):
                    logger.error("unable to write file %s", filepath, exc_info=True)
                    return {'error': "unable to write file"}

            return {"error": ""}

    @name_space.route('/reloadconfig')
    class reloadConfig(Resource):
        @api.doc(description="Recompute the user menus.", params={'token': 'A valid token'})
        @token_required()
        def get(self, user=None):
            logger.info(user)
            token = request.args["token"]
            from kibana_helpers import computeMenus
            finalcategory = computeMenus({"_source": user}, token, "console")
            return {
                'version': state.VERSION,
                'error': "",
                'cred': {'token': token, 'user': user},
                "menus": finalcategory,
            }

    @name_space.route('/streamfile')
    class streamFile(Resource):
        @api.doc(description="Stream file.", params={'token': 'A valid token'})
        @token_required()
        def get(self, user=None):
            logger.info(user)
            file_path = request.args["file"]

            if not is_path_within_roots(file_path, get_allowed_stream_roots(user)):
                logger.warning(f"streamfile denied for path: {file_path}")
                return {"data": "", "error": "not allowed"}

            if not os.path.isfile(file_path):
                return {"data": "", "error": "File not found"}

            try:
                data = read_last_bytes(file_path, 32000)
                return {"data": data.decode('utf-8', errors='ignore'), "error": ""}
            except Exception as e:
                logger.error(f"Error reading file: {e}")
                return {"data": "", "error": "Error reading file"}

    @app.route('/api/v1/upload', methods=['POST', 'GET', 'OPTIONS'])
    @token_required()
    def upload_file(user=None):
        logger.info(">>> File upload")
        queue = request.args.get('queue')
        logger.info("Destination:" + queue)
        if request.method == 'POST':
            if 'file' not in request.files:
                logger.error('No file part')
                return {"error": "NoFilePart"}
            file = request.files['file']
            if file.filename == '':
                logger.error('No selected file')
                return {"error": "NoSelectedFile"}
            if file:
                logger.info("FileName=" + file.filename)
                logger.info('file' * 100)
                logger.info(file)
                logger.info(user)
                data = file.read()
                state.conn.send_message(
                    queue,
                    base64.b64encode(data),
                    {
                        "file": file.filename,
                        "token": request.args.get('token'),
                        "user": json.dumps(user),
                        "upload_headers": request.headers.environ.get('HTTP_UPLOAD_HEADERS'),
                    },
                )
                return {"error": ""}
        return {"error": ""}
