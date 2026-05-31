"""
File-system routes: listdir, files (download/upload), reloadconfig,
streamfile, and upload endpoint. Also contains file utility helpers.
"""

import os
import json
import base64
import string
import random
import logging
from pathlib import Path
from zipfile import ZipFile

import flask
from flask import request, send_file
from flask_restx import Resource, fields

import state
from middleware import token_required, check_post_parameters
from helpers.disk_helper import list_dir

logger = logging.getLogger()


# ---------------------------------------------------------------------------
# File utility helpers
# ---------------------------------------------------------------------------

def retrieve_app_info(rec_id):
    try:
        if state.elkversion >= 7:
            app = state.es.get(index="nyx_app", id=rec_id)
        else:
            app = state.es.get(index="nyx_app", doc_type="doc", id=rec_id)

        logger.info(app)
        if app['_source']['type'] == 'file-system':
            regex = ''
            if 'regex' in app['_source']['config']:
                regex = app['_source']['config']['regex']
            return app['_source']['config']['rootpath'], regex

    except Exception as e:
        logger.error("Unable to retrive root path of the app")
        logger.error(e)

    return None, None


def remove_prefix(text, prefix):
    if text.startswith(prefix):
        return text[len(prefix):]
    return text


def randomString(stringLength):
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for i in range(stringLength))


def get_all_file_paths(directory):
    file_paths = []
    for root, directories, files in os.walk(directory):
        for filename in files:
            filepath = os.path.join(root, filename)
            file_paths.append(filepath)
    return file_paths


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

            if req['rec_id'] == -1:
                prepath = "/"
                regex = r".*\.log$"
            else:
                prepath, regex = retrieve_app_info(req['rec_id'])

            if prepath is None:
                return {'error': "unknown app"}

            prepath = os.path.abspath(prepath)
            logger.info(f"prepath : {prepath}")

            dirpath = os.path.abspath(f"{prepath}/{path}")
            logger.info(f"dirpath : {dirpath}")

            if not dirpath.startswith(prepath):
                return {'error': "not allowed"}

            return list_dir(dirpath, path, regex)

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
                prepath = "/"
                regex = r".*\.log$"
            else:
                prepath, regex = retrieve_app_info(rec_id)

            if prepath is None:
                return {'error': "unknown app"}

            prepath = os.path.abspath(prepath)
            logger.info(f"prepath : {prepath}")

            dirpath = os.path.abspath(f"{prepath}/{path}")
            logger.info(f"dirpath : {dirpath}")

            if not dirpath.startswith(prepath):
                return {'error': "not allowed"}

            if len(files_list) == 0:
                return {'error': 'error in file format'}
            elif len(files_list) == 1:
                if rec_id == '-1':
                    objpath = os.path.abspath(f"{dirpath}")
                    files_list[0] = files_list[0].split('/')[-1]
                else:
                    objpath = os.path.abspath(f"{dirpath}/{files_list[0]}")

                logger.info(f"objpath : {objpath}")

                if not objpath.startswith(prepath):
                    return {'error': "not allowed"}

                if os.path.isfile(objpath):
                    return flask.send_file(objpath, download_name=files_list[0])
                elif os.path.isdir(objpath):
                    logger.info(get_all_file_paths(objpath))
                    filepaths_list = get_all_file_paths(objpath)
                    zip_file_name = f"{randomString(10)}.zip"

                    Path("./zip_folder").mkdir(parents=True, exist_ok=True)

                    with ZipFile(f"./zip_folder/{zip_file_name}", 'w') as zip_:
                        for file in filepaths_list:
                            fname = f".{remove_prefix(file, dirpath)}"
                            zip_.write(file, fname)

                    logger.info(os.path.abspath(f"./zip_folder/{zip_file_name}"))

                    ret = send_file(
                        os.path.abspath(f"./zip_folder/{zip_file_name}"),
                        download_name=files_list[0],
                    )
                    ret.content_type = 'zipfile'
                    os.remove(f"./zip_folder/{zip_file_name}")
                    return ret
            else:
                filepaths_list = []

                for fil in files_list:
                    objpath = os.path.abspath(f"{dirpath}/{fil}")

                    if not objpath.startswith(prepath):
                        return {'error': "not allowed"}

                    logger.info(f"****{fil}   -> {objpath}    -  {os.path.isfile(objpath)}")

                    if os.path.isfile(objpath):
                        filepaths_list.append(objpath)
                    elif os.path.isdir(objpath):
                        logger.info(get_all_file_paths(objpath))
                        filepaths_list += get_all_file_paths(objpath)

                logger.info(filepaths_list)

                zip_file_name = f"{randomString(10)}.zip"
                Path("./zip_folder").mkdir(parents=True, exist_ok=True)

                with ZipFile(f"./zip_folder/{zip_file_name}", 'w') as zip_:
                    for file in filepaths_list:
                        fname = f".{remove_prefix(file, dirpath)}"
                        zip_.write(file, fname)

                logger.info(os.path.abspath(f"./zip_folder/{zip_file_name}"))

                ret = send_file(
                    os.path.abspath(f"./zip_folder/{zip_file_name}"),
                    download_name=files_list[0],
                )
                os.remove(f"./zip_folder/{zip_file_name}")
                ret.content_type = 'zipfile'
                return ret

        @api.expect(filesPostAPI)
        def post(self, user=None):
            rec_id = request.args["rec_id"]
            path = request.args["path"]

            req = json.loads(request.data.decode("utf-8"))
            files_list = req['files']

            prepath, regex = retrieve_app_info(rec_id)

            if prepath is None:
                return {'error': "unknown app"}

            prepath = os.path.abspath(prepath)
            logger.info(f"prepath : {prepath}")

            dirpath = os.path.abspath(f"{prepath}/{path}")
            logger.info(f"dirpath : {dirpath}")

            if not dirpath.startswith(prepath):
                return {'error': "not allowed"}
            if len(files_list) == 0:
                return {'error': 'error in file format'}
            if len(files_list) >= 1:
                for _file in files_list:
                    _file = files_list[0]
                    data_file_to_upload = base64.b64decode(_file['data'])
                    file_name = _file['file_name']

                    filepath = os.path.abspath(f"{dirpath}/{_file['file_name']}")
                    logger.info(f"filepath : {filepath}")

                    if not filepath.startswith(prepath):
                        return {'error': "not allowed"}

                    try:
                        newFile = open(filepath, "wb")
                        bytearr = bytearray(data_file_to_upload)
                        newFile.write(bytearr)
                    except:
                        logger.error(f"unable to write file {filepath}")
                    finally:
                        newFile.close()

                return {"error": ""}
            else:
                return {'error': 'dont handle multiple files upload for now'}

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
