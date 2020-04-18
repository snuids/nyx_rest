"""
v2.11.0 AMA 31/OCT/2019  Fixed a security issue that occured when the login is the mail address and get tokenized.
v2.12.0 VME 07/JAN/2020  Send a message to delete a token from all instances of the rest api when Logout.
v2.13.0 VME 23/JAN/2020  TTL tokens dictionnary, to avoid an alive token in the rest api and dead in redis.
v2.14.0 VME 05/FEB/2020  File system v1
v2.14.3 AMA 05/FEB/2020  Scrolls IDs are now correctly deleted
v2.15.0 VME 20/FEB/2020  Login will send all privileges and filters if admin
v2.15.1 VME 20/FEB/2020  Bug fixing
v3.0.0  AMA 23/FEB/2020  Compatible with elastic version 7.4.2
v3.0.1  VME 05/MAR/2020  Redisign of the files end point 
v3.0.2  VME 15/MAR/2020  Fixed a few postgresql issues
v3.1.0  VME 15/MAR/2020  Fixed an issue when % character is used in kibana
v3.3.1  AMA 06/Apr/2020  Fixed a privilege issue for collections with filtered columns
v3.3.2  AMA 09/Apr/2020  Token added to upload route
v3.3.3  AMA 10/Apr/2020  Added headers to send message API
v3.4.0  AMA 15/Apr/2020  Query filter can use elastic seacrh queries
v3.5.0  VME 15/Apr/2020  passing header "upload_headers" to broker when calling upload endpoint
v3.6.0  AMA 17/Apr/2020  PG queries can use an offset 
v3.6.2  AMA 18/Apr/2020  PG queries support ordering
"""

import re
import json
import time
import uuid
import flask
import redis
import base64
import prison
import random
import string
import random
import psycopg2
import requests
import operator
import importlib

import threading
import cachetools
import subprocess
import os,logging
import pandas as pd
import elasticsearch
from pathlib import Path
from flask import Response
from functools import wraps
from flask import send_file
from zipfile import ZipFile

from datetime import datetime
from datetime import timedelta
from importlib import resources
from pg_common import loadPGData
from passlib.hash import pbkdf2_sha256

from flask import make_response,url_for
from flask_cors import CORS, cross_origin
from amqstompclient import amqstompclient
from flask_restplus import Api, Resource, fields
from flask import Flask, jsonify, request,Blueprint
from logging.handlers import TimedRotatingFileHandler
from common import loadData,applyPrivileges,kibanaData,getELKVersion
from logstash_async.handler import AsynchronousLogstashHandler
from elasticsearch import Elasticsearch as ES, RequestsHttpConnection as RC


VERSION="3.6.2"
MODULE="nyx_rest"+"_"+str(os.getpid())

WELCOME=os.environ["WELCOMEMESSAGE"]
ICON=os.environ["ICON"]

elkversion=6


indices={}
indices_refresh_seconds=60
last_indices_refresh=datetime.now()-timedelta(minutes=10)

translations={}
last_translation_refresh_seconds=60
last_translation_refresh=datetime.now()-timedelta(minutes=10)


# tokens={}
tokens=cachetools.TTLCache(maxsize=1000, ttl=5*60)
tokenlock=threading.RLock()
userlock = threading.RLock()
logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(module)s - %(funcName)s: %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger()

lshandler=None

if os.environ["USE_LOGSTASH"]=="true":
    logger.info ("Adding logstash appender")
    lshandler=AsynchronousLogstashHandler("logstash", 5001, database_path='logstash_test.db')
    lshandler.setLevel(logging.ERROR)
    logger.addHandler(lshandler)

handler = TimedRotatingFileHandler("logs/nyx_rest_api.log",
                                when="d",
                                interval=1,
                                backupCount=30)

logFormatter = logging.Formatter('%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s')
handler.setFormatter( logFormatter )
logger.addHandler(handler)

logger.info("Starting...")
logger.info("REST API %s" %(VERSION))

userActivities=[]

app = Flask(__name__, static_folder='temp', static_url_path='/temp')#, static_url_path='/temp')
blueprint = Blueprint('api', __name__, url_prefix='')

class Custom_API(Api):
    @property
    def specs_url(self):
        '''
        The Swagger specifications absolute url (ie. `swagger.json`)

        :rtype: str
        '''
        return url_for(self.endpoint('specs'), _external=False)

api = Custom_API(blueprint, doc='/api/doc/',version='1.0', title='Nyx Rest API',
    description='Nyx Rest API')

app.register_blueprint(blueprint)

name_space = api.namespace('api/v1', description='Main APIs')

CORS(app)

logger.info("Starting redis connection")
logger.info("IP=>"+os.environ["REDIS_IP"]+"<")
redisserver = redis.Redis(host=os.environ["REDIS_IP"], port=6379, db=0)
OUTPUT_FOLDER=os.environ["OUTPUT_FOLDER"]
OUTPUT_URL=os.environ["OUTPUT_URL"]

pg_connection=None
pg_thread=None

class DateTimeEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, (datetime, datetime.date, datetime.time)):
            return obj.isoformat()
        elif isinstance(obj, datetime.timedelta):
            return (datetime.min + obj).time().isoformat()

        return super(DateTimeEncoder, self).default(obj)

def check_pg():
    global pg_connection
    while True:
        time.sleep(10)
        try:
            logger.info("Check PG...")
            cur = pg_connection.cursor()
            cur.execute('SELECT 1')
            cur.close()
        except Exception as e:
            logger.error("Unable to check posgresql",exc_info=True)
            pg_connection=None
            get_postgres_connection()
            pass


def get_postgres_connection():
    global pg_connection,pg_thread
    logger.info(">>> Create PG Connection")
    if pg_connection!=None:
        return pg_connection
    try:
        pg_connection = psycopg2.connect(user = os.environ["PG_LOGIN"],
                                    password = os.environ["PG_PASSWORD"],
                                    host = os.environ["PG_HOST"],
                                    port = os.environ["PG_PORT"],
                                    database = os.environ["PG_DATABASE"])
        cursor = pg_connection.cursor()
        # Print PostgreSQL Connection properties
        logger.info ( pg_connection.get_dsn_parameters())
        # Print PostgreSQL version
        cursor.execute("SELECT version();")
        record = cursor.fetchone()
        logger.info("Connected to - %s" % (record))

        if pg_thread== None:
            logger.info("Creating PG ping thread.")
            pg_thread = threading.Thread(target = check_pg)
            pg_thread.start()


        return pg_connection
    except (Exception, psycopg2.Error) as error :
        logger.error("Error while connecting to PostgreSQL", error)
    return None

def clean_kibana_url_0(url):
    url=url.replace('<iframe src="https://',"")
    url=url.replace('" height="600" width="800"></iframe>',"")
    url=url.replace('/kibana/app/','/kibananyx/app/')
    return url
    
def clean_kibana_url(url,column,filter):
    regex = r"(query:'[^']*')"
    replacement="("+(" OR ".join([column+":"+x for x in filter]))+")"

    matches = re.finditer(regex, url)

    for matchNum, match in enumerate(matches):
        matchNum = matchNum + 1

        for groupNum in range(0, len(match.groups())):
            groupNum = groupNum + 1

            query=match.group(groupNum)
            first=query.find(":")

            minquery=query[first+2:-1]
            if minquery=='':
                minquery=replacement
            else:
                minquery+=" AND "+replacement

            minquery="query:'"+minquery+"'"
            print(minquery)
            url=url.replace(match.group(groupNum),minquery)
    return url    
                   

def getUserFromToken(request):
    global tokens
    token=request.args.get('token')
    with tokenlock:
        if token in tokens:
            return tokens[token]
        redusr=redisserver.get("nyx_tok_"+token)
        logger.info("nyx_fulltok_"+token)
        if redusr!=None:
            logger.info("Retrieved user "+token+ " from redis.")
            redusrobj=json.loads(redusr)
            tokens[token]=redusrobj
            logger.info("Token reinitialized from redis cluster.")
            return redusrobj

    logger.info("Invalid Token:"+token)
    return None


#---------------------------------------------------------------------------
# CHECK POST
#---------------------------------------------------------------------------
def check_post_parameters(*parameters):
    def wrapper(f):        
        @wraps(f)
        def decorated_function(*args, **kwargs):
            try:
                req= json.loads(request.data.decode("utf-8"))
                for param in parameters:
                    if not param in req:
                        return {'error':"MISSING_PARAM:"+param}        
            except Exception as e:                
                logger.error("Unable to decode body")
                return {'error':"UNABLE_TO_DECODE_BODY"}

            return f(*args, **kwargs)
        return decorated_function
    return wrapper

#---------------------------------------------------------------------------
# DECORATOR
#---------------------------------------------------------------------------
def token_required(*roles):
    def wrapper(f):        
        @wraps(f)
        def decorated_function(*args, **kwargs):
            logger.info(">>> START:"+request.path+ ">>>>"+request.method)
            starttime=int(datetime.now().timestamp()*1000)
            ret=None
            usr=None
            if not "token" in request.args:
                ret={'error':"NO_TOKEN"}
            else:
                usr=getUserFromToken(request)
                if usr==None:
                    ret={'error':"UNKNOWN_TOKEN"}
                else:
                    
                    ok=False
                    if len(roles)==0:
                        ok=True
                    elif "admin" in usr["privileges"]:
                        ok=True
                    else:
                        for priv in usr["privileges"]:
                            if priv in roles:
                                ok=True
                                break
                    if ok:                        
                        kwargs["user"]=usr
                        ret= f(*args, **kwargs)
                    else:
                        ret={'error':"NO_PRIVILEGE"}
            endtime=int(datetime.now().timestamp()*1000)

            timespan = endtime-starttime
            logger.info(type(ret))

            logger.info("<<< FINISH:"+request.path)
            
            error=''
            if type(ret) == dict:
                error=ret["error"]

            if "token" in request.args:
                pushHistoryToELK(request,timespan,usr, request.args["token"],error)

            if type(ret) != dict:
                return ret
            
            ret["timespan"]=timespan
            return jsonify(ret)
        return decorated_function
    return wrapper

################################################################################
def pushHistoryToELK(request,timespan,usr,token,error):
    global userActivities

    rec={"url":request.path,"method":request.method,"timespan":timespan,"user":usr["login"] if usr else ""
            ,"token":token,"error":error,"@timestamp":int(datetime.now().timestamp())*1000}
    if "login" in request.path:
        agent={
                    "browser" : request.user_agent.browser
                    ,"version" : request.user_agent.version
                    ,"platform" : request.user_agent.platform
                    ,"language" : request.user_agent.language
                    ,"string" : request.user_agent.string
                }
        rec["agent"]=agent
    with userlock:
        userActivities.append(rec)        
        

#---------------------------------------------------------------------------
# API css
#---------------------------------------------------------------------------
@app.route('/api/v1/ui_css')
def cssRest():    
    logger.info("CSS called")
    if elkversion==7:
        res=es.get(index="nyx_config",id="nyx_css")    
    else:
        res=es.get(index="nyx_config",id="nyx_css",doc_type="doc")    
    #header("Content-type: text/xml")
    return Response(res["_source"]["file"], mimetype='text/css')
     

#---------------------------------------------------------------------------
# API configRest
#---------------------------------------------------------------------------
@name_space.route('/config')
@api.doc(description="Get the instance config.")
class configRest(Resource):    
    def get(self):
        logger.info("Config called")
        return {'error':"",'status':'ok','version':VERSION,'welcome':WELCOME,'icon':ICON}


#---------------------------------------------------------------------------
# API statusRest
#---------------------------------------------------------------------------
@name_space.route('/status')
@api.doc(description="Get the instance status.",params={'token': 'A valid token'})
class statusRest(Resource):
    @token_required("A1","A2")
    def get(self,user=None):        
        return {'error':"",'status':'ok','version':VERSION,'name':MODULE}

#---------------------------------------------------------------------------
# API errorRest
#---------------------------------------------------------------------------
@name_space.route('/error')
class errorRest(Resource):    
    @api.doc(description="Error log debug.",params={'token': 'A valid token'})
    @token_required("A1","A2")
    def get(self,user=None):
        logger.error("ERROR")
        return {'error':"",'status':'ok','version':VERSION,'name':MODULE}

#---------------------------------------------------------------------------
# API sendMessage
#---------------------------------------------------------------------------

sendMessageAPI = api.model('sendMessage_model', {
    'destination': fields.String(description="The destinaiton example: /queue/TEST", required=True),
    'body': fields.String(description="The message as a string.", required=True),
    'headers': fields.String(description="The headers as a string (STRINGIFIED).")
})

@name_space.route('/sendmessage')
class sendMessage(Resource):
    @token_required()
    @check_post_parameters("destination","body")
    @api.doc(description="Send a message to the broker.",params={'token': 'A valid token'})
    @api.expect(sendMessageAPI)
   # @api.doc(body={"destination":"/queue/TEST","body":"Hello"})
    def post(self,user=None):
        req= json.loads(request.data.decode("utf-8"))   
        headers=None 
        if "headers" in req and len(req["headers"])>0:
            headers=json.loads(req["headers"])
        conn.send_message(req["destination"],req["body"],headers=headers)  
        return {'error':""}


#---------------------------------------------------------------------------
# API list dir 
#---------------------------------------------------------------------------

listdirAPI = api.model('listdir_model', {
    'rec_id': fields.String(description="The application rec_id.", required=True),
    'path': fields.String(description="The relative path of the application.", required=True)
})

@name_space.route('/listdir')
class listDir(Resource):
    @token_required()
    @check_post_parameters("rec_id","path")
    @api.doc(description="List files and directories in a directory.",params={'token': 'A valid token'})
    @api.expect(listdirAPI)
    def post(self,user=None):
        req= json.loads(request.data.decode("utf-8"))
        
        path = req['path']
        logger.info(f"path    : {path}")

        prepath, regex = retrieve_app_info(req['rec_id'])

        if prepath is None:
            return {'error':"unknown app"}


        prepath = os.path.abspath(prepath)

        logger.info(f"prepath : {prepath}")

        dirpath = os.path.abspath(f"{prepath}/{path}")

        logger.info(f"dirpath : {dirpath}")

        if not dirpath.startswith(prepath):
            return {'error':"not allowed"}

        return list_dir(dirpath, path, regex)

def retrieve_app_info(rec_id):
    try:
        if elkversion==7:
            app=es.get(index="nyx_app",id=rec_id)
        else:
            app=es.get(index="nyx_app",doc_type="doc",id=rec_id)


        logger.info(app)
        if app['_source']['type'] == 'file-system':
            regex = ''
            if 'regex' in app['_source']['config']:
                regex = app['_source']['config']['regex']

            return app['_source']['config']['rootpath'], regex

    except elasticsearch.NotFoundError:
        logger.warn('Unknown app')
    except Exception as e:
        logger.error("Unable to retrive root path of the app")
        logger.error(e)

    return None

def list_dir(dir_path, rel_path, regex):
    try:
        dir_list = os.listdir(dir_path)
        
        ret = []        
        for i in dir_list:
            path = os.path.abspath(dir_path+'/'+i)
            print(path)

            stats = os.stat(path)
            obj_name = path.split('/')[-1]

            extension = 'dir'
            if os.path.isfile(path):
                obj_type = 'file'

                if regex != '':
                    z = re.match(regex, obj_name)

                    if z is None:
                        continue

                extension = obj_name.split('.')[-1]

            if os.path.isdir(path):
                obj_type = 'dir'

            obj = {
                'path' : (rel_path+'/'+i).replace('//','/'),
                'creation_time' : int(stats.st_ctime),
                'modification_time' : int(stats.st_mtime),
                'name' : obj_name,
                'type' : obj_type,
                'size' : stats.st_size,
                'extension' : extension 
            }

            ret.append(obj)
                
        return {'error':"", 'data':ret}
            
    except FileNotFoundError:
        logger.error(f"the directory {dir_path} doesnt exist")
        return {'error':"the directory doesnt exist"}
    except NotADirectoryError:
        logger.error(f"{dir_path} is not a directory")
        return {'error':"not a directory"}



#---------------------------------------------------------------------------
# API download file
#---------------------------------------------------------------------------

filesPostAPI = api.model('files_post_model', {
    'data': fields.String(description="A file in base64 format", required=True),
})

@name_space.route('/files')
class files(Resource):
    @token_required()
    # @check_post_parameters("rec_id","path","files")
    @api.doc(description="Download a file or a list of file.",
             params={'token': 'A valid token', 
                     'rec_id': 'The application rec_id', 
                     'path':'the relative path inside the app',
                     'files': 'A file or a list of file (comma separated). (GET, DELETE)',})
    # @api.expect(filesAPI)
    def get(self,user=None):
        rec_id=request.args["rec_id"]
        path=request.args["path"]
        files=request.args["files"].split(',')

        logger.info(f"path    : {path}")

        prepath, regex = retrieve_app_info(rec_id)

        if prepath is None:
            return {'error':"unknown app"}

        prepath = os.path.abspath(prepath)

        logger.info(f"prepath : {prepath}")

        dirpath = os.path.abspath(f"{prepath}/{path}")

        logger.info(f"dirpath : {dirpath}")

        if not dirpath.startswith(prepath):
            return {'error':"not allowed"}

        if len(files) == 0:
            return {'error':'error in file format'}
        elif len(files) == 1:

            objpath = os.path.abspath(f"{dirpath}/{files[0]}")

            logger.info(f"objpath : {objpath}")

            if not objpath.startswith(prepath):
                return {'error':"not allowed"}


            if os.path.isfile(objpath):
                return send_file(objpath, attachment_filename=files[0], cache_timeout=5)
            elif  os.path.isdir(objpath):

                logger.info(get_all_file_paths(objpath))

                filepaths_list = get_all_file_paths(objpath)

                zip_file_name = f"{randomString(10)}.zip"            

                Path("./zip_folder").mkdir(parents=True, exist_ok=True)

                with ZipFile(f"./zip_folder/{zip_file_name}",'w') as zip: 
                    # writing each file one by one 
                    for file in filepaths_list: 
                        fname = f".{remove_prefix(file, dirpath)}"
                        zip.write(file, fname) 
                
                logger.info(os.path.abspath(f"./zip_folder/{zip_file_name}"))

                ret = send_file(os.path.abspath(f"./zip_folder/{zip_file_name}"), attachment_filename=files[0], cache_timeout=5)
                ret.content_type = 'zipfile'
                os.remove(f"./zip_folder/{zip_file_name}")

                return ret

        else:
            
            zippath = os.path.abspath(f"{prepath}/download.zip")

            logger.info(f"zippath : {zippath}")

            filepaths_list = []

            for fil in files:
                objpath = os.path.abspath(f"{dirpath}/{fil}")

                if not objpath.startswith(prepath):
                    return {'error':"not allowed"}

                logger.info(f"****{fil}   -> {objpath}    -  {os.path.isfile(objpath)}")

                if os.path.isfile(objpath):
                    filepaths_list.append(objpath)
                elif  os.path.isdir(objpath):

                    logger.info(get_all_file_paths(objpath))

                    filepaths_list += get_all_file_paths(objpath)

            logger.info(filepaths_list)

            zip_file_name = f"{randomString(10)}.zip"            

            Path("./zip_folder").mkdir(parents=True, exist_ok=True)

            with ZipFile(f"./zip_folder/{zip_file_name}",'w') as zip: 
                # writing each file one by one 
                for file in filepaths_list: 
                    fname = f".{remove_prefix(file, dirpath)}"
                    zip.write(file, fname) 
            
            logger.info(os.path.abspath(f"./zip_folder/{zip_file_name}"))

            ret = send_file(os.path.abspath(f"./zip_folder/{zip_file_name}"), attachment_filename=files[0], cache_timeout=5)

            os.remove(f"./zip_folder/{zip_file_name}")
            ret.content_type = 'zipfile'
            return ret

            #return {'error':'zip mode not yet implemented'}

    @api.expect(filesPostAPI)
    def post(self,user=None):
        rec_id=request.args["rec_id"]
        path=request.args["path"]
        # files=request.args["files"].split(',')
        
        req= json.loads(request.data.decode("utf-8"))
        files = req['files']


        prepath, regex = retrieve_app_info(rec_id)

        if prepath is None:
            return {'error':"unknown app"}

        prepath = os.path.abspath(prepath)

        logger.info(f"prepath : {prepath}")

        dirpath = os.path.abspath(f"{prepath}/{path}")

        logger.info(f"dirpath : {dirpath}")

        if not dirpath.startswith(prepath):
            return {'error':"not allowed"}
        if len(files) == 0:
            return {'error':'error in file format'}
        if len(files) >= 1:

            for _file in files:
                _file = files[0]
                data_file_to_upload = base64.b64decode(_file['data'])
                file_name=_file['file_name']

                filepath = os.path.abspath(f"{dirpath}/{_file['file_name']}")

                logger.info(f"filepath : {filepath}")

                if not filepath.startswith(prepath):
                    return {'error':"not allowed"}

                try:
                    newFile = open(filepath, "wb")
                    bytearr = bytearray(data_file_to_upload)
                    newFile.write(bytearr)
                except:
                    logger.error(f"unable to write file {filepath}")
                finally:
                    newFile.close()

            return {"error":""}

        else:
            return {'error':'dont handle multiple files upload for now'}



def remove_prefix(text, prefix):
    if text.startswith(prefix):
        return text[len(prefix):]
    return text  # or whatever

def randomString(stringLength):
    """Generate a random string with the combination of lowercase and uppercase letters """

    letters = string.ascii_letters
    return ''.join(random.choice(letters) for i in range(stringLength))


def get_all_file_paths(directory): 
  
    # initializing empty file paths list 
    file_paths = [] 
  
    # crawling through directory and subdirectories 
    for root, directories, files in os.walk(directory): 
        for filename in files: 
            # join the two strings in order to form the full filepath. 
            filepath = os.path.join(root, filename) 
            file_paths.append(filepath) 
  
    # returning all file paths 
    return file_paths   

#---------------------------------------------------------------------------
# API reloadConfiguration
#---------------------------------------------------------------------------
@name_space.route('/reloadconfig')
class reloadConfig(Resource):
    @api.doc(description="Recompute the user menus.",params={'token': 'A valid token'})
    @token_required()
    def get(self,user=None):
        logger.info(user)
        token=request.args["token"]
        finalcategory=computeMenus({"_source":user},token)
        
        return {'version':VERSION,'error':"",'cred':{'token':token,'user':user},"menus":finalcategory}


def computeMenus(usr,token):
    
    refresh_translations()
    if elkversion==7:
        res3=es.search(size=1000,index="nyx_app",body={"sort" : [{ "order" : "asc" }]})
    else:
        res3=es.search(size=1000,index="nyx_app",doc_type="doc",body={"sort" : [{ "order" : "asc" }]})
    
    dict_dashboard=get_dict_dashboards(es)

    categories={}
    for app in res3["hits"]["hits"]:
        appl=app["_source"]
        appl["rec_id"]=app["_id"]

        if "privileges" in usr["_source"] and "privileges" in appl and not "admin" in usr["_source"]["privileges"]:
            if len([value for value in usr["_source"]["privileges"] if value in appl["privileges"]])==0:
                continue

        if appl.get("type")=="kibana":
            logger.info('compute kibana url for : '+str(appl.get('title')))

            config = appl["config"]


            old_kibana_url=config.get("url")

            config["url"]=compute_kibana_url(dict_dashboard, appl)

            if config.get("filtercolumn") is not None and config.get("filtercolumn")!="" and "filters" in usr["_source"] and len(usr["_source"]["filters"])>0:
                logger.info('compute kibana url for : '+str(appl.get('title')))
                config["url"]=clean_kibana_url(config.get('url'),config.get("filtercolumn"),usr["_source"]["filters"])

            if old_kibana_url != config.get("url"):
                logger.warning('the url calculated for app: '+appl.get('title')+' is desync from the database (ES)')
                logger.warning(config.get("url"))
                logger.warning(old_kibana_url)
                logger.warning('we have to update database !!!')

                app_to_index = appl.copy()
                del app_to_index['rec_id']                
                es.index(index=app['_index'], doc_type=app['_type'], id=app['_id'], body=app_to_index)

        if appl["category"] not in categories:
            categories[appl["category"]]={"subcategories":{}}

        if "subcategory" in appl and appl["subcategory"] in categories[appl["category"]]["subcategories"]:
            target=categories[appl["category"]]["subcategories"][appl["subcategory"]]
            
        elif "subcategory" in appl and appl["subcategory"] not in categories[appl["category"]]["subcategories"]:
            target=categories[appl["category"]]["subcategories"][appl["subcategory"]]=[]
        else:
            if "" in categories[appl["category"]]["subcategories"]:
                target=categories[appl["category"]]["subcategories"][""]
            else:
                target=categories[appl["category"]]["subcategories"][""]=[]
        target.append(appl)

    finalcategory=[]

    language=usr["_source"]["language"]
    logger.info("User language:"+language)

    for key in categories:
        loc_cat=get_translated_item(language,"menus",key)
        finalcategory.append({"category":key,"loc_category":loc_cat,"submenus":[]})
        target=finalcategory[-1]
        for key2 in categories[key]:

            for key3 in categories[key][key2]:
                loc_sub=get_translated_item(language,"menus",key3)
                target["submenus"].append({"title":key3,"loc_title":loc_sub,"apps":[]})
                for appli in categories[key][key2][key3]:
                    
                    del appli["category"]
                    if "subcategory" in appli:
                        del appli["subcategory"]
                    if "order" in appli:                    
                        del appli["order"]
                    if "privileges" in appli:                    
                        del appli["privileges"]
                                                
                    appli["loc_title"]=  get_translated_item(language,"menus",appli["title"])                                              
                    target["submenus"][-1]["apps"].append(appli)
                
                if len(target["submenus"][-1]["apps"])>0 and "icon" in target["submenus"][-1]["apps"][0]:
                    target["submenus"][-1]["icon"]=target["submenus"][-1]["apps"][0]["icon"]
    
    return finalcategory


#---------------------------------------------------------------------------
# API login
#---------------------------------------------------------------------------

loginAPI = api.model('login_model', {
    'login': fields.String(description="The user login", required=True),
    'password': fields.String(description="The user password.", required=True)
})

@name_space.route('/cred/login',methods=['POST'])    
class loginRest(Resource):
    @api.doc(description="Send a message to the broker.")
    @api.expect(loginAPI)
    def post(self):
        global tokens
        logger.info(">> LOGIN IN")        
        data=json.loads(request.data.decode("utf-8"))
        #logger.info(data)

        if ("login" in data) and ("password" in data):

            cleanlogin=data["login"].split(">")[0]

            try:
                if elkversion==7:
                    usr=es.get(index="nyx_user",id=cleanlogin)
                else:
                    usr=es.get(index="nyx_user",doc_type="doc",id=cleanlogin)
            except:
                logger.info("Not found",exc_info=True)
                usr=None
                logger.info("Searching by login")
                body={"size":"100",
                        "query": {
                            "bool": {
                                "must": [
                                    {
                                        "term": {
                                        "login.keyword": {
                                            "value": cleanlogin,
                                            "boost": 1
                                        }
                                        }
                                    }                                    
                                ]
                                
                            }
                        }
                    }
                if elkversion==7:
                    users=es.search(index="nyx_user",body=body)
                else:
                    users=es.search(index="nyx_user",doc_type="doc",body=body)
                #logger.info(users)
                if "hits" in users and "hits" in users["hits"] and len (users["hits"]["hits"])>0:
                    usr=users["hits"]["hits"][0]

            logger.info("USR_"*20)
            logger.info(usr)

            if usr !=None and pbkdf2_sha256.verify(data["password"], usr["_source"]["password"]):

                if usr["_source"].get("doublePhase",False)==True:
                    if "doublecode" in data:
                        logger.info("Must check code")
                        codeindb=redisserver.get("nyx_double_"+data["login"])
                        if(codeindb!=None):
                            codeindb=codeindb.decode("ascii")                    
                        logger.info("In redis:")
                        logger.info(codeindb)
                        logger.info(data["doublecode"])
                        if str(codeindb) != data["doublecode"]:    
                            redisserver.delete("nyx_double_"+data["login"])
                            return jsonify({'error':"ErrorDoublePhase"})
                    else:
                        randint=""+str(random.randint(10000, 99999))
                        redisserver.set("nyx_double_"+data["login"],randint,120)
                        logger.info("Code is "+randint)
                        conn.send_message("/topic/AUTH_SMS",json.dumps({"message":"Your access code is:"+randint,"phone":usr["_source"]["phone"]}))
                        return jsonify({'error':"DoublePhase"})

                if ">" in data["login"] and "admin" in usr["_source"]["privileges"]:
                    otheruser=data["login"].split(">")[1]
                    try:
                        if elkversion==7:
                            usr=es.get(index="nyx_user",id=otheruser)
                        else:
                            usr=es.get(index="nyx_user",doc_type="doc",id=otheruser)
                    except:
                        usr=None
                        return jsonify({'error':"Unknown User"})

                token=uuid.uuid4()
                        
                with tokenlock:
                    tokens[str(token)]=usr["_source"]       #TO BE DONE REMOVE PREVIOUS TOKENS OF THIS USER  

                usr["_source"]["password"]=""
                usr["_source"]["id"]=data["login"]

                redisserver.set("nyx_tok_"+str(token),json.dumps(usr["_source"]),3600*24)

                finalcategory=computeMenus(usr,str(token))

                all_priv=[]
                all_filters=[]
                if "admin" in usr["_source"]["privileges"]:
                    all_priv=[]
                    all_filters=[]

                    all_priv = loadData(es,conn,'nyx_privilege',{},'doc',False,(None, None, None)
                                                    ,True,usr['_source'],None,None,None)['records']

                    all_filters = loadData(es,conn,'nyx_filter',{},'doc',False,(None, None, None)
                                                    ,True,usr['_source'],None,None,None)['records']

                resp=make_response(jsonify({'version':VERSION,'error':"",'cred':{'token':token,'user':usr["_source"]},
                                                            "menus":finalcategory,"all_priv":all_priv,"all_filters":all_filters}))
                resp.set_cookie('nyx_kibananyx', str(token))

                setACookie("nodered",usr["_source"]["privileges"],resp,token)
                setACookie("anaconda",usr["_source"]["privileges"],resp,token)
                setACookie("cerebro",usr["_source"]["privileges"],resp,token)
                setACookie("kibana",usr["_source"]["privileges"],resp,token)
                setACookie("logs",usr["_source"]["privileges"],resp,token)

                pushHistoryToELK(request,0,usr["_source"], str(token),"")
                return resp
            else:
                return jsonify({'error':"Bad Credentials"})


        return jsonify({'error':"Bad Request"})

def setACookie(privilege,privileges,resp,token):
    
    if "admin" in privileges or (len(privileges)>0 and privilege in privileges):
        redisserver.set("nyx_"+privilege.lower()+"_"+str(token),"OK",3600*24)
        resp.set_cookie('nyx_'+privilege.lower(), str(token))

#---------------------------------------------------------------------------
# Logout
#---------------------------------------------------------------------------
@name_space.route('/cred/logout')
class logout(Resource):
    @token_required()
    @api.doc(description="Log the user out.",params={'token': 'A valid token'})
    def get(self,user=None):
        logger.info(">>> Logout")
        token=request.args.get('token')
        redisserver.delete("nyx_tok_"+str(token))
        redisserver.delete("nyx_nodered_"+str(token))
        redisserver.delete("nyx_cerebro_"+str(token))
        redisserver.delete("nyx_kibana_"+str(token))
        redisserver.delete("nyx_anaconda_"+str(token))
        redisserver.delete("nyx_logs_"+str(token))
        if token in tokens:
            del tokens[token]
        
        conn.send_message("/topic/LOGOUT_EVENT",token)

        return {"error":""}


#---------------------------------------------------------------------------
# reset password
#---------------------------------------------------------------------------

reset_passwordAPI = api.model('reset_password_model', {
    'login': fields.String(description="The user login", required=True),
    'new_password': fields.String(description="The user password.", required=True)
})

@name_space.route('/cred/resetpassword')
class reset_password(Resource):
    @token_required("admin","useradmin")
    @check_post_parameters("login","new_password")
    @api.doc(description="Resets a user password.",params={'token': 'A valid token'})
    @api.expect(reset_passwordAPI)
    def post(self,user=None):
        logger.info(">>> Reset password");
        req= json.loads(request.data.decode("utf-8"))  
        try:
            if elkversion==7:
                usrdb=es.get(index="nyx_user",id=req["login"])
            else:
                usrdb=es.get(index="nyx_user",doc_type="doc",id=req["login"])
        except:
            return {"error":"usernotfound"}
            
        usrdb["_source"]["password"]=pbkdf2_sha256.hash(req["new_password"])
        if elkversion==7:
            res=es.index(index="nyx_user",body=usrdb["_source"],id=req["login"])
        else:
            res=es.index(index="nyx_user",body=usrdb["_source"],doc_type="doc",id=req["login"])

        usrdb["_source"]["id"]=usrdb["_id"]

        if "queue" in req:
            conn.send_message(req["queue"],json.dumps({"byuser":user,"foruser":usrdb["_source"],"newpassword":req["new_password"]})) 

        return {"error":""}
    

#---------------------------------------------------------------------------
# Change password
#---------------------------------------------------------------------------

change_passwordAPI = api.model('change_password_model', {
    'old_password': fields.String(description="The user old password", required=True),
    'new_password': fields.String(description="The user password.", required=True)
})

@name_space.route('/cred/changepassword')
class change_password(Resource):
    @token_required()
    @check_post_parameters("old_password","new_password")
    @api.doc(description="Change an user password.",params={'token': 'A valid token'})
    @api.expect(change_passwordAPI)
    def post(self,user=None):
        logger.info(">>> Change password");
        req= json.loads(request.data.decode("utf-8"))  
        logger.info(req)
        logger.info(user)
        if elkversion==7:
            usrdb=es.get(index="nyx_user",id=user["id"])
        else:
            usrdb=es.get(index="nyx_user",doc_type="doc",id=user["id"])
        if pbkdf2_sha256.verify(req["old_password"], usrdb["_source"]["password"]):
            
            usrdb["_source"]["password"]=pbkdf2_sha256.hash(req["new_password"])
            if elkversion==7:
                res=es.index(index="nyx_user",body=usrdb["_source"],id=user["id"])
            else:
                res=es.index(index="nyx_user",body=usrdb["_source"],doc_type="doc",id=user["id"])
            logger.info(res)        
            return {"error":""}
        else:
            return {"error":"wrongpassword"}

    


#---------------------------------------------------------------------------
# Upload file
#---------------------------------------------------------------------------
@app.route('/api/v1/upload', methods=['POST','GET','OPTIONS'])
@token_required()
def upload_file(user=None):
    logger.info(">>> File upload");
    queue=request.args.get('queue')
    logger.info("Destination:"+queue);
    if request.method == 'POST':
        # check if the post request has the file part
        if 'file' not in request.files:
            logger.error('No file part')
            return {"error":"NoFilePart"}
        file = request.files['file']
        # if user does not select file, browser also
        # submit a empty part without filename
        if file.filename == '':
            logger.error('No selected file')
            return {"error":"NoSelectedFile"}
        if file:
            logger.info("FileName="+file.filename)
            logger.info('file'*100)
            logger.info(file)
            logger.info(user)
            data=file.read()
            conn.send_message(queue,base64.b64encode(data),{"file":file.filename,"token":request.args.get('token'), "user":json.dumps(user), "upload_headers":request.headers.environ.get('HTTP_UPLOAD_HEADERS')}) 
            return {"error":""}
    return {"error":""}

#---------------------------------------------------------------------------
# API query filter
#---------------------------------------------------------------------------

queryFilterAPI = api.model('queryFilter_model', {    
    
})
#{"size":200,"query":{"bool":{"must":[{"match_all":{}}]}}}

@name_space.route('/queryFilter/<string:rec_id>')
class genericQueryFilter(Resource):
    @token_required()
    @api.doc(description="Fills the query filters.",params={'token': 'A valid token'})
    @api.expect(queryFilterAPI)
    def post(self,rec_id,user=None):
        global es
        
        logger.info("Query Filter="+rec_id);    
        app=None
        if elkversion==7:
            app=es.get(index="nyx_app",id=rec_id)
        else:
            app=es.get(index="nyx_app",doc_type="doc",id=rec_id)

        if app==None:
            return  {"error":"UNKNOWN APP"}

        app=app["_source"]

        if "queryfilters" not in app["config"]:
            return  {"error":"NO QUERY FILTERS"}

        for queryf in app["config"]["queryfilters"]:
            if queryf["type"]=="queryselecter":
                #logger.info("Compute Selecter")
                cui=can_use_indice(queryf["index"],user,None)
                query={"from":0,"size":0,"aggregations":{queryf["column"]:{"terms":{"field":queryf["column"],"size":200,"order":[{"_key":"asc"}]}}}}
                query["query"]=cui[1]
                #logger.info(json.dumps(query))
                res=es.search(index=queryf["index"],body=query)
                #logger.info()
                queryf["buckets"]=res["aggregations"][queryf["column"]].get("buckets",[])

        

        #data= json.loads(request.data.decode("utf-8"))           
        #return loadPGData(es,appid,get_postgres_connection(),conn,data,(request.args.get("download","0")=="1")
        #            ,True,user,request.args.get("output","csv"),OUTPUT_URL,OUTPUT_FOLDER)
        return  {"error":"","queryfilters":app["config"]["queryfilters"]}


#---------------------------------------------------------------------------
# API generic search
#---------------------------------------------------------------------------

genericSearchAPI = api.model('genericSearch_model', {
    'size': fields.String(description="The max size", required=True),
    'query': fields.String(description="The query.", required=True)
})
#{"size":200,"query":{"bool":{"must":[{"match_all":{}}]}}}

@name_space.route('/pg_search/<string:appid>')
class genericSearchPG(Resource):
    @token_required()
    @api.doc(description="Execute the search from a sql app.",params={'token': 'A valid token'})
    @api.expect(genericSearchAPI)
    def post(self,appid,user=None):
        global es
        
        logger.info("PG Generic Search="+appid);    

        data= json.loads(request.data.decode("utf-8"))           
        return loadPGData(es,appid,get_postgres_connection(),conn,data,(request.args.get("download","0")=="1")
                    ,True,user,request.args.get("output","csv"),OUTPUT_URL,OUTPUT_FOLDER)

@name_space.route('/generic_search/<string:index>')
class genericSearch(Resource):
    @token_required()
    @api.doc(description="Generic search a database collection.",params={'token': 'A valid token'})
    @api.expect(genericSearchAPI)
    def post(self,index,user=None):
        global es
        
        logger.info("Generic Search="+index);    

        data= json.loads(request.data.decode("utf-8"))           
        cui=can_use_indice(index,user,data.get("query",None))
        if not cui[0]:
            logger.info("Index Not Allowed for user.")
            return {'error':"Not Allowed","records":[],"aggs":[]}

        logger.info("Must be filtered:"+str(cui[2]))
        
        data["query"]=cui[1]

        return loadData(es,conn,index,data,request.args.get("doc_type","doc"),(request.args.get("download","0")=="1"),cui
                ,True,user,request.args.get("output","csv"),OUTPUT_URL,OUTPUT_FOLDER)

#---------------------------------------------------------------------------
# API extLoadDataSource
#---------------------------------------------------------------------------
@name_space.route('/datasource/<string:dsid>')
@api.doc(description="DataSource.",params={'token': 'A valid token','start':'Start Time','end':'End Time'})
class extLoadDataSource(Resource):    
    @token_required()
    def get(self,dsid,start=None,end=None,user=None):
        start=request.args.get("start",None)
        end=request.args.get("end",None)
        logger.info("Data source called "+dsid+" start:"+str(start)+" end:"+str(end))

        if elkversion==7:
            ds=es.get(index="nyx_datasource",id=dsid)
        else:
            ds=es.get(index="nyx_datasource",doc_type="doc",id=dsid)
            

        logger.info("QUERY TYPE# "*20)
        query=ds["_source"]["query"]
        querytype=ds["_source"].get("type","elasticsearch")
        logger.info(querytype)
        
        if start !=None:
            query=query.replace("@START@",start)
        if end !=None:
            query=query.replace("@END@",end)

        logger.info("Final Query:"+query)


        if querytype=="postgres":
            recs=[]
            with get_postgres_connection().cursor() as cursor:
                cursor.execute(query)
                recs=cursor.fetchall()
                logger.info(recs)
            encoder = DateTimeEncoder()
            return {"error":"","records":json.loads(encoder.encode(recs))}

        else:
            r = requests.post('http://esnodebal:9200/_opendistro/_sql',json={"query":query})            
            records=json.loads(r.text)            
            if "schema" in records:
                results=[]
                cols=[]
                for col in records["schema"]:
                    if "alias" in col:
                        cols.append(col["alias"])
                    else:
                        cols.append(col["name"])

                for rec in records["datarows"]:
                    #for col in 
                    obj={}
                    for i,col in enumerate(cols):
                        obj[col]=rec[i]
                    
                    results.append(obj)
                
                return {"error":"","records":results}


                
            newrecords=[]
            if "aggregations" in records:
                aggs=records["aggregations"]
                for key in aggs:        
                    for rec in aggs[key]["buckets"]:
                        newrec={"key":rec["key"]}
                        for key2 in rec:
                            if type(rec[key2]) is dict:
                                if "value" in rec[key2]:
                                    newrec[key2]=rec[key2]["value"]
                        newrecords.append(newrec)
                    break
            else: #HITS 
                if "hits" in records and "hits" in records["hits"]:
                    for rec in records["hits"]["hits"]:
                        rec["_source"]["_id"]=rec["_id"]
                        newrecords.append(rec["_source"])
                    
            recjson=pd.DataFrame(newrecords).to_json(orient="records")

            return {"error":"","records":json.loads(recjson)}

#---------------------------------------------------------------------------
# API Kibana Load
#---------------------------------------------------------------------------
@app.route('/api/v1/kibana_load',methods=['POST'])
@token_required()
def kibanaLoad(user=None):
    global es
    
    logger.info("Kibana Load")


    outputformat=request.args.get("output","csv")
    logger.info("Output:"+outputformat)  

    token=request.args.get('token')

    logger.info("Full Key:"+"nyx_kib_msearch"+token)
    matchrequest=redisserver.get("nyx_kib_msearch"+token).decode('utf-8')
    logger.info(matchrequest)
    return kibanaData(es,conn,matchrequest,user,outputformat,True,OUTPUT_URL,OUTPUT_FOLDER)

#---------------------------------------------------------------------------
# API generic crud
#---------------------------------------------------------------------------
@app.route('/api/v1/pg_generic/<index>/<col>/<pkey>',methods=['GET','POST','DELETE'])
@token_required()
def pg_genericCRUD(index,col,pkey,user=None):
    global es,pg_connection

    met=request.method.lower()
    logger.info("PG Generic Table="+index+" Col:"+col+" Pkey:"+ pkey+" Method:"+met);    

    if met== 'get':   
        query="select * from "+index+ " where "+col+"="+str(pkey)

        description=None
        with get_postgres_connection().cursor() as cursor:
            cursor.execute(query)
            res=cursor.fetchone()
            description=[{"col":x[0],"type":x[1]} for x in cursor.description]
            
            res2={}
            for index,x in enumerate(cursor.description):
                if x[1] in [1082,1184,1114]:
                    print(res[index])
                    res2[x[0]]=res[index].isoformat()
                else:
                    res2[x[0]]=res[index]
                res2[x[0]+"_$type"]=x[1]
                
        pg_connection.commit()
        return {'error':"","data":res2,"columns":description}
    elif met== 'post':
        data= request.data.decode("utf-8")        
        logger.info("CREATE/UPDATE RECORD")
        logger.info(data)
        data=json.loads(data)
        
        if pkey!="NEW":
            query="UPDATE "+index+" set "
            cols=",".join([""+str(_["key"])+"='"+str(_["value"])+"' " for _ in data["record"]])
            query+=cols

            query+=" where "+col+"="+str(pkey)
            logger.info(query)
            with get_postgres_connection().cursor() as cursor:
                res=cursor.execute(query)
                logger.info(res)
            pg_connection.commit()
        else:
            query="INSERT INTO "+index+"  "
            cols=",".join([""+str(_["key"])+"" for _ in data["record"]])
            query+="("+cols+") VALUES ("
            vals=",".join(["'"+str(_["value"])+"'" for _ in data["record"]])
            query+=vals+")"
            logger.info("COUCOUC"*40)
            logger.info(query)
            with get_postgres_connection().cursor() as cursor:
                res=cursor.execute(query)
                logger.info(res)
            pg_connection.commit()


        return {'error':""}
    elif met== 'delete':
        try:
            with pg_connection.cursor() as cursor:
                query="delete from "+index+ " where "+col+"="+str(pkey)
                cursor.execute(query)
                res=cursor.fetchone()
                logger.info(res)

            pg_connection.commit()
            pass
        except:
            logger.error("Unable to delete record.",exc_info=True)
            ret=None
            return {'error':"unalbe to delete record"}

        return {'error':""}


#---------------------------------------------------------------------------
# API generic crud
#---------------------------------------------------------------------------
@app.route('/api/v1/generic/<index>/<object>',methods=['GET','POST','DELETE'])
@token_required()
def genericCRUD(index,object,user=None):
    global es,elkversion
    data = None

    met=request.method.lower()
    logger.info("Generic Index="+index+" Object:"+object+" Method:"+met);    
               
    cui=can_use_indice(index,user,None)
    if not cui[0]:
        logger.info("Index Not Allowed for user.")
        return {'error':"Not Allowed","records":[],"aggs":[]}

    if met== 'get':        
        try:
            if elkversion==7:
                ret=es.get(index=index,id=object)
            else:
                ret=es.get(index=index,id=object,doc_type=request.args.get("doc_type","doc"))
        except:
            return {'error':"unable to get data","data":None}
        return {'error':"","data":ret}
    elif met== 'post':
        try:
            data= request.data.decode("utf-8")        
            if index=="nyx_user":
                dataobj=json.loads(data)
                if("$pbkdf2-sha256" not in dataobj["password"]):
                    dataobj["password"]=pbkdf2_sha256.hash(dataobj["password"])
                    data=json.dumps(dataobj)
            if elkversion==7:
                es.index(index=index,body=data,id=object)
            else:
                es.index(index=index,body=data,doc_type=request.args.get("doc_type","doc"),id=object)
        except:
            logger.error("unable to post data",exc_info=True)
            return {'error':"unable to post data"}

    elif met== 'delete':
        try:
            if elkversion==7:
                ret=es.delete(index=index,id=object)
            else:
                ret=es.delete(index=index,id=object,doc_type=request.args.get("doc_type","doc"))
            logger.info(ret)
        except:
            return {'error':"unable to delete data"}


    send_event(user=user, indice=index, method=met, _id=object, doc_type=request.args.get("doc_type","doc"), obj=data)


    return {'error':""}


def send_event(user, indice, method, _id, doc_type=None, obj=None):    
    notif_dest = None
    
    global indices
    for ind in indices:
        pat=ind["_source"]["indicepattern"]
        
        if re.search(pat, indice)!=None:
            tmp = ind["_source"].get('notifications')
            if tmp is not None and tmp != '':
                notif_dest=tmp
                break
    
    if notif_dest is not None:
        obj_to_send = {
            'user': user,
            'method': method,
            'indice': indice,
            'id': _id,
        }
        
        if doc_type is not None:
            obj_to_send['doc_type'] = doc_type
        
        if obj is not None:
            obj_to_send['obj'] = obj
            
        print(obj_to_send)
        conn.send_message(notif_dest, json.dumps(obj_to_send))  
    
    else:
        print('no notif to send')


def handleAPICalls():
    global es,userActivities,conn,elkversion
    while True:
        try:
            logger.info("APIs history")
            elkversion=getELKVersion(es)
            with userlock:
                apis=userActivities[:]
                userActivities=[]
                if(len(apis)>0):
                    messagebody=""
                    indexdatepattern="nyx_apicalls-"+datetime.now().strftime("%Y.%m.%d").lower()
                    for api in apis:
                        action={}
                        if elkversion==7:
                            action["index"]={"_index":indexdatepattern}
                        else:
                            action["index"]={"_index":indexdatepattern,"_type":"doc"}

                        messagebody+=json.dumps(action)+"\r\n"
                        messagebody+=json.dumps(api)+"\r\n"
                    es.bulk(messagebody)
            if conn != None:
                logger.debug("Sending Life Sign")
                conn.send_life_sign()
                logger.debug("Sleeping")
        except Exception as e:
            logger.error("Unable to send life sign or api history.")
            logger.error(e)


        time.sleep(5)



#---------------------------------------------------------------------------
# API extLoadDataSource
#---------------------------------------------------------------------------
@name_space.route('/esmapping/<string:index_pattern>')
@api.doc(description="Get ES mapping based on an index pattern.",params={'token': 'A valid token'})
class esMapping(Resource):    
    @token_required()
    def get(self,index_pattern='*', user=None):
        global es
        logger.info('get ES mapping')
        try:
            mappings=es.indices.get_mapping(index=index_pattern)
            mappings=[{"id":x,"obj":mappings[x]} for x in mappings if not x.startswith('.')]
            mappings.sort(key=operator.itemgetter('id'))
            return {"error":"","data":mappings}
        except elasticsearch.NotFoundError:
            return {"error":"","data":None}



#---------------------------------------------------------------------------
# refresh_indices
#---------------------------------------------------------------------------
def refresh_indices():
    global indices,last_indices_refresh,indices_refresh_seconds
    if last_indices_refresh+timedelta(seconds=indices_refresh_seconds)>datetime.now():
        return
    logger.info("Refresh Indices")    
    if elkversion==7:
        indices=es.search(index="nyx_indice",body={})["hits"]["hits"]
    else:
        indices=es.search(index="nyx_indice",body={},doc_type="doc")["hits"]["hits"]
    last_indices_refresh=datetime.now()

#---------------------------------------------------------------------------
# refresh_translations
#---------------------------------------------------------------------------
def refresh_translations():
    global translations,last_translation_refresh,last_translation_refresh_seconds, es
    if last_translation_refresh+timedelta(seconds=last_translation_refresh_seconds)>datetime.now():
        return
    logger.info("Refreshing Translations")    
    if elkversion==7:
        translationsrec=es.search(index="nyx_translation",body={"size":1000})["hits"]["hits"]
    else:
        translationsrec=es.search(index="nyx_translation",body={"size":1000},doc_type="doc")["hits"]["hits"]

    for tran in translationsrec:    
        source=tran["_source"]
        for key in source:
            if len(key)==2:
                if key not in translations:
                    translations[key]={}
                
                if source["area"] not in translations[key]:
                    translations[key][source["area"]]={}
                
                translations[key][source["area"]][source["item"]]=source[key]

    logger.info(translations)    
    last_translation_refresh=datetime.now()

def get_translated_item(language,area,item):
    global translations
    if area not in translations[language]:
        return item

    if item not in translations[language][area]:
        return item
    
    return translations[language][area][item]
    
#---------------------------------------------------------------------------
# can_use_indice
#---------------------------------------------------------------------------
def can_use_indice(indice,user,query):            
    global indices 
    refresh_indices()
    
    if query==None:
        query={
            "bool": {
              "must": [
                {
              "query_string": {
                "query": "*",
                "analyze_wildcard": True,
                "default_field": "*"
              }
            }]
              }}
    logger.info(query)
    queryindex=-1
    for index,que in enumerate(query["bool"]["must"]):
        if "query_string" in que:
            queryindex=index
            oldquery=que["query_string"]["query"]
            break

    if queryindex==-1:
        query["bool"]["must"].insert(0,{
              "query_string": {
                "query": "*",
                "analyze_wildcard": True,
                "default_field": "*"
              }
            })
        oldquery=""
        queryindex=0
    
    resultsmustbefiltered=None

    for ind in indices:
        pat=ind["_source"]["indicepattern"]

        # Check if a column is used to filter the results
        if re.search(pat, indice) !=None and "privilegecolumn" in ind["_source"] and ind["_source"]["privilegecolumn"]!="":
            resultsmustbefiltered=ind["_source"]["privilegecolumn"]

        # Check if a privilege is required to access the collection
        if re.search(pat, indice) !=None and "privileges" in ind["_source"] and ind["_source"]["privileges"]!="":
            if len([value for value in user["privileges"] if value in ind["_source"]["privileges"]])==0:
                logger.info("Not allowed")
                return (False,query,resultsmustbefiltered)

        if re.search(pat, indice) !=None:
            if "filtercolumn" in ind["_source"] and ind["_source"]["filtercolumn"]!="":
                if "filters" in user and len (user["filters"])>0:
                    newquery= " OR ".join([ind["_source"]["filtercolumn"]+":"+x for x in user["filters"]])
                    if len(oldquery)==0:
                        query["bool"]["must"][queryindex]["query_string"]["query"]=newquery
                        return (True,query,resultsmustbefiltered)
                    else:
                        query["bool"]["must"][queryindex]["query_string"]["query"]=oldquery +" AND ("+newquery+")"
                        return (True,query,resultsmustbefiltered)
                        
                else:
                    return (True,query,resultsmustbefiltered)                        
            else:
                return (True,query,resultsmustbefiltered)
    
    return (True,query,resultsmustbefiltered)


#---------------------------------------------------------------------------
# compute kibana url
#---------------------------------------------------------------------------
def compute_kibana_url(dashboard_dict, appl):
    if appl.get('config').get('kibanaId') is None:
        return appl.get('config').get('url')

    url = "/dashboard/" + appl.get('config')['kibanaId'] + ""

    time = "from:now-7d,mode:quick,to:now"

    if appl.get('config').get('kibanaTime') is not None:
        time = appl.get('config').get('kibanaTime')

    refresh = "refreshInterval:(pause:!t,value:0)"

    if appl.get('timeRefresh') and appl.get('timeRefreshValue'):
        if 'refreshInterval' in appl.get('timeRefreshValue'): # to handle the transition, we want to keep only the else part
            refresh = appl.get('timeRefreshValue')
        else: 
            refresh = 'refreshInterval:(pause:!f,value:'+str(appl.get('timeRefreshValue'))+')'

    try:
        dash = dashboard_dict[appl.get('config').get('kibanaId')]
    except:
        logger.error("Unable to compute kibana URL")
        logger.error(appl.get('config'))
        return 'INVALIDURL'

    dash_obj = dash.get('_source').get('dashboard')

    url += "?embed=true&_g=("+refresh+",time:(" + time +"))"
    url += "&_a=(description:'" + dash_obj.get('description') + "'"
    url += ",filters:!(),fullScreenMode:!f"  

    if dash_obj.get('optionsJSON'):
        options = json.loads(dash_obj.get('optionsJSON'))

        # {'darkTheme': False, 'hidePanelTitles': False, 'useMargins': True} => 'darkTheme:!f,hidePanelTitles:!f,useMargins:!t'
        url_options = ','.join([str(k)+':'+str(v) for k, v in options.items()]).replace('True', '!t').replace('False', '!f')
        url += ",options:(" + url_options + ")"
    else:
        url += ",options:()"

    panels = []
    panels_json = json.loads(dash_obj.get('panelsJSON'))

    for pan in panels_json:
        if dash.get('_source').get('migrationVersion') and \
           dash.get('_source').get('migrationVersion').get('dashboard') in ['7.0.0','7.1.0','7.2.0','7.3.0']:
            for ref in dash.get('_source').get('references'):
                if ref.get('name')==pan.get('panelRefName') :
                    pan['id']=ref.get('id')  
                    pan['type']=ref.get('type')  

        if pan is not None and pan.get("embeddableConfig") is not None and pan["embeddableConfig"].get("colors") is not None:
            newcols={}
            for colkey in pan["embeddableConfig"]["colors"]:
                newcols[colkey.replace("%","%25").replace(" ","%20")]=pan["embeddableConfig"]["colors"][colkey]
            pan["embeddableConfig"]["colors"]=newcols


        if pan is not None and pan.get("embeddableConfig") is not None and pan["embeddableConfig"].get( "vis") is not None and pan["embeddableConfig"]["vis"].get("colors") is not None:
            newcols={}
            for colkey in pan["embeddableConfig"]["vis"]["colors"]:
                newcols[colkey.replace("%","%25").replace(" ","%20")]=pan["embeddableConfig"]["vis"]["colors"][colkey]
            pan["embeddableConfig"]["vis"]["colors"]=newcols


        panels.append(prison.dumps(pan))

    url += ",panels:!(" + ','.join(panels).replace('#', "%23").replace('&', "%26") + ")"

    query = "query:(language:lucene,query:'*')"

    if dash_obj.get('kibanaSavedObjectMeta') and dash_obj.get('kibanaSavedObjectMeta').get('searchSourceJSON'):
        query_2 = json.loads(dash_obj.get('kibanaSavedObjectMeta').get('searchSourceJSON'))

        if query_2.get('query'):
            query = 'query:'+prison.dumps(query_2.get('query'))

    url += "," + query + ",timeRestore:!f,title:Test,viewMode:view)";  

    space = ''
    if dash.get('_source').get('namespace') and dash.get('_source').get('namespace') != 'default':
        space = 's/' + dash.get('_source').get('namespace')

    return ('./kibananyx/'+space+"/app/kibana#"+url)

#---------------------------------------------------------------------------
# get dictionary of dashboards
#---------------------------------------------------------------------------
def get_dict_dashboards(es):
    query={
        "query": {
            "bool": {
                "must": [
                        {
                        "query_string": {
                            "query": "type: dashboard"
                        }
                    }
                ]
            }
        }
    }

    res=es.search(index=".kibana", body=query, size=10000)
    return {dash['_id'].split(':')[-1]: dash for dash in res['hits']['hits']}


#=============================================================================

def messageReceived(destination,message,headers):
    logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    logger.info("Message Received:"+destination)

    if "LOGOUT_EVENT" in destination:
        if message in tokens:
            del tokens[message]
    else:
        logger.error("Unknown destination %s" %(destination))

#>> AMQC
server={"ip":os.environ["AMQC_URL"],"port":os.environ["AMQC_PORT"]
                ,"login":os.environ["AMQC_LOGIN"],"password":os.environ["AMQC_PASSWORD"]}
#logger.info(server)                
conn=amqstompclient.AMQClient(server
    , {"name":MODULE,"version":VERSION,"lifesign":"/topic/NYX_MODULE_INFO"},['/topic/LOGOUT_EVENT'],callback=messageReceived)
#conn,listener= amqHelper.init_amq_connection(activemq_address, activemq_port, activemq_user,activemq_password, "RestAPI",VERSION,messageReceived)
connectionparameters={"conn":conn}

#>> ELK

es=None
logger.info (os.environ["ELK_SSL"])

if os.environ["ELK_SSL"]=="true":
    host_params = {'host':os.environ["ELK_URL"], 'port':int(os.environ["ELK_PORT"]), 'use_ssl':True}
    es = ES([host_params], connection_class=RC, http_auth=(os.environ["ELK_LOGIN"], os.environ["ELK_PASSWORD"]),  use_ssl=True ,verify_certs=False)
else:
    host_params="http://"+os.environ["ELK_URL"]+":"+os.environ["ELK_PORT"]
    es = ES(hosts=[host_params])


#>> THREAD
thread = threading.Thread(target = handleAPICalls)
thread.start()


elkversion=getELKVersion(es)
refresh_translations()

logger.info("Scanning files in lib...")
logger.info("========================")
try:
    for ext_lib in os.listdir("lib"):   
        if ".py" in  ext_lib and "ext" in ext_lib:
            logger.info("Importing 2:"+ext_lib) 
            logger.info("lib."+ext_lib.replace(".py","")) 

            module = importlib.import_module("lib."+ext_lib.replace(".py",""))
            module.config(api,conn,es,redisserver,token_required)
except:
    logger.info('no lib directory found')

if __name__ != '__main__':
    gunicorn_logger = logging.getLogger("gunicorn.error")
    logger.handlers = gunicorn_logger.handlers
    logger.setLevel(gunicorn_logger.level)
    if lshandler != None:
        logger.info("ADDING LOGSTASH HANDLER")
        gunicorn_logger.addHandler(lshandler)

if __name__ == '__main__':    
    logger.info("AMQC_URL          :"+os.environ["AMQC_URL"])
    app.run(threaded=False,host= '0.0.0.0')