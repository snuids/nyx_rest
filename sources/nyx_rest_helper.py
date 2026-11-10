import json
import re
import time
import uuid
import base64
import smtplib
import zipfile
import datetime
import psycopg2
import threading
import subprocess 
import traceback
import os,logging,sys
from email import encoders
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase

from logging.handlers import TimedRotatingFileHandler
from amqstompclient import amqstompclient
from datetime import datetime
from functools import wraps
#from elasticsearch import Elasticsearch as ES, RequestsHttpConnection as RC
from opensearchpy import OpenSearch as ES, RequestsHttpConnection as RC
from logstash_async.handler import AsynchronousLogstashHandler

from common import loadData ,kibanaData
from lib.export_authz import kibana_indices, load_rules, may_export
from pg_common import loadPGData
import state
from config import settings

# One version, one release: this worker ships with the API image of the same tag (see
# Dockerfilehelper), so the version it announces on the broker is the API's.
VERSION=state.VERSION
MODULE="NYX_Helper"
QUEUE=["/queue/REST_LOAD_DATA","/queue/REST_LOAD_PGDATA","/queue/REST_LOAD_KIBANA"]

pg_connection=None
pg_thread=None

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
            discard_pg_connection()


def discard_pg_connection():
    """Drop the shared connection so the next message reconnects instead of failing."""
    global pg_connection
    conn = pg_connection
    pg_connection = None
    if conn is not None:
        try:
            conn.close()
        except Exception:
            logger.warning("Unable to close the PostgreSQL connection", exc_info=True)


def rollback_pg_connection(conn):
    """Undo the failed statement: one connection serves every message of the worker."""
    if conn is None:
        return
    try:
        conn.rollback()
    except Exception:
        logger.warning("Unable to roll back the failed statement", exc_info=True)
        discard_pg_connection()



def get_postgres_connection():
    global pg_connection,pg_thread
    logger.info(">>> Create PG Connection")
    if pg_connection!=None:
        return pg_connection
    try:
        pg_connection = psycopg2.connect(user = settings.PG_LOGIN,
                                    password = settings.PG_PASSWORD,
                                    host = settings.PG_HOST,
                                    port = settings.PG_PORT,
                                    database = settings.PG_DATABASE)
        # Same reason as the API: a statement that fails must not leave the transaction
        # aborted for the next message of this worker, and the connection must not sit
        # `idle in transaction` between two exports.
        pg_connection.autocommit = True
        cursor = pg_connection.cursor()
        # Print PostgreSQL Connection properties (never the password)
        dsn_params = pg_connection.get_dsn_parameters()
        dsn_params.pop("password", None)
        logger.info(dsn_params)
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
        # `logger.error("...", error)` passes `error` as a formatting argument with no `%s` in the
        # message: logging raises TypeError there, so a database that refuses the connection ended
        # up escaping this function instead of returning None. `exc_info` keeps the trace.
        logger.error("Error while connecting to PostgreSQL: %s", error, exc_info=True)
    return None

################################################################################
def account_behind(mes):
    """The `nyx_user` record behind an export request, or `None` when it must be dropped.

    Two things come from here. The rights: the API checks `can_use_indice` on every read, but this
    worker reads Elasticsearch directly, so it re-applies the rule itself. And the recipient: the
    mail used to go to `mes["user"]["id"]`, an address the message chose, which turned any export
    into a way of sending an index to a mailbox of the caller's choosing.
    """
    global es
    user_id = (mes.get("user") or {}).get("id")
    if not user_id:
        logger.error("Export request without an account, dropped")
        return None
    if es is None:
        logger.error("No cluster client, export dropped")
        return None
    try:
        record = es.get(index="nyx_user", id=user_id)
    except Exception:
        logger.error("Unknown account %s, export dropped", user_id)
        return None
    user = dict(record.get("_source") or {})
    user["id"] = record.get("_id", user_id)
    return user


def messageReceived(destination,message,headers):
    global es
    logger.debug("==> "*10)
    logger.debug("Message Received %s" % destination)
    logger.debug(message)

    if "LOAD_DATA" in destination:
        mes=json.loads(message)
        user=account_behind(mes)
        if user is None or not may_export(mes.get("index"), user, load_rules(es)):
            logger.error("LOAD_DATA refused: %s may not export %s",
                         (mes.get("user") or {}).get("id"), mes.get("index"))
            return
        mes["user"]=user
        res=remoteLoadData(mes)
        sendMail(res,mes)

    if "LOAD_PGDATA" in destination:
        mes=json.loads(message)
        user=account_behind(mes)
        if user is None:
            logger.error("LOAD_PGDATA refused: no known account")
            return
        # A PostgreSQL export has no privilege model to re-apply here (the tables are declared per
        # application, and `pg_search` does not check them either): the account is authenticated,
        # the recipient is the real one, and that is all this path can promise for now.
        mes["user"]=user
        res=remoteLoadPGData(mes)
        sendMail(res,mes)

    if "LOAD_KIBANA" in destination:
        mes=json.loads(message)
        user=account_behind(mes)
        rules=load_rules(es)
        indices=kibana_indices(mes.get("matchrequest"))
        if user is None or rules is None or not indices:
            logger.error("LOAD_KIBANA refused: unknown account, unreadable rules or no index")
            return
        allowed=all(may_export(indice, user, rules) for indice in indices)
        if not allowed:
            logger.error("LOAD_KIBANA refused: %s may not export %s", user.get("id"), indices)
            return
        mes["user"]=user
        res=remoteKibanaData(mes)
        res["total"]=res["records"]
        res["file"]="./outputs/"+res["fileName"]
        res["took"]=res["timing"]["load"]+res["timing"]["export"]+res["timing"]["process"]
        #res=remoteLoadData(mes)
        sendMail(res,mes)

    logger.debug("<== "*10)

################################################################################
# Send Mail
################################################################################

def sendMail(task,mes):
    global SMTP_ADDRESS,SMTP_USER,SMTP_PASSWORD
    server = None
    zip_filename = None
    
    try:
        logger.info("Sending mail:<"+SMTP_ADDRESS+"> <"+SMTP_USER+"> <hidden> Port<"+str(SMTP_PORT)+">")
        logger.info("Recipient: "+mes["user"]["id"])
        
        if settings.SMTP_SSL:
            logger.info("OPENING SERVER SSL")
            server = smtplib.SMTP_SSL(SMTP_ADDRESS, SMTP_PORT)
            logger.info("SERVER OPENED")    
            server.ehlo()
        else:
            logger.info("OPENING SERVER")
            server = smtplib.SMTP(SMTP_ADDRESS, SMTP_PORT)
            logger.info("SERVER OPENED")  

        if settings.SMTP_TLS:
            logger.info("START TLS")
            server.starttls()

        logger.info("SERVER OPENED2")
        if len(SMTP_USER)>0:
            server.login(SMTP_USER, SMTP_PASSWORD)

        logger.info("SERVER OPENED3")

        msg = MIMEMultipart()
        msg['Subject'] = "Data From NYX"
        msg['From'] = SMTP_FROM
        msg['To'] = mes["user"]["id"]
        msg.preamble = 'Data From Nyx'
        text="""Dear USER

Please find attached your data:

- Records: RECORDS
- Export Time: EXPORT (ms)

Regards
"""
        text=text.replace("USER",mes["user"]["firstname"]+" "+mes["user"]["lastname"])
        text=text.replace("RECORDS",str(task["total"]))
        text=text.replace("EXPORT",str(task["took"]))
        
        logger.info(text)
        msg.attach(MIMEText(text))

        # Create unique zip filename to avoid conflicts
        zip_filename = "report_" + str(uuid.uuid4()) + ".zip"
        logger.info("Zipping file: " + zip_filename)
        
        if not os.path.exists(task["file"]):
            logger.error("Source file does not exist: " + task["file"])
            raise FileNotFoundError("Source file not found: " + task["file"])
        
        file_zip = zipfile.ZipFile(zip_filename, 'w')
        file_zip.write(task["file"], compress_type=zipfile.ZIP_DEFLATED)
        file_zip.close()

        part = MIMEBase('application', "octet-stream")
        part.set_payload(open(zip_filename, "rb").read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment; filename="'+"report"+".zip"+'"')
        msg.attach(part)

        res=server.send_message(msg)
        logger.info("Mail sent to: "+mes["user"]["id"])
        logger.info("Send result: "+str(res))
        
    except Exception as e:
        logger.error("Failed to send email to: "+mes["user"]["id"], exc_info=True)
        raise
    finally:
        # Clean up zip file
        if zip_filename and os.path.exists(zip_filename):
            try:
                os.remove(zip_filename)
                logger.info("Cleaned up temporary zip file: " + zip_filename)
            except Exception as e:
                logger.error("Failed to remove zip file: " + zip_filename, exc_info=True)
        
        # Close SMTP connection
        if server:
            try:
                server.quit()
                logger.info("SMTP connection closed")
            except Exception as e:
                logger.error("Failed to close SMTP connection", exc_info=True)

def remoteLoadData(message):
    logger.info("=== "*10)
    logger.info(message)
    return loadData(es,conn,message["index"],message["data"],message["doc_type"],message["download"]
        ,message["cui"],False,message["user"],message["outputformat"],OUTPUT_URL,OUTPUT_FOLDER)

def remoteLoadPGData(message):
    logger.info("=== "*10)
    logger.info(message)

    try:
        return loadPGData(es,message["appid"],get_postgres_connection(),conn,message["data"],message["download"]
                    ,False,message["user"],message["outputformat"],OUTPUT_URL,OUTPUT_FOLDER)
    except Exception:
        # Without this the failed statement leaves the shared connection's transaction
        # aborted and every later export of this worker dies for an unrelated reason.
        logger.error("Unable to export the SQL app " + str(message.get("appid")), exc_info=True)
        rollback_pg_connection(pg_connection)
        raise

def remoteKibanaData(message):
    logger.info("=== "*10)
    logger.info(message)
    #(es,conn,matchrequest,user,outputformat,is_rest_api,OUTPUT_URL,OUTPUT_FOLDER):
    return kibanaData(es,conn,message["matchrequest"],message["user"],message["outputformat"],False,OUTPUT_URL,OUTPUT_FOLDER)

logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(module)s - %(funcName)s: %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger()



lshandler=None

if settings.USE_LOGSTASH:
    logger.info ("Adding logstash appender")
    lshandler=AsynchronousLogstashHandler("logstash", 5001, database_path='logstash_test.db')
    lshandler.setLevel(logging.ERROR)
    logger.addHandler(lshandler)

handler = TimedRotatingFileHandler("logs/"+MODULE+".log",
                                when="d",
                                interval=1,
                                backupCount=30)

logFormatter = logging.Formatter('%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s')
handler.setFormatter( logFormatter )
logger.addHandler(handler)

logger.info("==============================")
logger.info("Starting: %s" % MODULE)
logger.info("Module:   %s" %(VERSION))
logger.info("==============================")

OUTPUT_FOLDER=settings.OUTPUT_FOLDER
OUTPUT_URL=settings.OUTPUT_URL

SMTP_ADDRESS=settings.SMTP_ADDRESS
SMTP_USER=settings.SMTP_USER
SMTP_PASSWORD=settings.SMTP_PASSWORD
SMTP_FROM=settings.SMTP_FROM
SMTP_PORT=settings.SMTP_PORT

#>> AMQC
server={"ip":settings.AMQC_URL,"port":settings.AMQC_PORT
                ,"login":settings.AMQC_LOGIN,"password":settings.AMQC_PASSWORD
                ,"heartbeats":(120000,120000),"earlyack":True}
logger.info("AMQC server %s:%s (login=%s)" %(settings.AMQC_URL, settings.AMQC_PORT, settings.AMQC_LOGIN))
conn=amqstompclient.AMQClient(server
    , {"name":MODULE,"version":VERSION,"lifesign":"/topic/NYX_MODULE_INFO"},QUEUE,callback=messageReceived)
#conn,listener= amqHelper.init_amq_connection(activemq_address, activemq_port, activemq_user,activemq_password, "RestAPI",VERSION,messageReceived)
connectionparameters={"conn":conn}

#>> ELK
es=None
logger.info(settings.ELK_SSL)

if settings.ELK_SSL:
    elk_url = settings.ELK_URL
    if elk_url.startswith('http://') or elk_url.startswith('https://'):
        host_params = elk_url
    else:
        host_params = f"https://{elk_url}:{settings.ELK_PORT}"
    logger.info("ELK Host Params:"+str(host_params))
    es = ES([host_params], connection_class=RC, http_auth=(settings.ELK_LOGIN, settings.ELK_PASSWORD),  use_ssl=True ,verify_certs=False)
else:
    host_params=f"http://{settings.ELK_URL}:{settings.ELK_PORT}"
    es = ES(hosts=[host_params])


if __name__ == '__main__':    
    logger.info(f"AMQC_URL          :{settings.AMQC_URL}")
    while True:
        time.sleep(5)
        try:            
            conn.send_life_sign()
        except Exception as e:
            logger.error("Unable to send life sign.")
            logger.error(e)
    #app.run(threaded=True,host= '0.0.0.0')