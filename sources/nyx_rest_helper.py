import json
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
from elasticsearch import Elasticsearch as ES, RequestsHttpConnection as RC
from logstash_async.handler import AsynchronousLogstashHandler

from common import loadData ,kibanaData
from pg_common import loadPGData

VERSION="0.1.0"
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
            pg_connection=None
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

################################################################################
def messageReceived(destination,message,headers):
    global es
    logger.debug("==> "*10)
    logger.debug("Message Received %s" % destination)
    logger.debug(message)
    
    if "LOAD_DATA" in destination:
        mes=json.loads(message)
        # if "data" in mes and "size" in mes["data"]:
        #     logger.info("Update Size to 50000")
        #     mes["data"]["size"]=50000
        res=remoteLoadData(mes)
        sendMail(res,mes)

    if "LOAD_PGDATA" in destination:
        mes=json.loads(message)
        # if "data" in mes and "size" in mes["data"]:
        #     logger.info("Update Size to 50000")
        #     mes["data"]["size"]=50000
        res=remoteLoadPGData(mes)
        sendMail(res,mes)        

    if "LOAD_KIBANA" in destination:
        mes=json.loads(message)
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
    logger.info("Sending mail:<"+SMTP_ADDRESS+"> <"+SMTP_USER+"> <"+len(SMTP_PASSWORD)*"*"+"> Port<"+str(SMTP_PORT)+">")
    #logger.info(task)
    #logger.info(mes)
    
    print(os.environ["SMTP_TLS"])

    if os.environ["SMTP_SSL"]=="true":
        logger.info("OPENING SERVER SSL")
        server = smtplib.SMTP_SSL(SMTP_ADDRESS, SMTP_PORT)
        logger.info("SERVER OPENED")    
        server.ehlo()
    else:
        logger.info("OPENING SERVER")
        server = smtplib.SMTP(SMTP_ADDRESS, SMTP_PORT)
        logger.info("SERVER OPENED")  

    if os.environ["SMTP_TLS"]=="true":
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

    #extension=mes["outputformat"]

    logger.info("Zipping file")
    file_zip = zipfile.ZipFile("report.zip", 'w')
    file_zip.write(task["file"], compress_type=zipfile.ZIP_DEFLATED)
    file_zip.close()
    #extension=".zip"

    part = MIMEBase('application', "octet-stream")
    part.set_payload(open("report.zip", "rb").read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', 'attachment; filename="'+"report"+".zip"+'"')
    msg.attach(part)

    res=server.send_message( msg)
    logger.info("Mail sent to:"+mes["user"]["id"])
    logger.info(res)

def remoteLoadData(message):
    logger.info("=== "*10)
    logger.info(message)
    return loadData(es,conn,message["index"],message["data"],message["doc_type"],message["download"]
        ,message["cui"],False,message["user"],message["outputformat"],OUTPUT_URL,OUTPUT_FOLDER)

def remoteLoadPGData(message):
    logger.info("=== "*10)
    logger.info(message)

    return loadPGData(es,message["appid"],get_postgres_connection(),conn,message["data"],message["download"]
                ,False,message["user"],message["outputformat"],OUTPUT_URL,OUTPUT_FOLDER)

def remoteKibanaData(message):
    logger.info("=== "*10)
    logger.info(message)
    #(es,conn,matchrequest,user,outputformat,is_rest_api,OUTPUT_URL,OUTPUT_FOLDER):
    return kibanaData(es,conn,message["matchrequest"],message["user"],message["outputformat"],False,OUTPUT_URL,OUTPUT_FOLDER)

logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(module)s - %(funcName)s: %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger()



lshandler=None

if os.environ["USE_LOGSTASH"]=="true":
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

OUTPUT_FOLDER=os.environ["OUTPUT_FOLDER"]
OUTPUT_URL=os.environ["OUTPUT_URL"]

SMTP_ADDRESS=os.environ["SMTP_ADDRESS"]
SMTP_USER=os.environ["SMTP_USER"]
SMTP_PASSWORD=os.environ["SMTP_PASSWORD"]
SMTP_FROM=os.environ["SMTP_FROM"]
SMTP_PORT=int(os.environ["SMTP_PORT"])

#>> AMQC
server={"ip":os.environ["AMQC_URL"],"port":os.environ["AMQC_PORT"]
                ,"login":os.environ["AMQC_LOGIN"],"password":os.environ["AMQC_PASSWORD"]
                ,"heartbeats":(120000,120000),"earlyack":True}
logger.info(server)                
conn=amqstompclient.AMQClient(server
    , {"name":MODULE,"version":VERSION,"lifesign":"/topic/NYX_MODULE_INFO"},QUEUE,callback=messageReceived)
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


if __name__ == '__main__':    
    logger.info("AMQC_URL          :"+os.environ["AMQC_URL"])
    while True:
        time.sleep(5)
        try:            
            conn.send_life_sign()
        except Exception as e:
            logger.error("Unable to send life sign.")
            logger.error(e)
    #app.run(threaded=True,host= '0.0.0.0')