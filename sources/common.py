import json
import uuid
import pytz
import tzlocal
import logging
import datetime
import traceback
import pandas as pd
from datetime import datetime
from datetime import timedelta
from elasticsearch.client import IndicesClient

from cachetools import cached, LRUCache, TTLCache

@cached(cache=TTLCache(maxsize=1024, ttl=300))
def get_es_info(es):
    print('get_es_info')
    return es.info()


def getELKVersion(es):
    return int(get_es_info(es).get('version').get('number').split('.')[0])

def filterReports(res,user):
    logger=logging.getLogger()
    logger.info("Filter reports")

    if "admin" in user["privileges"]:
        return res
    return list(filter(lambda x:True if x["_source"]["creds"]["user"].get("user","NA")==user["id"] else False,res))

def filterReportDefs(res,user):
    logger=logging.getLogger()
    logger.info("Filter reports defs")

    if "admin" in user["privileges"]:
        return res

    newlist=[]

    for rep in res:
        
        rep2=rep["_source"].get("privileges",[])
        print(rep2)
        for rep_privilege in rep2:
            if rep_privilege in user["privileges"]:
                newlist.append(rep)
                break
    return newlist


def applyPrivileges(res,user,column):
    logger=logging.getLogger()
    logger.info("Apply privileges")
    logger.info(column)
    logger.info(user)
    
    if "admin" in user["privileges"]:
        return res
    
    privhash={}
    for priv in user["privileges"]:
        privhash[priv]=True

    res2=[]
    for rec in res:
        if column in rec["_source"]:
            for priv2 in  rec["_source"][column].split(','):
#                logger.info(priv2)
#                logger.info(privhash)
                if priv2 in privhash:
                    res2.append(rec)
                break

    return res2

def flattenJson(y):
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '$')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '#')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out

#################################################################################
def cleanElasticRecords(records):
    cleanrecs=[]
    if "hits" in records and "hits" in records["hits"]:
        for rec in records["hits"]["hits"]:
            newrec=rec["_source"]
            newrec["_id"]=rec["_id"]
            newrec["_index"]=rec["_index"]        
            cleanrecs.append(flattenJson(newrec))
    return cleanrecs

#################################################################################
def loadData(es,conn,index,data,doc_type,download,cui,is_rest_api,user,outputformat,OUTPUT_URL,OUTPUT_FOLDER):
    logger=logging.getLogger()
    elkversion=getELKVersion(es)

    logger.info("LOAD DATA index=%s doc_type=%s" %(index,doc_type))

    start=datetime.now().timestamp()

    extra=data.get("extra",None)
    if extra !=None:
        del data["extra"]
    
    if "size" in data:
        maxsize=data["size"]
        if maxsize==0:
            data["size"]=1
            maxsize=1

    else:
        maxsize=200

    logger.info (data)

    if elkversion==7:
        response = es.search(
            index=index,
            body=json.dumps(data),
            scroll='1m',
        )
    else:
        response = es.search(
            index=index,
            body=json.dumps(data),
            scroll='1m',doc_type=doc_type,
        )
    hits=[]
    aggs=[]
    scroll_ids=[]

    total=0

    if "_scroll_id" in response:
        scroll_ids.append(response['_scroll_id'])

    if "hits" in response and "total" in response["hits"]:
        if isinstance(response["hits"]["total"],dict):
            total=response["hits"]["total"]["value"]
        else:
            total=response["hits"]["total"]


    while len(response['hits']['hits']):
        if "hits" in response and "hits" in response["hits"]:  
            hits+=response["hits"]["hits"]
        if "aggregations" in response:
            aggs=response["aggregations"]

        if len(hits)>=maxsize and is_rest_api:
            break

        #print([item["_id"] for item in response["hits"]["hits"]])
        scroll_ids.append(response['_scroll_id'])
        response = es.scroll(scroll_id=response['_scroll_id'], scroll='1m')

    if index.find("nyx_reporttask")==0:
        hits=filterReports(hits,user)    
    if index.find("nyx_reportdef")==0:
        hits=filterReportDefs(hits,user)    


    if cui[2]!=None and cui[2]!="":
        hits=applyPrivileges(hits,user,cui[2])    

    res2=es.clear_scroll(body={'scroll_id': scroll_ids})
    
    if not download:
        return {'error':"","took":round(datetime.now().timestamp()-start,2),"total":total,"records":hits,"aggs":aggs}
    
    exportcolumns=None

    if len(hits)>5000 and is_rest_api:
        logger.info("Using Rest Helper Process...")
        if extra!=None:
            data["extra"]=extra

        conn.send_message("/queue/REST_LOAD_DATA"
            ,json.dumps({"index":index,"data":data,"doc_type":doc_type
                ,"download":download,"cui":cui,"is_rest_api":is_rest_api,"user":user,
                "outputformat":outputformat})
            )

        return {'error':"","took":round(datetime.now().timestamp()-start,2),"total":total,"type":"mail"}

    if extra != None:
        exportcolumns=extra.get("exportColumns",None)
        if exportcolumns !=None:
            exportcolumns=[x.strip() for x in exportcolumns.split(",")]
            logger.info(exportcolumns)

    cleanrecs=cleanElasticRecords({"hits":{"hits":hits}})
    
    outputname="export_"+str(uuid.uuid4())+'.'+outputformat
    url=OUTPUT_URL+outputname

    df=pd.DataFrame(cleanrecs)

    datescol={}

    cols=set([])
    try:
        mappings=es.indices.get_mapping(index=index)
        for key in mappings:
            if "mappings" in mappings[key]:
                for typ in mappings[key]["mappings"]:                    
                    if "properties" in mappings[key]["mappings"][typ]:
                        cols=mappings[key]["mappings"][typ]["properties"]
                        cols=list(filter(lambda x:True if cols[x].get("type","NA")=="date" else False,[_ for _ in cols]))
                        cols=set(cols)
                    break
            break
    except:
        logger.error("Unable to convert column.",exc_info=True)
        
    if len(cols)>0:
        containertimezone = pytz.timezone(tzlocal.get_localzone().zone)
        for col in df.columns:
            if col in cols:
                logger.info("Must convert date:"+col)
                if df[col].dtype == "int64":
                    df[col] = pd.to_datetime(
                        df[col], unit='ms', utc=True).dt.tz_convert(containertimezone)
                    df[col]=df[col].apply(lambda x:x if x.year>1970 else "")
                else:
                    df[col] = pd.to_datetime(
                        df[col], utc=True).dt.tz_convert(containertimezone)



        

    if exportcolumns!= None:
        finalcols=[]
        finalcolnames=[]
        for col in exportcolumns:
            spl=col.split("->")
            if spl[0] in df.columns:
                
                if len(spl)>1:
                    finalcols.append(spl[0])
                    finalcolnames.append(spl[1])
                else:
                    finalcols.append(col)
                    finalcolnames.append(col)
        
        df=df[finalcols]
        df.columns=finalcolnames

        logger.info(finalcols)
    logger.info(df.columns)

    if outputformat=="xlsx":
        writer = pd.ExcelWriter(OUTPUT_FOLDER+outputname, engine='xlsxwriter',options={'remove_timezone': True})   
        df.to_excel(writer, sheet_name=index.replace("*",""),index=False)
        worksheet = writer.sheets[index.replace("*","")]  # pull worksheet object
        for idx, col in enumerate(df):  # loop through all columns
            series = df[col]
            max_len = max((
                series.astype(str).map(len).max(),  # len of largest item
                len(str(series.name))  # len of column name/header
                )) + 1  # adding a little extra space
            worksheet.set_column(idx, idx, max_len)  # set column width
        writer.close()        
    else:
        df.to_csv(OUTPUT_FOLDER+outputname)

    logger.info("Write:"+OUTPUT_FOLDER+outputname)

    return {'error':"","took":round(datetime.now().timestamp()-start,2),"total":total,"type":"direct","url":url,"file":OUTPUT_FOLDER+outputname}


def kibanaData(es,conn,matchrequest,user,outputformat,is_rest_api,OUTPUT_URL,OUTPUT_FOLDER):
    logger=logging.getLogger()
    starttime=int(datetime.now().timestamp()*1000)

    hits=0

    try:
        matchres=es.msearch(matchrequest.replace('"size":0','"size":10000'))

        elktime=int(datetime.now().timestamp()*1000)
        
        for response in matchres["responses"]:
            logger.info("===>"+str(len(response["hits"]["hits"])))
            hits=hits+len(response["hits"]["hits"])
            #array=[]
            
            #for arr in response["hits"]["hits"]:                
        logger.info("Hits="+str(hits))

        if hits>3000 and is_rest_api:
            logger.info("Using Rest Helper Process...")
            conn.send_message("/queue/REST_LOAD_KIBANA"
                ,json.dumps({"matchrequest":matchrequest,"user":user,"outputformat":outputformat
                    ,"is_rest_api":is_rest_api,})
                )

            return {'error':"","total":hits,"took":(elktime-starttime),"type":"mail"}



    except Exception as exc:
        logger.error("Unable to execute msearch results.")
        logger.error(exc)
        logger.error( traceback.format_exc())
        return None

    pds=[]       
    for response in matchres["responses"]:
        array=[]
        for arr in response["hits"]["hits"]:
            obj=arr["_source"]
            obj["_id"]=arr["_id"]
            obj["_index"]=arr["_index"]
            #obj["_source"]=arr["_source"]        
            array.append(obj)
        res=pd.DataFrame(array)
        found=False
        for df in pds:
            #if df == res: Unfortunately, this function does not work on high volume
            if df.shape[0] == res.shape[0] and df.shape[1] == res.shape[1]:
                logger.info("DF are identical")
                found=True
                break                            

        if not found:
            pds.append(res)
    dataframetime=int(datetime.now().timestamp()*1000)

    outputname="export_"+str(uuid.uuid4())+'.'+outputformat
    url=OUTPUT_URL+outputname

    logger.info("Saving:"+OUTPUT_FOLDER+outputname)
    writer = pd.ExcelWriter(OUTPUT_FOLDER+outputname)
    for i in range(0,len(pds)):
        pds[i].to_excel(writer, sheet_name='Sheet'+str(i+1))
    writer.save()
    writer.close()
    exporttime=int(datetime.now().timestamp()*1000)
    
    timing={"load":(elktime-starttime)
                    ,"process":(dataframetime-elktime)
                    ,"export":(exporttime-dataframetime)
                    }

    return {'error':"","fileName":outputname,"records":hits,"timing":timing,"type":"direct","url":url}
