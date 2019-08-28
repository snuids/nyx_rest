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

def filterReports(res,user):
    logger=logging.getLogger()
    logger.info("Filter reports")
    logger.info(user)

    if "admin" in user["privileges"]:
        return res

    return list(filter(lambda x:True if x["_source"]["creds"]["user"].get("user","NA")==user["id"] else False,res))
    




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
    
    # logger.info("user "*20)
    # logger.info(user)
    # logger.info("privhash "*20)
    # logger.info(privhash)
    # logger.info("privhash "*20)

    res2=[]
    for rec in res:
        if column in rec["_source"]:
            # logger.info("REC======")
            # logger.info(rec)
            for priv2 in  rec["_source"][column]:
                logger.info(priv2)
                logger.info(privhash)
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
    logger.info("LOAD DATA index=%s doc_type=%s" %(index,doc_type))

    start=datetime.now().timestamp()

    extra=data.get("extra",None)
    if extra !=None:
        del data["extra"]

    #res=es.search(index=index,body=json.dumps(data),doc_type=doc_type)

    if "size" in data:
        maxsize=data["size"]
        if maxsize==0:
            data["size"]=1
            maxsize=1

    else:
        maxsize=200

    logger.info (data)
    response = es.search(
        index=index,
        body=json.dumps(data),
        scroll='10m',doc_type=doc_type
    )

#    logger.info(response)



    hits=[]
    aggs=[]

    total=0

#    logger.info("response - "*20)

    if "hits" in response and "total" in response["hits"]:
#        print(response["hits"]["total"])
        if response["hits"]["total"] is dict:
            total=response["hits"]["total"]["value"]
        else:
            total=response["hits"]["total"]

    #print("TOTAL=%d" %(total))

    while len(response['hits']['hits']):
        # process results
        #print(response)
        if "hits" in response and "hits" in response["hits"]:  
#            print(len(response["hits"]["hits"]))
            hits+=response["hits"]["hits"]
        if "aggregations" in response:
            #print("COUCOUCOUCOCUOCUOUC"*30)
            aggs=response["aggregations"]
            #print(response["aggregations"])
            #print(aggs)

        if len(hits)>maxsize and is_rest_api:
            break

        #print([item["_id"] for item in response["hits"]["hits"]])
        response = es.scroll(scroll_id=response['_scroll_id'], scroll='10m')

    
#    logger.info("data "*30)

#    logger.info(data)
    # if "hits" in res and "hits" in res["hits"]:
    #     hits=res["hits"]["hits"]
    # else:
    #     hits=[]

    # if "aggregations" in res:
    #     aggs=res["aggregations"]
    # else:
    #     aggs=[]

    if index.find("nyx_reporttask")==0:
        hits=filterReports(hits,user)    

    # logger.info("BEF "*30)
    # logger.info(hits)
    if cui[2]!=None and cui[2]!="":
        hits=applyPrivileges(hits,user,cui[2])    
    # logger.info("AFT "*30)
    # logger.info(hits)

    
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

    print(len(hits))

    cleanrecs=cleanElasticRecords({"hits":{"hits":hits}})
    
    outputname="export_"+str(uuid.uuid4())+'.'+outputformat
    url=OUTPUT_URL+outputname

    df=pd.DataFrame(cleanrecs)

### GET MAPPING
    datescol={}
    try:
        containertimezone=pytz.timezone(tzlocal.get_localzone().zone)

        indices_client = IndicesClient(es)
        map=indices_client.get_mapping(index=index,doc_type="doc")
        for key in map:
            indice=map[key]
            if "mappings" in indice and "doc" in indice["mappings"] and "properties" in indice["mappings"]["doc"]:
                for col in indice["mappings"]["doc"]["properties"]:
#                    logger.info("===>"+indice["mappings"]["doc"]["properties"][col]["type"])
                    if indice["mappings"]["doc"]["properties"][col]["type"]=="date":
                        datescol[col]=True

        #print(df["DateAccident"])
        for ind,col in enumerate(df.columns):
            if col in datescol:
                logger.info("Convert column:"+col)
                logger.info("Type:"+str(df.dtypes[ind]))
                if str(df.dtypes[ind])=="int64":
                    #logger.info("Type:"+str(df.dtypes[ind]))
                    df[col]=pd.to_datetime(df[col],unit='ms',utc=True).dt.tz_convert(containertimezone)
                    mindt=pytz.utc.localize(datetime(1971, 1, 1))
                    #logger.info(mindt)
                    df[col]=df[col].apply(lambda x:x if x>=mindt else "")
                    #logger.info("Type:"+str(df.dtypes[ind]))
                else:
                    df[col]=pd.to_datetime(df[col],utc=True).dt.tz_convert(containertimezone)

#        logger.info(df["actualStart"])

    except Exception as e:
        logger.error("Unable to read mapping.",exc_info=True)
        logger.error(e)

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
