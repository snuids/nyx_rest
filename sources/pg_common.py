import json
import uuid
import pytz
import tzlocal
import logging
import datetime
import traceback
import pandas as pd
#from datetime import datetime
from datetime import timedelta
from cachetools import cached, LRUCache, TTLCache



@cached(cache=TTLCache(maxsize=1024, ttl=30))
def getAppByID(es,appid):
    logger=logging.getLogger()
    logger.info("Loading APP:>>"+appid)
    res=es.get("nyx_app",id=appid,doc_type="doc")

    return res

#################################################################################

def loadPGData(es,appid,pgconn,conn,data,download,is_rest_api,user,outputformat,OUTPUT_URL,OUTPUT_FOLDER):
    logger=logging.getLogger()
    start=datetime.datetime.now().timestamp()

    logger.info("LOAD PG DATA:"+appid)
    maxsize=1000
    if data!=None and "size" in data:
        maxsize=data["size"]

    app=getAppByID(es,appid)

    query=app["_source"]["config"]["sql"]

    if data != None and "query" in data:
        if len(data["query"])>0:
            query=query.replace("${FILTER}","%"+data["query"]+"%")
        else:
            query=query.replace("${FILTER}","%")
    else:
        query=query.replace("${FILTER}","%")

    order=query.lower().rfind("order by ")
    query1=query
    query2=""
    if order!=-1:
        query1=query[:order]
        query2=query[order:]


    rangequery=""

    if data!=None and "range" in data:
        
        startr=data["range"]["gte"]
        endr=data["range"]["lte"]
        if "timefield" in app["_source"]["config"]:
            timecol=app["_source"]["config"]["timefield"]
            rangequery=timecol+" >= '"+datetime.datetime.fromtimestamp(startr/1000).isoformat()+"' and "+timecol+" <= '"+datetime.datetime.fromtimestamp(endr/1000).isoformat()+"'"

            

            if "where" in query.lower():
                if "group" in query.lower():
                    rind=query1.lower().rindex("group")
                    query1=query1[:rind]+ " AND ("+rangequery+")"+" "+query1[rind:]
                else:
                    query1+=" AND ("+rangequery+")"
            else:
                query1+=" where "+rangequery
            logger.info(query1)


    fields=[_["field"] for _ in app["_source"]["config"]["headercolumns"]]

    query=query1+query2

    hits=[]

    sqlcount=query1
    queryselect=sqlcount.lower().index("select")
    queryfrom=sqlcount.lower().index("from")
    sqlcount2=sqlcount[0:len("select")]+" count(*) "+sqlcount[queryfrom:]

    sqlcounthist=""

    aggtime=None
    aggtimeval=10

    if "graphicChecked" in app["_source"] and app["_source"]["graphicChecked"]:

        totalminutes=(endr-startr)/(1000*60)
        if totalminutes<5:
            aggtime="second"
            aggtimeval=1
        elif totalminutes<240:
            aggtime="minute"
            aggtimeval=60
        elif totalminutes<5*60*24:
            aggtime="hour"
            aggtimeval=60*60
        elif totalminutes<40*60*28:
            aggtime="day"
            aggtimeval=60*60*24            
        else:
            aggtime="week"            
            aggtimeval=60*60*24*7            


        sqlcounthist=sqlcount[0:len("select")]+" count(*),date_trunc('"+aggtime+"',date2) as datein "+sqlcount[queryfrom:]+ " GROUP BY datein"
        aggtime="1"+aggtime[0]


    count=0

    aggs=None

    with pgconn.cursor() as cursor:
        logger.info(sqlcount2)
        cursor.execute(sqlcount2)
        res=cursor.fetchone()
        count=res[0]


        if len(sqlcounthist)>0:
            logger.info(sqlcounthist)
            cursor.execute(sqlcounthist)    
            arecs = cursor.fetchall()   
            
            aggs=[]
            for rec in arecs:
                obj={"doc_count":rec[0],"key":int(rec[1].timestamp()*1000),"key_as_string":rec[1].isoformat()}
                aggs.append(obj)
            aggs={"2":{"buckets":aggs}}

        #logger.info(res)

        cursor.execute(query+" LIMIT "+str(maxsize))    
        recs = cursor.fetchall() 
        colnames = [desc[0] for desc in cursor.description]

    pgconn.commit()

    pkey="_id"
    if "pkey" in app["_source"]["config"]:
        pkey=app["_source"]["config"]["pkey"]

    if not pkey in fields:
        fields.append(pkey)
    
    fieldsindex=[]
    for field in fields:
        try:
            fieldsindex.append(colnames.index(field))
        except:
            fieldsindex.append(-1)


    for rec in recs:
        obj={}
        for i,field in enumerate(fields):            
            if fieldsindex[i]!=-1:
                recval=rec[fieldsindex[i]]
                if isinstance(recval,datetime.date):                
                    obj[field]=recval.isoformat()
                else:
                    obj[field]=recval
            else:
                obj[field]=""
            if field==pkey:
                obj["_id"]=rec[fieldsindex[i]]

        hits.append(obj)
    

    took=int((datetime.datetime.now().timestamp()-start)/1000)    

    if not download:
        return {'error':"","took":took
                ,"total":count,"records":hits,"colnames":[{"col":x[0],"type":x[1]} for x in cursor.description], "aggs":aggs,"aggtimeval":aggtimeval,"aggtime":aggtime}    

    exportcolumns=None
    
    if len(hits)>5000 and is_rest_api:
        logger.info("Using Rest Helper Process...")

        conn.send_message("/queue/REST_LOAD_PGDATA"
            ,json.dumps({"appid":appid, "data":data
                ,"download":download,"is_rest_api":is_rest_api,"user":user,
                "outputformat":outputformat})
            )

        return {'error':"","took":took,"total":count,"type":"mail"}


    exportColumns=None

    if "exportColumns" in app["_source"]["config"]:
        exportcolumns=app["_source"]["config"]["exportColumns"]
        exportcolumns=[x.strip() for x in exportcolumns.split(",")]
        logger.info("===== "*30)
        logger.info(exportcolumns)

#     cleanrecs=cleanElasticRecords(res)
    
    outputname="export_"+str(uuid.uuid4())+'.'+outputformat
    url=OUTPUT_URL+outputname

    df=pd.DataFrame(hits)

    for x in [desc for desc in cursor.description]:
        print(x)

### GET MAPPING
    datescol={}
    try:
        containertimezone=pytz.timezone(tzlocal.get_localzone().zone)

        map=[desc for desc in cursor.description]
        for m in map:
            col=m[0]
            typ=m[1]
            if m[1] in [1082,1184,1114]:
                datescol[col]=True
        print(datescol)

    except Exception as e:
        logger.error("Unable to read mapping.",exc_info=True)
        logger.error(e)

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

    if exportcolumns!= None:
        finalcols=[]
        for col in exportcolumns:
            if col in df.columns:
                finalcols.append(col)
        
        df=df[finalcols]

#     #logger.info(finalcols)
#     #logger.info(df.columns)

    if outputformat=="xlsx":
        writer = pd.ExcelWriter(OUTPUT_FOLDER+outputname, engine='xlsxwriter',options={'remove_timezone': True})   
        df.to_excel(writer, sheet_name=app["_source"]["config"]["index"],index=False)
        worksheet = writer.sheets[app["_source"]["config"]["index"]]  # pull worksheet object
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

    took=int((datetime.datetime.now().timestamp()-start)/1000)    
    return {'error':"","took":took,"total":count,"type":"direct","url":url,"file":OUTPUT_FOLDER+outputname}
