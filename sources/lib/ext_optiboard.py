import os
import json
import uuid

import arrow
import datetime
import logging
import requests
import feedparser
from os import listdir
from flask import make_response,url_for
from flask import Flask, jsonify, request,Blueprint
from flask_restplus import Namespace, Api, Resource, fields

print("***>"*100)

logger=logging.getLogger()

# api = Namespace('optiboard', description='Optiboard APIs')

def get_carousel_config(es,opti_id):
    
    config = None
    res    = None    
    try:
        logger.info('TRY to retrieve a carousel with this name: '+str(opti_id)+' ...')
        res = es.search(index="nyx_carousel", body={"query":{"match":{"name.keyword":{"query":str(opti_id)}}}})

        if res is None or res['hits']['total'] == 0:
            raise Exception("carousel not found by name")


    except:
        logger.info('... failed !')
        try:
            logger.info('TRY to retrieve a carousel with this id: '+str(opti_id)+' ...')
            res = config=es.get(index="nyx_carousel",doc_type="doc",id=str(opti_id))
        except:
            logger.info('... failed !')
            res = None
            
    
    if res is not None and res['hits']['total'] > 0:
        car = res['hits']['hits'][0]
        id_view_array = []
        
        for i in car['_source']['id_array']:
            id_view_array.append(i['id'])


        res_views = es.mget(index = 'nyx_view_carousel',
                                   doc_type = 'doc',
                                   body = {'ids': id_view_array})

        _source = {
            'pages': []
        }

        for i in res_views['docs']:
            page = {}
            if '_source' in i:
                for j in i['_source']:
                    #print(j)
                    page[j] = i['_source'][j]

                _source['pages'].append(page)

        #print(_source)
        
        config = {
            '_index': 'nyx_carousel',
             '_type': 'doc',
             '_id': str(opti_id),
             '_version': 17,
             'found': True,
             '_source': _source 
        }        
    else:
        try:
            logger.info('TRY to retrieve a carRousel with this id: '+str(opti_id)+' ...')
            config=es.get(index="nyx_carrousel",doc_type="doc",id=str(opti_id))
        except:
            logger.info('... failed !')
            return None
        
    return config["_source"]["pages"]



def loadRss(url):
    logger.info("LOAD RSS:"+url)
    NewsFeed = feedparser.parse(url)
    rsstitles=[]
    for entry in NewsFeed.entries:
        rsstitles.append(entry["title"])
    return rsstitles

def get_weather(api_key, language,location):
    url = "https://api.openweathermap.org/data/2.5/forecast?q={}&lang={}&units=metric&appid={}".format(location, language,api_key)
    r = requests.get(url)
    weather= r.json()
    weather["list"][0]["main"]["temp"]=int((weather["list"][0]["main"]["temp_min"]+weather["list"][0]["main"]["temp_max"])/2)
    weather["list"][8]["main"]["temp"]=int((weather["list"][8]["main"]["temp_min"]+weather["list"][8]["main"]["temp_max"])/2)

    weather["list"][0]["main"]["url"]="https://openweathermap.org/img/w/"+weather["list"][0]["weather"][0]["icon"]+".png"
    weather["list"][8]["main"]["url"]="https://openweathermap.org/img/w/"+weather["list"][8]["weather"][0]["icon"]+".png"
    

    retobj={"today":weather["list"][0],"tomorrow":weather["list"][8]}
    
    return retobj    


def config(api,conn,es,redis,token_required):
    logger.info(redis)

    #---------------------------------------------------------------------------
    # API LIFE SIGN
    #---------------------------------------------------------------------------
    @api.route('/api/v1/ext/optiboard/lifesign')
    @api.doc(description="Optiboard. Life Sign")
    class optiboard(Resource):                    
        def post(self):
            logger.info("Optiboard lifesign. Client.")

            req= json.loads(request.data.decode("utf-8"))

            guid=req["config"]["guid"]
            disk=req.get("disk",{})
            version=req.get("version","NA")
            starttime=req.get("starttime","")
            logger.info("===>"+guid+"<===")
            
            if redis.get("optiboard_"+guid)==None:
                logger.info("Token does not exists in redis")

                try:
                    record=es.get(index="optiboard_token",doc_type="doc",id=guid)
                    logger.info(">="*30)
                    
                    record=record["_source"]
                    if record["accepted"]==1:
                        redis.set("optiboard_"+guid,json.dumps(record),300)
                        return {'error':""}            
                except:
                    logger.info("Record does not exists. Returning error.")     
                    return {'error':"KO"}
            
            redopti=redis.get("optiboard_"+guid)
            redoptiobj=json.loads(redopti)
            redoptiobj["starttime"]=starttime
            redoptiobj["version"]=version
            redoptiobj["disk"]=disk
            redoptiobj["@timestamp"]=arrow.utcnow().isoformat().replace("+00:00", "Z")
            redoptiobj["station"]=redoptiobj["optiboard"]

            index='optiboard_status-'+datetime.datetime.now().strftime('%Y.%m')
            es.index(index=index,body=redoptiobj,doc_type="doc")            

            return {'error':""}   

    #---------------------------------------------------------------------------
    # API POLL
    #---------------------------------------------------------------------------
    @api.route('/api/v1/ext/optiboard/poll')
    @api.doc(description="Optiboard. Poll End point", params={'guid': 'A guid'})
    class optiboard(Resource):                    
        def get(self):
            logger.info("Optiboard poll called. Client.")
            
            guid=request.args["guid"]
            logger.info("===>"+guid+"<===")

            if redis.get("optiboard_"+guid)==None:
                logger.info("Token does not exists in redis")

                try:
                    record=es.get(index="optiboard_token",doc_type="doc",id=guid)
                    logger.info(">="*30)
                    
                    record=record["_source"]
                    logger.info("1")
                    if record["accepted"]==1:
                        logger.info("2")
                        redis.set("optiboard_"+guid,json.dumps(record),300)
                        logger.info("3")
                        return {'error':""}            
                except Exception as e:
                    logger.info("Record does not exists. Returning error.",exc_info=True)     
                    return {'error':"KO"}

            res=es.search(index="optiboard_command",doc_type="doc",body=
                    {"from":0,"size":200,"query":{"bool":{"filter":[{"bool":{"must":[{"bool":{"must":[{"term":{"guid.keyword":{"value":guid}}},{"term":{"executed":{"value":0,"boost":1.0}}}]}}]}}]}}}
                    )
            logger.info(res)



            commands=[]

            if "hits" in res and "hits" in res["hits"] and len (res["hits"]["hits"]) >0:
                commands=[x["_source"] for x in res["hits"]["hits"]]

                execTime=arrow.utcnow().isoformat().replace("+00:00", "Z")            

                q = {
                        "script": {
                            "inline": "ctx._source.executed=1;ctx._source.execTime=\""+execTime+"\"",
                            "lang": "painless"
                        },
                        "query": {"bool":{"filter":[{"bool":{"must":[{"bool":{"must":[{"term":{"guid.keyword":{"value":guid}}},{"term":{"executed":{"value":0,"boost":1.0}}}]}}]}}]}}                    
                    }
                    

                res2=es.update_by_query(body=q, doc_type='doc', index='optiboard_command')
                logger.info(res2)

            return {'error':"OK","commands":commands}            

    #---------------------------------------------------------------------------
    # API GETCONFIG
    #---------------------------------------------------------------------------
    getConfigAPI = api.model('getConfig_model', {
        'guid': fields.String(description="A screen guid", required=False)
    })
    
    @api.route('/api/v1/ext/optiboard/getconfig')
    @api.doc(description="Optiboard get Config")
    class optiboardGetConfig(Resource):   
        @api.expect(getConfigAPI)                 
        def post(self):
            logger.info("Optiboard config called. ")            
            req= json.loads(request.data.decode("utf-8"))  
            logger.info(req)
            guid=req["guid"]

            try:
                record=es.get(index="optiboard_token",doc_type="doc",id=guid)
                record=record["_source"]
            except:
                logger.info("Record does not exists. Creating it.")
                newrecord=req
                newrecord["accepted"]=0
                newrecord["@creationtime"]=arrow.utcnow().isoformat().replace("+00:00", "Z")                
                es.index(index="optiboard_token",doc_type="doc",id=guid,body=newrecord)
                record=newrecord

            logger.info(record)
            logger.info(record)
            if record["accepted"]==1:
                logger.info("Record is valid")
                if "rss" in record:
                    record["rss_feed"]=loadRss(record["rss"])

                if "weather" in record:
                    record["weather"]=get_weather(record["weather"]["apikey"], record["weather"]["language"],record["weather"]["location"])

                if "carrousel" in record:
                    record["carrousel"]=get_carousel_config(es,record["carrousel"])

                    if record['carrousel'] is None: 
                        return {'error':"Unable to retrieve carousel",'errorcode':101}            
                        

                return {'error':"",'rec':record}            
            else:
                return {'error':"Waiting for approval",'errorcode':100}            



            return {'error':"Unknown error",'errorcode':99}   
        

    #---------------------------------------------------------------------------
    # API GETTOKEN
    #---------------------------------------------------------------------------
    @api.route('/api/v1/ext/optiboard/gettoken')
    @api.doc(description="Optiboard get token", params={'guid': 'A guid'})
    class optiboardGetToken(Resource):                    
        def get(self):
            logger.info("Optiboard get token called. ")  

            guid=request.args["guid"]
            logger.info("GUID="+guid)  
            try:
                record=es.get(index="optiboard_token",doc_type="doc",id=guid)
                record=record["_source"]
                token=uuid.uuid4()
                if record["accepted"]==1:
                    redis.set("nyx_tok_"+str(token),"OK",3600*24)
                    resp=make_response(jsonify({'error':"","token":token}))
                else:
                    resp=make_response(jsonify({'error':"KO"}))
                #resp.set_cookie('nyx_kibananyx', str(token))                
                
                return resp
            except Exception as e:
                logger.info("Record does not exists. Ignoring request.",exc_info=1)
                

            return {'error':"Unknown error",'errorcode':99}                    