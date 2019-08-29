import json
import logging
import requests
import pandas as pd
from flask_restplus import Namespace,Api, Resource, fields


print("***>"*100)

logger=logging.getLogger()

def config(api,conn,es,redis,token_required):
    pass
    #---------------------------------------------------------------------------
    # API configRest
    #---------------------------------------------------------------------------
    # @api.route('/api/v1/datasource/<string:dsid>')
    # @api.doc(description="DataSource.",params={'token': 'A valid token'})

    # class extLoadDataSource(Resource):    
    #     @token_required()
    #     def get(self,dsid,user=None):
    #         logger.info("Data source called "+dsid)

    #         ds=es.get(index="nyx_datasource",doc_type="doc",id=dsid)
    #         query=ds["_source"]["query"]
            
    #         r = requests.post('http://esnodebal:9200/_opendistro/_sql',json={"query":query})            
    #         records=json.loads(r.text)            
            
    #         newrecords=[]

    #         if "aggregations" in records:
    #             aggs=records["aggregations"]
    #             for key in aggs:        
    #                 for rec in aggs[key]["buckets"]:
    #                     newrec={"key":rec["key"]}
    #                     for key2 in rec:
    #                         if type(rec[key2]) is dict:
    #                             if "value" in rec[key2]:
    #                                 newrec[key2]=rec[key2]["value"]
    #                     newrecords.append(newrec)
    #                 break
    #         else: #HITS 
    #             if "hits" in records and "hits" in records["hits"]:
    #                 for rec in records["hits"]["hits"]:
    #                     rec["_source"]["_id"]=rec["_id"]
    #                     newrecords.append(rec["_source"])
                    
    #         recjson=pd.DataFrame(newrecords).to_json(orient="records")

    #         return {"error":"","records":json.loads(recjson)}