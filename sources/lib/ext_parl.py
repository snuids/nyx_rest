import json
import logging
import threading
import numpy as np
import pandas as pd
import pytz,tzlocal
import elasticsearch
from datetime import datetime
from datetime import timedelta
from flask import Flask, jsonify, request,Blueprint
from flask_restplus import Namespace,Api, Resource, fields
from operator import itemgetter
import distance
from elastic_helper import es_helper 



logger=logging.getLogger()
print("***>"*100)

tokens = {}
tokenlock=threading.RLock()
containertimezone=pytz.timezone(tzlocal.get_localzone().zone)

def getUserFromToken(request, redis):
    global tokens
    token=request.args.get('token')
    with tokenlock:
        if token in tokens:
            return tokens[token]
        redusr=redis.get("nyx_tok_"+token)
        logger.info("nyx_fulltok_"+token)
        if redusr!=None:
            logger.info("Retrieved user "+token+ " from redis.")
            redusrobj=json.loads(redusr)
            tokens[token]=redusrobj
            logger.info("Token reinitialized from redis cluster.")
            return redusrobj

    logger.error("Invalid Token:"+token)
    return None

def config(api,conn,es,redis,token_required):
    #---------------------------------------------------------------------------
    # API configRest
    #---------------------------------------------------------------------------
    @api.route('/api/v1/ext/parliament/bhddashboard')
    @api.doc(description="BHD Dashboard.",params={'guid': 'A valid guid'})
    class bhddashboard(Resource):    
        def get(self,user=None):
            logger.info("BHD Dashboard called")

            if 'guid' not in request.args and 'token' not in request.args:
                return {'error':"GUID OR TOKEN REQUIRED"}

            res = None
                
            
            if 'guid' in request.args:
                try:
                    res = es.get(index='optiboard_token', doc_type='doc', id=request.args["guid"])
                except elasticsearch.NotFoundError:
                    logger.error('GUID Not found')
                    return {'error':"BAD GUID"}

                if 'accepted' in res['_source'] and res['_source']['accepted']!=1:
                    logger.error('GUID Not valid')
                    return {'error':"GUID NOT VALID"}
            
            
            if 'token' in request.args:
                roles=['bhd-dashboard']
                usr=getUserFromToken(request, redis)

                logger.info(usr)

                if usr==None:
                    return {'error':"UNKNOWN_TOKEN"}
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
                    if not ok:
                        return {'error':"NO_PRIVILEGE"}

            management = False
            if 'management' in request.args and request.args['management'].lower() == 'true':
                management = True
            

            records = bhd_dashboard(es, management, filter_no_open_work=True)

            logger.info(records)
            logger.info(type(records))

            return {'error':"","records":json.dumps(records)}

    #---------------------------------------------------------------------------
    # API Network
    #---------------------------------------------------------------------------
    @api.route('/api/v1/ext/parliament/bhdnetwork')
    @api.doc(description="BHD Network.",params={'token': 'A valid token'})    
    class bhdnetwork(Resource):    
        @token_required()
        def get(self,user=None):
            logger.info("BHD Network called")
         

            return {'error':"","records":bhd_network(es)}            
            #return {}

def bhd_network(es):
    body=json.loads("""{"from":0,"size":2000,"query":{"bool":{"filter":[{"bool":{"must":[{"bool":{"must":[{"range":{"datetime":{"from":"now-3d","to":null,"include_lower":false,"include_upper":true,"boost":1.0}}},{"range":{"datetime":{"from":null,"to":"now","include_lower":true,"include_upper":false,"boost":1.0}}}],"adjust_pure_negative":true,"boost":1.0}}],"adjust_pure_negative":true,"boost":1.0}}],"adjust_pure_negative":true,"boost":1.0}}}""")

    res=es.search(index="parl_history_kizeo", body=body)
    table=[x["_source"] for x in res["hits"]["hits"]]
    df=pd.DataFrame(table)
    df["user"].unique()
    to=df["to"].dropna().unique()
    user=df["user"].dropna().unique()
    all1=[x for x in to]
    all2=[x for x in user]

    alluser=pd.DataFrame(all1+all2)
    alluser.columns=["x"]
    alluser["x"].drop_duplicates(inplace=True)

    alluser.sort_values(by=["x"]).reset_index(inplace=True)


    userhashtable={row[0]:x for x,row in alluser.iterrows()}

    links=[]
    for x in table:
        if "to" in x:        
            links.append({"sid":userhashtable[x["user"]],"tid":userhashtable[x["to"]]})
    nodes=[{"id":userhashtable[x],"name":x} for x in userhashtable]

    finalres={
      "nodes": nodes,
      "links": links,
      "nodeSize":10,
      "canvas":False
    }

    return finalres






def bhd_dashboard(es, management=None, filter_no_open_work=True):
    
    bhd_ppl = get_ppl_bhd(es)

    
    work_ppl = get_work_ppl(es, filter_no_open_work)
    #CLEANING NAME FROM KIZEO
    clean_work_ppl = {}
    for worktech in work_ppl:
        if worktech in bhd_ppl:
            clean_work_ppl[worktech] = work_ppl[worktech]
        else:
            for bhdtech in bhd_ppl:
                if distance.nlevenshtein(worktech, bhdtech, method=1) < 0.15:
                    clean_work_ppl[bhdtech] = work_ppl[worktech]
                    
    
    dict_in_out = get_stat_in_out(es)
    #CLEANING NAME FROM KIZEO
    clean_dict_in_out = {}
    for worktech in dict_in_out:
        if worktech in bhd_ppl:
            clean_dict_in_out[worktech] = dict_in_out[worktech]
        else:
            for bhdtech in bhd_ppl:
                if distance.nlevenshtein(worktech, bhdtech, method=1) < 0.15:
                    clean_dict_in_out[bhdtech] = dict_in_out[worktech]
    
    
    
    for bhdtech in bhd_ppl:
        if bhdtech not in clean_work_ppl:
            

            clean_work_ppl[bhdtech] = {
                'works'      : [],
                'work_number': 0,
                'work_closed': 0,
            }

        field_list = ['firstname', 'lastname', 'bhd_active', 'technic', 'zone', 'shift', 'tel', 'management', 'matricule']
        for i in field_list:
            if i in bhd_ppl[bhdtech]:
                clean_work_ppl[bhdtech][i] = bhd_ppl[bhdtech][i]



    ret = [clean_work_ppl[x] for x in clean_work_ppl]
    ret.sort(reverse=True, key=works_len)

    for tech in ret:

        final_user = ''
        if 'final_user' in tech:
            final_user = tech['final_user']
        elif 'firstname' in tech and 'lastname' in tech:
            final_user =  compute_user_name(tech['firstname'], tech['lastname']) 
            tech['final_user'] = final_user

        if final_user in clean_dict_in_out:
            tech['in_out'] = clean_dict_in_out[final_user]



    df=pd.DataFrame(ret)

    if management is not None:
        df = df[df['management']==management]
        
    
    df['work_open'] = df['work_number']-df['work_closed']
    df=df.sort_values('work_open', ascending=False)
    df=df[(df['work_open']!=0) | (df['bhd_active'])]
    df=df.head(12)
    df=df.sort_values(['technic','work_open','work_number'], ascending=[True,False,False]).fillna('')
    

    df['final_user']=df['final_user'].fillna(df['firstname'].str.lower()+' '+df['lastname'].str.lower())

    # ret = {row['final_user']:row.to_dict() for index, row in df.iterrows()}

    ret = [row.to_dict() for index, row in df.iterrows()]

    dict_old = retrieve_dict_old(es)

    for i in ret:
        if 'matricule' in i and i['matricule'] in dict_old:
            i['work_old'] = dict_old[i['matricule']]

    return ret
    

def works_len(e):
    if 'works' in e:
        return len(e['works'])
    else:
        return 0

def compute_user_name(firstname, lastname):
    return str(firstname.lower().strip()+' '+lastname.lower().strip()).replace('-', ' ')

def retrieve_dict_old(es):
    today_time = datetime.min.time()
    today_datetime = datetime.combine(datetime.now().date(), today_time)

    dataframe=es_helper.elastic_to_dataframe(es,index="parl_kizeo*",scrollsize=1000
                                        ,timestampfield="update_time_dt"
                                        ,query='wo_state:Underway'
                                        ,end=today_datetime
                                        ,_source=['update_time_dt', 'wo_state', 'user_name'])  


    #return dataframe
    dict_old={}
    for key, value in dataframe.groupby('user_name').count()['_id'].iteritems():
        try:
            dict_old[key.split(' ')[0]]=value
        except:
            print('unable to split this key: '+str(key))

    return dict_old


def get_stat_in_out(es):
    containertimezone=pytz.timezone(tzlocal.get_localzone().zone)

    today_time = datetime.min.time()
    today_datetime = datetime.combine(datetime.now().date(), today_time)

    startstr=containertimezone.localize(today_datetime).isoformat()
    endstr=containertimezone.localize(today_datetime+timedelta(days=1)).isoformat()


    query_history_kizeo={"from":0,"size":10000,"query":{"bool":{"filter":[{"bool":{"must":[{"bool":{"must":[{"range":{"datetime":{"from":startstr,"to":endstr,"include_lower":False,"include_upper":True,}}}],}}],}}],}}}


    res = es.search(index="parl_history_kizeo",body=query_history_kizeo)

    dict_ppl = None

    if res['hits']['total'] > 0:
        dict_ppl={}
        for i in res['hits']['hits']:
            rec = i['_source']

            pest_list = ['pcc pcc', 'admin team']

            if rec['type'] == 'transfer':
                if rec['user'] not in pest_list and rec['to'] not in pest_list:
                    if rec['user'] not in dict_ppl:
                        dict_ppl[rec['user']] = {'in': 0, 'out': 0}
                    if rec['to'] not in dict_ppl:
                        dict_ppl[rec['to']] = {'in': 0, 'out': 0}

                    dict_ppl[rec['user']]['out'] += 1
                    dict_ppl[rec['to']]['in'] += 1
                    
    return dict_ppl



def get_ppl_bhd(es):

    query_bhd_tec={"from":0,"size":10000,"query": {"bool": {"must": [{"match_all": {}}]}}}


    res = es.search(index="parl_bhd_technician",body=query_bhd_tec)
    
    dict_ppl = None

    if res['hits']['total'] > 0:

        dict_ppl={}
        for i in res['hits']['hits']:
            rec = i['_source']
            rec['work'] = []
            rec['matricule'] = i['_id']
            final_name = compute_user_name(rec['firstname'], rec['lastname']) 
            rec['final_user'] = final_name
            dict_ppl[final_name] = rec
    

    return dict_ppl

def get_work_ppl(es, filter_no_open_work=True):
    today_time = datetime.min.time()
    today_datetime = datetime.combine(datetime.now().date(), today_time)

    startstr=containertimezone.localize(today_datetime).isoformat()
    endstr=containertimezone.localize(today_datetime+timedelta(days=1)).isoformat()

    querykizeo={"from":0,"size":1000,"query":
                {"bool":{"filter":[
                    {"bool":{"must":[
                        {"bool":{"must":[
                            {"range":{"update_time_dt":
                                      {"from":startstr,"to":endstr,
                                       "include_lower":False,"include_upper":True,}}}],}}],}}],}}}
    
    works=es.search(index="parl_kizeo",body=querykizeo)
    if "hits" in works and "hits" in works["hits"]:
        tmp = []
        for i in works["hits"]["hits"]:
            obj = {
                'n_d_ot'    :i['_source']['n_d_ot'],
                'wo_state'  :i['_source']['wo_state'],
                'final_user':i['_source']['final_user'].lower(),
                'direction' :i['_source']['direction'],
                'priority'  :i['_source']['description_de_l_intervention'],
                'intervention_date_limite'  :i['_source']['date_limite_d_intervention_dt'],
                'floor'     :i['_source']['floor'],
                'building'  :i['_source']['building'],
                'location'  :i['_source']['location'],
                'zone'      :i['_source']['zone'],
            }

            if 'date_de_creation_new__dt' in i['_source']:
                obj['creation_date']  = i['_source']['date_de_creation_new__dt']
            else:
                obj['creation_date']  = i['_source']['create_time_dt']

            
            if 'appel_bhd' in i['_source'] and i['_source']['appel_bhd']=='1':
                obj['appel_bhd'] = True
            else:
                obj['appel_bhd'] = False
            
            tmp.append(obj)
                
            
        works = tmp
    else:
        works=[]
        
    work_ppl={x['final_user']:{'works':[], 'final_user': x['final_user']} for x in works}
    for work in works:
        work_ppl[work["final_user"]]['works'].append(work)
        
    for tech in work_ppl:
        work_ppl[tech]['works'] = sorted(work_ppl[tech]['works'], key=itemgetter('creation_date'), reverse=True)
        work_ppl[tech]['work_number'] = len(work_ppl[tech]['works'])

        closed = 0
        for work in work_ppl[tech]['works']:
            if work['wo_state'].lower() == 'closed':
                closed += 1

        work_ppl[tech]['works'] = [x for x in work_ppl[tech]['works'] if not (x['wo_state'].lower() == 'closed')]


        work_ppl[tech]['work_closed'] = closed
        
    
    if filter_no_open_work:
        work_ppl = dict((k, v) for k,v in work_ppl.items() if v['work_closed']!=v['work_number'])

    
    return work_ppl


