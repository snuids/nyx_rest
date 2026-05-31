"""
Data routes: queryFilter, pg_search, generic_search, datasource, kibana_load,
pg_genericCRUD, genericCRUD.
"""

import re
import json
import logging

import dateutil.parser
import requests
import pandas as pd

from flask import request, jsonify
from flask_restx import Resource, fields

import state
from config import settings
from middleware import token_required, check_post_parameters, DateTimeEncoder
from db_postgres import get_postgres_connection
from es_helpers import can_use_indice
from common import loadData, get_mappings, kibanaData
from pg_common import loadPGData, getAppByID, get_sql_server_connection

logger = logging.getLogger()


def register(app, api, name_space):

    queryFilterAPI = api.model('queryFilter_model', {})

    @name_space.route('/queryFilter/<string:rec_id>')
    class genericQueryFilter(Resource):
        @token_required()
        @api.doc(description="Fills the query filters.", params={'token': 'A valid token'})
        @api.expect(queryFilterAPI)
        def post(self, rec_id, user=None):
            logger.info("Query Filter=" + rec_id)
            data = json.loads(request.data.decode("utf-8"))

            app = None
            if state.elkversion >= 7:
                app = state.es.get(index="nyx_app", id=rec_id)
            else:
                app = state.es.get(index="nyx_app", doc_type="doc", id=rec_id)

            if app is None:
                return {"error": "UNKNOWN APP"}

            app = app["_source"]

            if "queryfilters" not in app["config"]:
                return {"error": "NO QUERY FILTERS"}

            selected = []
            if "selected" in data:
                selected = data["selected"]
            else:
                selected = ["" for i in range(0, len(app["config"]["queryfilters"]) + 1)]

            timerange = None
            if "timerange" in data:
                timerange = data["timerange"]
                timerange[0] = dateutil.parser.parse(timerange[0])
                timerange[1] = dateutil.parser.parse(timerange[1])

            addquery = []
            alladdqueries = []
            for index, queryf in enumerate(app["config"]["queryfilters"]):
                if queryf["type"] == "queryselecter" and \
                        selected[index] != "" and selected[index] != "*":
                    qht = get_mappings(state.es, app["config"]["index"])
                    qcol = queryf["field"]
                    if qcol in qht and qht[qcol] == "text":
                        qcol += ".keyword"
                    val = selected[index]
                    if isinstance(val, str):
                        addquery.append(qcol + ":\"" + val + "\"")
                    else:
                        addquery.append(qcol + ":" + str(val))

                alladdqueries.append(" AND ".join(addquery))

            finaladd = " AND ".join(addquery)
            logger.info("Add query:" + finaladd)

            for index, queryf in enumerate(app["config"]["queryfilters"]):
                if queryf["type"] == "queryselecter":
                    qht = get_mappings(state.es, app["config"]["index"])
                    qcol = queryf["field"]
                    if qcol in qht and qht[qcol] == "text":
                        qcol += ".keyword"

                    cui = can_use_indice(app["config"]["index"], user, None)

                    size = 200
                    try:
                        if queryf.get('querySize'):
                            size = int(queryf.get('querySize'))
                    except:
                        logger.warning('unable to retrieve query size')
                        pass

                    query = {
                        "from": 0,
                        "size": 0,
                        "aggregations": {
                            qcol: {
                                "terms": {
                                    "field": qcol,
                                    "size": size,
                                    "order": [{"_key": "asc"}],
                                }
                            }
                        },
                    }
                    query["query"] = cui[1]

                    if index > 0 and len(alladdqueries[index - 1]) > 0 and \
                            len(addquery) > 0 and \
                            "query" in query and "bool" in query["query"] and \
                            "must" in query["query"]["bool"]:
                        query["query"]["bool"]["must"][0]["query_string"]["query"] = (
                            "(" + query["query"]["bool"]["must"][0]["query_string"]["query"] +
                            ") AND " + alladdqueries[index - 1]
                        )

                    if "timefield" in app["config"] and timerange is not None:
                        field = app["config"]["timefield"]
                        newobj = {"range": {}}
                        newobj["range"][field] = {
                            "gte": int(timerange[0].timestamp()) * 1000,
                            "lte": int(timerange[1].timestamp()) * 1000,
                            "format": "epoch_millis",
                        }
                        query["query"]["bool"]["must"].append(newobj)

                    res = state.es.search(index=app["config"]["index"], body=query)
                    queryf["buckets"] = res["aggregations"][qcol].get("buckets", [])

            return {"error": "", "queryfilters": app["config"]["queryfilters"]}

    genericSearchAPI = api.model('genericSearch_model', {
        'size': fields.String(description="The max size", required=True),
        'query': fields.String(description="The query.", required=True),
    })

    @name_space.route('/pg_search/<string:appid>')
    class genericSearchPG(Resource):
        @token_required()
        @api.doc(description="Execute the search from a sql app.",
                 params={'token': 'A valid token'})
        @api.expect(genericSearchAPI)
        def post(self, appid, user=None):
            logger.info("PG Generic Search=" + appid)
            data = json.loads(request.data.decode("utf-8"))
            return loadPGData(
                state.es, appid, get_postgres_connection(), state.conn, data,
                (request.args.get("download", "0") == "1"),
                True, user,
                request.args.get("output", "csv"),
                state.OUTPUT_URL, state.OUTPUT_FOLDER,
            )

    @name_space.route('/generic_search/<string:index>')
    class genericSearch(Resource):
        @token_required()
        @api.doc(description="Generic search a database collection.",
                 params={'token': 'A valid token'})
        @api.expect(genericSearchAPI)
        def post(self, index, user=None):
            logger.info("Generic Search=" + index)
            data = json.loads(request.data.decode("utf-8"))
            cui = can_use_indice(index, user, data.get("query", None))
            if not cui[0]:
                logger.info("Index Not Allowed for user.")
                return {'error': "Not Allowed", "records": [], "aggs": []}

            logger.info("Must be filtered:" + str(cui[2]))
            data["query"] = cui[1]

            return loadData(
                state.es, state.conn, index, data,
                request.args.get("doc_type", "doc"),
                (request.args.get("download", "0") == "1"),
                cui, True, user,
                request.args.get("output", "csv"),
                state.OUTPUT_URL, state.OUTPUT_FOLDER,
            )

    @name_space.route('/datasource/<string:dsid>')
    @api.doc(description="DataSource.",
             params={'token': 'A valid token', 'start': 'Start Time', 'end': 'End Time'})
    class extLoadDataSource(Resource):
        @token_required()
        def get(self, dsid, start=None, end=None, user=None):
            start = request.args.get("start", None)
            end = request.args.get("end", None)
            flat = request.args.get("flat", "false").lower() in ["true", "1", "yes"]
            logger.info(
                "Data source called " + dsid + " start:" + str(start) +
                " end:" + str(end) + " flat:" + str(flat)
            )

            if state.elkversion >= 7:
                ds = state.es.get(index="nyx_datasource", id=dsid)
            else:
                ds = state.es.get(index="nyx_datasource", doc_type="doc", id=dsid)

            logger.info("QUERY TYPE# " * 20)
            query = ds["_source"]["query"]
            querytype = ds["_source"].get("type", "elasticsearch")
            logger.info(querytype)

            if start is not None:
                query = query.replace("@START@", start)
            if end is not None:
                query = query.replace("@END@", end)

            logger.info("Final Query:" + query)

            if querytype == "postgres":
                recs = []
                with get_postgres_connection().cursor() as cursor:
                    cursor.execute(query)
                    recs = cursor.fetchall()
                    logger.info(recs)
                encoder = DateTimeEncoder()
                records = json.loads(encoder.encode(recs))
                return records if flat else {"error": "", "records": records}

            else:
                elk_base = settings.ELK_URL
                if ':' not in elk_base:
                    elk_base = f"{elk_base}:9200"
                protocol = "https" if settings.ELK_SSL else "http"
                sqlpost = f"{protocol}://{elk_base}/_sql"
                r = requests.post(sqlpost, json={"query": query})
                records = json.loads(r.text)

                if "columns" in records:
                    results = []
                    cols = []
                    for col in records["columns"]:
                        if "alias" in col:
                            cols.append(col["alias"])
                        else:
                            cols.append(col["name"])

                    for rec in records["rows"]:
                        obj = {}
                        for i, col in enumerate(cols):
                            obj[col] = rec[i]
                        results.append(obj)

                    return results if flat else {"error": "", "records": results}

                newrecords = []
                if "aggregations" in records:
                    aggs = records["aggregations"]
                    for key in aggs:
                        for rec in aggs[key]["buckets"]:
                            newrec = {"key": rec["key"]}
                            for key2 in rec:
                                if type(rec[key2]) is dict:
                                    if "value" in rec[key2]:
                                        newrec[key2] = rec[key2]["value"]
                            newrecords.append(newrec)
                        break
                else:
                    if "hits" in records and "hits" in records["hits"]:
                        for rec in records["hits"]["hits"]:
                            rec["_source"]["_id"] = rec["_id"]
                            newrecords.append(rec["_source"])

                recjson = pd.DataFrame(newrecords).to_json(orient="records")
                records = json.loads(recjson)
                return records if flat else {"error": "", "records": records}

    @app.route('/api/v1/kibana_load', methods=['POST'])
    @token_required()
    def kibanaLoad(user=None):
        logger.info("Kibana Load")
        outputformat = request.args.get("output", "csv")
        logger.info("Output:" + outputformat)
        token = request.args.get('token')
        logger.info("Full Key:" + "nyx_kib_msearch" + token)
        matchrequest = state.redisserver.get("nyx_kib_msearch" + token).decode('utf-8')
        logger.info(matchrequest)
        return kibanaData(
            state.es, state.conn, matchrequest, user, outputformat,
            True, state.OUTPUT_URL, state.OUTPUT_FOLDER,
        )

    @app.route('/api/v1/pg_generic/<index>/<col>/<pkey>', methods=['GET', 'POST', 'DELETE'])
    @token_required()
    def pg_genericCRUD(index, col, pkey, user=None):
        met = request.method.lower()
        logger.info(
            "PG Generic Table=" + index + " Col:" + col + " Pkey:" + pkey + " Method:" + met
        )

        app_id = request.args.get("app", None)
        db_type = "postgres"
        ap = None
        if app_id is not None:
            ap = getAppByID(state.es, app_id)
            if ap is not None:
                db_type = ap["_source"]["config"].get("databaseType", "postgres")

        if met == 'get':
            if isinstance(pkey, str):
                query = "select * from \"" + index + "\" where " + col + "='" + str(pkey + "'")
            else:
                query = "select * from \"" + index + "\" where " + col + "=" + str(pkey)

            description = None

            if db_type == "sqlserver":
                def convert_sql_server_type_to_python_type(sql_type):
                    if sql_type == "int":
                        return 23
                    elif sql_type == "str":
                        return 1043
                    elif sql_type == "datetime":
                        return 1184
                    else:
                        return -1

                sqconn = get_sql_server_connection(ap)
                with sqconn.cursor() as cursor:
                    cursor.execute(query)
                    res = cursor.fetchone()
                    description = [
                        {"col": x[0], "type": convert_sql_server_type_to_python_type(x[1].__name__)}
                        for x in cursor.description
                    ]

                    res2 = {}
                    for index, x in enumerate(cursor.description):
                        if x[1] in [1082, 1184, 1114]:
                            print(res[index])
                            res2[x[0]] = res[index].isoformat()
                        else:
                            res2[x[0]] = res[index]
                        res2[x[0] + "_$type"] = convert_sql_server_type_to_python_type(
                            x[1].__name__
                        )
                sqconn.commit()
            else:
                with get_postgres_connection().cursor() as cursor:
                    cursor.execute(query)
                    res = cursor.fetchone()
                    description = [{"col": x[0], "type": x[1]} for x in cursor.description]

                    res2 = {}
                    for index, x in enumerate(cursor.description):
                        if x[1] in [1082, 1184, 1114]:
                            print(res[index])
                            res2[x[0]] = res[index].isoformat()
                        else:
                            res2[x[0]] = res[index]
                        res2[x[0] + "_$type"] = x[1]

                state.pg_connection.commit()
            return {'error': "", "data": res2, "columns": description}

        elif met == 'post':
            data = request.data.decode("utf-8")
            logger.info("CREATE/UPDATE RECORD")
            logger.info(data)
            data = json.loads(data)

            if pkey != "NEW":
                query = "UPDATE \"" + index + "\" set "
                cols = ",".join(
                    ["" + str(_["key"]) + "='" + str(_["value"]) + "' " for _ in data["record"]]
                )
                query += cols
                query += " where " + col + "=" + str(pkey)
                logger.info(query)
                with get_postgres_connection().cursor() as cursor:
                    res = cursor.execute(query)
                    logger.info(res)
                state.pg_connection.commit()
            else:
                query = "INSERT INTO \"" + index + "\"  "
                cols = ",".join(["" + str(_["key"]) + "" for _ in data["record"]])
                query += "(" + cols + ") VALUES ("
                vals = ",".join(["'" + str(_["value"]) + "'" for _ in data["record"]])
                query += vals + ")"
                logger.info(query)
                with get_postgres_connection().cursor() as cursor:
                    res = cursor.execute(query)
                    logger.info(res)
                state.pg_connection.commit()

            return {'error': ""}

        elif met == 'delete':
            try:
                with state.pg_connection.cursor() as cursor:
                    query = (
                        "delete from \"" + index + "\" where " + col + "=" + str(pkey)
                    )
                    cursor.execute(query)
                state.pg_connection.commit()
            except:
                logger.error("Unable to delete record.", exc_info=True)
                return {'error': "unable to delete record"}

            return {'error': ""}

    @app.route('/api/v1/generic/<index>/<object>', methods=['GET', 'POST', 'DELETE'])
    @token_required()
    def genericCRUD(index, object, user=None):
        from es_helpers import send_event

        data = None
        met = request.method.lower()
        logger.info("Generic Index=" + index + " Object:" + object + " Method:" + met)

        cui = can_use_indice(index, user, None)
        if not cui[0]:
            logger.info("Index Not Allowed for user.")
            return {'error': "Not Allowed", "records": [], "aggs": []}

        if met == 'get':
            try:
                if state.elkversion >= 7:
                    ret = state.es.get(index=index, id=object)
                else:
                    ret = state.es.get(index=index, id=object,
                                       doc_type=request.args.get("doc_type", "doc"))
            except:
                return {'error': "unable to get data", "data": None}
            return {'error': "", "data": ret}

        elif met == 'post':
            try:
                data = request.data.decode("utf-8")
                if index == "nyx_user":
                    dataobj = json.loads(data)
                    if "$pbkdf2-sha256" not in dataobj["password"]:
                        from passlib.hash import pbkdf2_sha256
                        dataobj["password"] = pbkdf2_sha256.hash(dataobj["password"])
                        data = json.dumps(dataobj)
                if state.elkversion >= 7:
                    state.es.index(index=index, body=data, id=object)
                else:
                    state.es.index(index=index, body=data,
                                   doc_type=request.args.get("doc_type", "doc"), id=object)
            except:
                logger.error("unable to post data", exc_info=True)
                return {'error': "unable to post data"}

        elif met == 'delete':
            try:
                if state.elkversion >= 7:
                    ret = state.es.delete(index=index, id=object)
                else:
                    ret = state.es.delete(index=index, id=object,
                                          doc_type=request.args.get("doc_type", "doc"))
                logger.info(ret)
            except:
                return {'error': "unable to delete data"}

        send_event(
            user=user, indice=index, method=met, _id=object,
            doc_type=request.args.get("doc_type", "doc"), obj=data,
        )

        return {'error': ""}
