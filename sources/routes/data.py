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
from middleware import (
    token_required, check_post_parameters, DateTimeEncoder, fingerprint, request_token
)
from db_postgres import get_postgres_connection, discard_postgres_connection
from es_helpers import (
    can_use_indice, is_nyx_user_indice, can_read_nyx_user, requires_admin_for_write, is_admin,
)
from es_read import fetch_document, find_user_by_login, is_not_found
from common import loadData, get_mappings, kibanaData
from pg_common import loadPGData, getAppByID, get_sql_server_connection
from helpers.sql_queries import build_crud_query

logger = logging.getLogger()


# The identifier of a SQL record travels in the URL, and the console interpolates the key
# column of the row it displays: a row whose key is NULL leaves as the string "null" — the
# `cars` app has `pkey: year` and two rows without a year, which `pg_search` returns as
# `_id: null`. Bound to an integer column that is `invalid input syntax for type integer:
# "null"`; the statement then aborted the shared connection's transaction, so every later
# request of that worker — on any SQL app, not just that one — answered 500 with
# `InFailedSqlTransaction` until the health check reconnected, ten seconds later. Refuse
# the placeholder here: there is no record behind it, so no connection is worth touching.
PLACEHOLDER_KEYS = ("", "null", "none", "nan", "undefined")


def is_placeholder_key(value):
    return value is None or str(value).strip().lower() in PLACEHOLDER_KEYS


def rollback_sql_connection(conn):
    """Undo the failed statement on the connection the route used.

    A PostgreSQL connection is shared by every request of the worker: without this the
    transaction stays aborted and the next request fails for a reason that has nothing to
    do with it. A rollback that itself fails means the connection cannot be reused, so it
    is dropped and the next call reconnects instead of failing for ten seconds.
    """
    if conn is None:
        return
    try:
        conn.rollback()
    except Exception:
        logger.warning("Unable to roll back the failed statement", exc_info=True)
        if conn is getattr(state, "pg_connection", None):
            discard_postgres_connection()






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
            app = state.es.get(index="nyx_app", id=rec_id)
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
                    if not cui[0]:
                        # The facet lists the distinct values of a field: serving it to an
                        # account that may not read the index discloses its data without ever
                        # returning a document. The route used to search regardless.
                        logger.info("Index not allowed for user, facet skipped")
                        queryf["buckets"] = []
                        continue

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
            try:
                return loadPGData(
                    state.es, appid, get_postgres_connection(), state.conn, data,
                    (request.args.get("download", "0") == "1"),
                    True, user,
                    request.args.get("output", "csv"),
                    state.OUTPUT_URL, state.OUTPUT_FOLDER,
                )
            except Exception:
                # A 500 is an empty screen with no explanation in the console, and a
                # statement that fails on the shared connection takes the whole worker
                # with it unless the transaction is closed here.
                logger.error("Unable to read the SQL app " + appid, exc_info=True)
                rollback_sql_connection(state.pg_connection)
                return {'error': "unable to load data", "records": [], "aggs": None,
                        "total": 0, "colnames": []}

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

            ds = state.es.get(index="nyx_datasource", id=dsid)
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
        token, _ = request_token(request)
        if token is None:
            return {"error": "NO_TOKEN"}
        logger.info("Kibana msearch key for token " + fingerprint(token))
        matchrequest = state.redisserver.get("nyx_kib_msearch" + token)
        if matchrequest is None:
            # The key is written by nginx when the console asks for an export and lives for as
            # long as the request. Without it there is no msearch to run: `.decode()` on None
            # raised AttributeError, which the worker answered with a 500 and an empty body —
            # an export that failed without saying why.
            logger.warning("kibana_load: no msearch request for token " + fingerprint(token))
            return {"error": "NO_EXPORT_REQUEST"}
        matchrequest = matchrequest.decode('utf-8')
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

        if met in ('get', 'delete') and is_placeholder_key(pkey):
            logger.info("PG record identifier is a placeholder: refused before the database")
            return {'error': "invalid record identifier", "data": None}

        if met == 'get':
            try:
                query, params = build_crud_query(db_type, met, index, col, pkey)
            except ValueError:
                return {'error': "invalid SQL identifier"}

            description = None
            dbconn = None

            try:
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

                    dbconn = get_sql_server_connection(ap)
                    if dbconn is None:
                        return {'error': "unable to connect to database"}
                    with dbconn.cursor() as cursor:
                        cursor.execute(query, params)
                        res = cursor.fetchone()
                        description = [
                            {"col": x[0],
                             "type": convert_sql_server_type_to_python_type(x[1].__name__)}
                            for x in cursor.description
                        ]

                        res2 = {}
                        for index, x in enumerate(cursor.description):
                            if x[1] in [1082, 1184, 1114]:
                                res2[x[0]] = res[index].isoformat()
                            else:
                                res2[x[0]] = res[index]
                            res2[x[0] + "_$type"] = convert_sql_server_type_to_python_type(
                                x[1].__name__
                            )
                    dbconn.commit()
                else:
                    dbconn = get_postgres_connection()
                    if dbconn is None:
                        return {'error': "unable to connect to database"}
                    with dbconn.cursor() as cursor:
                        cursor.execute(query, params)
                        res = cursor.fetchone()
                        description = [{"col": x[0], "type": x[1]}
                                       for x in cursor.description]

                        res2 = {}
                        for index, x in enumerate(cursor.description):
                            if x[1] in [1082, 1184, 1114]:
                                res2[x[0]] = res[index].isoformat()
                            else:
                                res2[x[0]] = res[index]
                            res2[x[0] + "_$type"] = x[1]

                    dbconn.commit()
            except Exception:
                logger.error("Unable to read the SQL record.", exc_info=True)
                rollback_sql_connection(dbconn)
                return {'error': "unable to read record", "data": None}

            return {'error': "", "data": res2, "columns": description}

        elif met == 'post':
            data = request.data.decode("utf-8")
            logger.info("CREATE/UPDATE RECORD")
            data = json.loads(data)

            try:
                query, params = build_crud_query(
                    db_type, met, index, col, pkey, data["record"]
                )
            except (KeyError, TypeError, ValueError):
                return {'error': "invalid record"}

            if db_type == "sqlserver":
                dbconn = get_sql_server_connection(ap)
            else:
                dbconn = get_postgres_connection()

            if dbconn is None:
                logger.error("No database connection available for record create/update.")
                return {'error': "unable to connect to database"}

            try:
                with dbconn.cursor() as cursor:
                    cursor.execute(query, params)
                dbconn.commit()
            except Exception:
                logger.error("Unable to create or update the SQL record.", exc_info=True)
                rollback_sql_connection(dbconn)
                return {'error': "unable to save record"}

            return {'error': ""}

        elif met == 'delete':
            dbconn = None
            try:
                if db_type == "sqlserver":
                    dbconn = get_sql_server_connection(ap)
                else:
                    dbconn = get_postgres_connection()

                if dbconn is None:
                    logger.error("No database connection available for record delete.")
                    return {'error': "unable to connect to database"}

                query, params = build_crud_query(db_type, met, index, col, pkey)
                with dbconn.cursor() as cursor:
                    cursor.execute(query, params)
                dbconn.commit()
            except Exception:
                logger.error("Unable to delete record.", exc_info=True)
                rollback_sql_connection(dbconn)
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

        # Writing to these indices means granting oneself rights: `nyx_user` holds the
        # privileges and the password hashes, `nyx_app` decides what each screen may do,
        # `nyx_privilege` what each role may see, `nyx_indice` which data each index
        # exposes and `nyx_filter` which rows are visible. Leaving them open made the read
        # restriction bypassable in two requests.
        if met in ("post", "delete") and requires_admin_for_write(index) and not is_admin(user):
            logger.info("protected index: write refused for a non-admin account")
            return {'error': "Not Allowed"}

        if met == 'get':
            # `can_use_indice` only sees an index name: it cannot tell *which* document is
            # requested. Reading by identifier therefore has its own check, otherwise
            # `GET /generic/nyx_user/<other>` returned another account's hash.
            if is_nyx_user_indice(index) and not can_read_nyx_user(user, object):
                logger.info("nyx_user: reading another account refused")
                return {'error': "Not Allowed", "data": None}
            error, ret = fetch_document(state.es, index, object)
            return {'error': error, "data": ret}

        elif met == 'post':
            try:
                data = request.data.decode("utf-8")
                if index == "nyx_user":
                    dataobj = json.loads(data)
                    from passlib.hash import pbkdf2_sha256
                    from password_policy import generate_temporary_password
                    provided = dataobj.get("password") or ""
                    existing = None
                    try:
                        existing = state.es.get(index="nyx_user", id=object)["_source"]
                    except Exception as exc:
                        if not is_not_found(exc):
                            # A read failure is not an absence. Falling through to the
                            # creation branch would mint a temporary password nobody knows
                            # and close the account — the very defect this guard exists to
                            # prevent, this time triggered by a cluster hiccup.
                            logger.error("nyx_user: account unreadable before the write",
                                         exc_info=True)
                            return {'error': "unable to read the account"}
                        # Genuine absence. Eight accounts are indexed under another identifier
                        # than the one their owner signs in with (`amarchand@icloud.com` for
                        # the login `amarchand`), so look for the same person under
                        # `login.keyword` before treating this as a creation.
                        hit = find_user_by_login(state.es, object)
                        existing = hit["_source"] if hit else None

                    if provided and "$pbkdf2-sha256" not in provided:
                        dataobj["password"] = pbkdf2_sha256.hash(provided)
                    elif existing is not None:
                        # An update, not a creation. The interface sends the stored hash back —
                        # or leaves the field out — and either way the secret is already
                        # decided. Minting a fresh one here reset the password of every account
                        # whose language, phone or privilege an administrator edited, and left
                        # it closed behind `mustResetPassword`.
                        dataobj["password"] = existing.get("password", "")
                        dataobj["mustResetPassword"] = existing.get("mustResetPassword", False)
                    else:
                        # A new account with nothing usable supplied: one is generated that
                        # nobody knows, and the account stays closed until its owner has
                        # chosen one.
                        dataobj["password"] = pbkdf2_sha256.hash(generate_temporary_password())
                        dataobj["mustResetPassword"] = True
                    data = json.dumps(dataobj)
                state.es.index(index=index, body=data, id=object)
            except:
                logger.error("unable to post data", exc_info=True)
                return {'error': "unable to post data"}

        elif met == 'delete':
            try:
                ret = state.es.delete(index=index, id=object)
                logger.info(ret)
            except:
                return {'error': "unable to delete data"}

        send_event(
            user=user, indice=index, method=met, _id=object,
            doc_type=request.args.get("doc_type", "doc"), obj=data,
        )

        return {'error': ""}
