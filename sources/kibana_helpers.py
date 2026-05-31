"""
Kibana URL helpers: URL cleaning, dashboard URL computation, and computeMenus.
"""

import re
import json
import logging

import prison

import state

logger = logging.getLogger()


def clean_kibana_url_0(url):
    url = url.replace('<iframe src="https://', "")
    url = url.replace('" height="600" width="800"></iframe>', "")
    url = url.replace('/kibana/app/', '/kibananyx/app/')
    return url


def clean_kibana_url(url, column, filter):
    regex = r"(query:'[^']*')"
    replacement = "(" + (" OR ".join([column + ":" + x for x in filter])) + ")"

    matches = re.finditer(regex, url)

    for matchNum, match in enumerate(matches):
        matchNum = matchNum + 1

        for groupNum in range(0, len(match.groups())):
            groupNum = groupNum + 1

            query = match.group(groupNum)
            first = query.find(":")

            minquery = query[first + 2:-1]
            if minquery == '':
                minquery = replacement
            else:
                minquery += " AND " + replacement

            minquery = "query:'" + minquery + "'"
            print(minquery)
            url = url.replace(match.group(groupNum), minquery)
    return url


def compute_kibana_url(dashboard_dict, appl):
    if appl.get('config').get('kibanaId') is None:
        return appl.get('config').get('url')

    url = "/dashboard/" + appl.get('config')['kibanaId'] + ""
    shorturl = "/app/dashboards#/view/" + appl.get('config')['kibanaId'] + ""

    time = "from:now-7d,mode:quick,to:now"

    if appl.get('config').get('kibanaTime') is not None:
        time = appl.get('config').get('kibanaTime')

    refresh = "refreshInterval:(pause:!t,value:0)"

    if appl.get('timeRefresh') and appl.get('timeRefreshValue'):
        if 'refreshInterval' in appl.get('timeRefreshValue'):
            refresh = appl.get('timeRefreshValue')
        else:
            refresh = 'refreshInterval:(pause:!f,value:' + str(appl.get('timeRefreshValue')) + ')'

    try:
        dash = dashboard_dict[appl.get('config').get('kibanaId')]
    except:
        logger.error("Unable to compute kibana URL")
        logger.error(appl.get('config'))
        return 'INVALIDURL', 'INVALIDURL'

    dash_obj = dash.get('_source').get('dashboard')

    url += "?embed=true&_g=(" + refresh + ",time:(" + time + "))"
    shorturl += "?embed=true&_g=(" + refresh + ",time:(" + time + "))"
    url += "&_a=(description:'" + dash_obj.get('description') + "'"
    url += ",filters:!(),fullScreenMode:!f"

    if dash_obj.get('optionsJSON'):
        options = json.loads(dash_obj.get('optionsJSON'))
        url_options = ','.join(
            [str(k) + ':' + str(v) for k, v in options.items()]
        ).replace('True', '!t').replace('False', '!f')
        url += ",options:(" + url_options + ")"
    else:
        url += ",options:()"

    panels = []
    panels_json = json.loads(dash_obj.get('panelsJSON'))

    for pan in panels_json:
        if dash.get('_source').get('migrationVersion') and \
           dash.get('_source').get('migrationVersion').get('dashboard') in [
               '7.0.0', '7.1.0', '7.2.0', '7.3.0']:
            for ref in dash.get('_source').get('references'):
                if ref.get('name') == pan.get('panelRefName'):
                    pan['id'] = ref.get('id')
                    pan['type'] = ref.get('type')

        if pan is not None and pan.get("embeddableConfig") is not None and \
                pan["embeddableConfig"].get("colors") is not None:
            newcols = {}
            for colkey in pan["embeddableConfig"]["colors"]:
                newcols[colkey.replace("%", "%25").replace(" ", "%20")] = \
                    pan["embeddableConfig"]["colors"][colkey]
            pan["embeddableConfig"]["colors"] = newcols

        if pan is not None and pan.get("embeddableConfig") is not None and \
                pan["embeddableConfig"].get("vis") is not None and \
                pan["embeddableConfig"]["vis"].get("colors") is not None:
            newcols = {}
            for colkey in pan["embeddableConfig"]["vis"]["colors"]:
                newcols[colkey.replace("%", "%25").replace(" ", "%20")] = \
                    pan["embeddableConfig"]["vis"]["colors"][colkey]
            pan["embeddableConfig"]["vis"]["colors"] = newcols

        panels.append(prison.dumps(pan))

    url += ",panels:!(" + ','.join(panels).replace('#', "%23").replace('&', "%26") + ")"

    query = "query:(language:lucene,query:'*')"

    if dash_obj.get('kibanaSavedObjectMeta') and \
            dash_obj.get('kibanaSavedObjectMeta').get('searchSourceJSON'):
        query_2 = json.loads(dash_obj.get('kibanaSavedObjectMeta').get('searchSourceJSON'))
        if query_2.get('query'):
            query = 'query:' + prison.dumps(query_2.get('query'))

    url += "," + query + ",timeRestore:!f,title:Test,viewMode:view)"

    space = ''
    if dash.get('_source').get('namespace') and \
            dash.get('_source').get('namespace') != 'default':
        space = 's/' + dash.get('_source').get('namespace')

    finalurl = ('./kibananyx/' + space + "/app/kibana#" + url).replace("//", "/")
    finalshorturl = ('./kibananyx/' + space + "" + shorturl).replace("//", "/")
    return finalurl, finalshorturl


def get_dict_dashboards(es):
    query = {
        "query": {
            "bool": {
                "must": [{"query_string": {"query": "type: dashboard"}}]
            }
        }
    }
    res = es.search(index=".kibana*", body=query, size=10000)
    return {dash['_id'].split(':')[-1]: dash for dash in res['hits']['hits']}


def computeMenus(usr, token, apptag):
    from es_helpers import refresh_translations, get_translated_item

    refresh_translations()
    if state.elkversion >= 7:
        res3 = state.es.search(size=1000, index="nyx_app", body={"sort": [{"order": "asc"}]})
    else:
        res3 = state.es.search(size=1000, index="nyx_app", doc_type="doc",
                               body={"sort": [{"order": "asc"}]})

    dict_dashboard = get_dict_dashboards(state.es)

    categories = {}
    for app in res3["hits"]["hits"]:
        appl = app["_source"]
        appl["rec_id"] = app["_id"]

        if "privileges" in usr["_source"] and "privileges" in appl and \
                "admin" not in usr["_source"]["privileges"]:
            if len([value for value in usr["_source"]["privileges"]
                    if value in appl["privileges"]]) == 0:
                continue

        if apptag == "console":
            if "apptags" in appl and len(appl["apptags"]) > 0 and apptag not in appl["apptags"]:
                continue
        else:
            if "apptags" not in appl or len(appl["apptags"]) == 0:
                continue
            if apptag not in appl["apptags"]:
                continue

        if appl.get("type") == "kibana":
            logger.info('compute kibana url for : ' + str(appl.get('title')))

            config = appl["config"]
            old_kibana_url = config.get("url")
            old_kibana_shorturl = config.get("shorturl")

            try:
                config["url"], config["shorturl"] = compute_kibana_url(dict_dashboard, appl)
            except Exception as e:
                logger.error("Unable to compute kibana url", exc_info=True)
                config["url"] = old_kibana_url
                config["shorturl"] = old_kibana_shorturl

            if config.get("filtercolumn") is not None and \
                    config.get("filtercolumn") != "" and \
                    "filters" in usr["_source"] and \
                    len(usr["_source"]["filters"]) > 0:
                config["url"] = clean_kibana_url(
                    config.get('url'), config.get("filtercolumn"), usr["_source"]["filters"]
                )

            if old_kibana_url != config.get("url") or \
                    old_kibana_shorturl != config.get("shorturl"):
                logger.warning(
                    'the url calculated for app: ' + appl.get('title') +
                    ' is desync from the database (ES)'
                )
                logger.warning(config.get("url"))
                logger.warning(old_kibana_url)
                logger.warning('we have to update database !!!')
                app_to_index = appl.copy()
                del app_to_index['rec_id']
                state.es.index(index=app['_index'], id=app['_id'], body=app_to_index)

        if appl["category"] not in categories:
            categories[appl["category"]] = {"subcategories": {}}

        if "subcategory" in appl and \
                appl["subcategory"] in categories[appl["category"]]["subcategories"]:
            target = categories[appl["category"]]["subcategories"][appl["subcategory"]]
        elif "subcategory" in appl and \
                appl["subcategory"] not in categories[appl["category"]]["subcategories"]:
            target = categories[appl["category"]]["subcategories"][appl["subcategory"]] = []
        else:
            if "" in categories[appl["category"]]["subcategories"]:
                target = categories[appl["category"]]["subcategories"][""]
            else:
                target = categories[appl["category"]]["subcategories"][""] = []
        target.append(appl)

    finalcategory = []

    language = usr["_source"]["language"]
    logger.info("User language:" + language)

    for key in categories:
        loc_cat = get_translated_item(language, "menus", key)
        finalcategory.append({"category": key, "loc_category": loc_cat, "submenus": []})
        target = finalcategory[-1]
        for key2 in categories[key]:
            for key3 in categories[key][key2]:
                loc_sub = get_translated_item(language, "menus", key3)
                target["submenus"].append({"title": key3, "loc_title": loc_sub, "apps": []})
                for appli in categories[key][key2][key3]:
                    del appli["category"]
                    if "subcategory" in appli:
                        del appli["subcategory"]
                    if "order" in appli:
                        del appli["order"]
                    if "privileges" in appli:
                        del appli["privileges"]
                    appli["loc_title"] = get_translated_item(language, "menus", appli["title"])
                    target["submenus"][-1]["apps"].append(appli)

                if len(target["submenus"][-1]["apps"]) > 0 and \
                        "icon" in target["submenus"][-1]["apps"][0]:
                    target["submenus"][-1]["icon"] = target["submenus"][-1]["apps"][0]["icon"]

    return finalcategory
