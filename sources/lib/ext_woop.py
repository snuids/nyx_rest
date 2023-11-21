import json
import requests
import os,logging

from pytz import timezone, UTC
from dateutil.parser import isoparse
from functools import wraps
from datetime import datetime
from datetime import timedelta
from flask import Flask, jsonify, request,Blueprint, make_response
from flask_restplus import Namespace,Api, Resource, fields



print("***>"*100)

logger=logging.getLogger()


ONFLEET_URL_TASK = "https://onfleet.com/api/v2/tasks"
ONFLEET_APIKEY = os.environ['ONFLEET_APIKEY']


def config(api,conn,es,redis,token_required):

    name_space_deliveries = api.namespace('api/v1/woop/deliveries', description='Main APIs')

    #---------------------------------------------------------------------------
    # WOOP endpoints
    #---------------------------------------------------------------------------

    @name_space_deliveries.route("/<string:task_id>")
    class WoopDelivery(Resource):
        def delete(self, task_id):

            logger.info(task_id)
            logger.info(request)

            res=requests.get(f"{ONFLEET_URL_TASK}/{task_id}", auth=(ONFLEET_APIKEY, ""))            

            logger.info(res.json())

            if res.status_code == 200:
                logger.info("we found the task in onfleet")
                onfleet_task = res.json()

                dependencies = onfleet_task.get('dependencies', [])
                
                dependencies_tasks = []
                for dep_task in dependencies:
                    res=requests.get(f"{ONFLEET_URL_TASK}/{dep_task}", auth=(ONFLEET_APIKEY, ""))        
                    dependencies_tasks.append(res.json())    

                task_already_started = False
                if onfleet_task['state'] > 1:
                    task_already_started = True

                for dependencies_task in dependencies_tasks:
                    if dependencies_task['state'] > 1:
                        task_already_started = True
                    
                if task_already_started:
                    logger.info("the task is already started or completed => we cannot delete it anymore")
                    return make_response(jsonify({
                                            "reasons": [
                                                "Task is already started or completed."
                                            ],
                                            "comment": "It's too late!"
                                            }), 403)



                for dep_task in dependencies:
                    logger.info(f"trying to delete dependency {dep_task}")
                    res=requests.delete(f"{ONFLEET_URL_TASK}/{dep_task}", auth=(ONFLEET_APIKEY, "")) 
                    logger.info(res.status_code)           
                    
                logger.info(f"trying to delete {task_id}")
                res=requests.delete(f"{ONFLEET_URL_TASK}/{task_id}", auth=(ONFLEET_APIKEY, ""))   
                logger.info(res.status_code)         
                return make_response(jsonify({"error": ""}), 204)

            else:
                logger.info("task not found in onfleet")
                return make_response(jsonify({"error": ""}), 202)

            


    @name_space_deliveries.route('/')
    @api.doc(description="deliveries endpoint for woop connection")
    class WoopDeliveries(Resource):

        def get(self):
            return {"error":"get"}

        
        @check_post_parameters("orderId","referenceNumber","retailer","picking","delivery")
        def post(self):
            try:
                data=json.loads(request.data.decode("utf-8"))

                logger.info(data)

                customer = data.get('retailer', {}).get('code', '')

                store_id = ""
                prefix = ""
                if customer == 'supermarches-match':
                    store_id = data.get('retailer', {}).get('store', {}).get('id', '')
                    prefix = "MAT "

                task_type = f"{customer}_{store_id}"

                metadata = [{
                    "name": "connector",
                    "type": "string",
                    "value": "woop"
                },{
                    "name": "customer",
                    "type": "string",
                    "value": customer
                },{
                    "name": "task_type",
                    "type": "string",
                    "value": task_type
                }]

                picking_task = create_picking(data, metadata, prefix)
                picking_task_id = picking_task.get('id', None)
                delivery_task = create_delivery(data, metadata, prefix, pickup_id=picking_task_id)


                logger.info(data)
                return make_response(jsonify({
                    "error": "",
                    "deliveryId": delivery_task['id'],
                    "trackingPageUrl":  ""
                }), 201)
            except Exception as e:
                logger.error(e)
                return make_response(jsonify({
                            "reasons": [
                                f"Unhandled error"
                            ]
                        }), 400)




#---------------------------------------------------------------------------
# CHECK POST
#---------------------------------------------------------------------------
def check_post_parameters(*parameters):
    def wrapper(f):        
        @wraps(f)
        def decorated_function(*args, **kwargs):
            try:
                req= json.loads(request.data.decode("utf-8"))
                reasons = []
                for param in parameters:
                    if not param in req:
                        reasons.append(f"Missing argument '{param}'")

                if len(reasons) > 0:
                    return make_response(jsonify({
                        "reasons": reasons
                    }), 400)
            except Exception as e:                
                logger.error("Unable to decode body")
                return make_response(jsonify({
                            "reasons": [
                                f"Unable to decode body"
                            ]
                        }), 400)

            return f(*args, **kwargs)
        return decorated_function
    return wrapper



tz = timezone('Europe/Paris')

def create_delivery(delivery_input, metadata, prefix="", pickup_id=None):
    delivery = delivery_input['delivery']
    
    start_delivery = isoparse(delivery['interval'][0]['start'])
    end_delivery = isoparse(delivery['interval'][0]['end'])
    
    
    address = f"{delivery['location']['addressLine1']} {delivery['location']['postalCode']} {delivery['location']['city']}"
    
    task = {
        "organization": 'CGwZVvB16KfI43EcCR3TqGV~',
        'container': {'type': 'TEAM', 'team': 'zVabf5TnN2er7uvPd9jSUyxJ'},
        "state": 0,
        "pickupTask": False,
        "metadata": metadata,
        "completeAfter": int(start_delivery.timestamp()*1000),
        "completeBefore": int(end_delivery.timestamp()*1000),
        "recipients": [{
            "name": f"{delivery['contact']['firstName']} {delivery['contact']['lastName']}",
            "phone": delivery['contact']['phone']
        }],
        "destination": {
            "notes": "",
            "address": {
                "name": f"{prefix}{address}",
                "unparsed": f"{address}",
            }
        }
    }

    task['notes'] = f"commande : {delivery_input.get('referenceNumber', '')}\r\n"
    
    if pickup_id is not None:
        task['dependencies'] = [pickup_id]
    
    onfleet_task = None
    res=None
    try:
        res=requests.post(ONFLEET_URL_TASK, auth=(ONFLEET_APIKEY, ""), json=task)
        logger.info(res.status_code)
        onfleet_task = res.json()
    except:
        logger.error('unable to create task in onfleet')
    
    return onfleet_task


def create_picking(delivery_input, metadata, prefix=""):
    picking = delivery_input['picking']
    
    start_picking = isoparse(picking['interval'][0]['start'])
    end_picking = isoparse(picking['interval'][0]['end'])
    
    
    address = f"{picking['location']['addressLine1']} {picking['location']['postalCode']} {picking['location']['city']}"
    
    task = {
        "organization": 'CGwZVvB16KfI43EcCR3TqGV~',
        'container': {'type': 'TEAM', 'team': 'zVabf5TnN2er7uvPd9jSUyxJ'},
        "state": 0,
        "pickupTask": True,
        "metadata": metadata,
        "completeAfter": int(start_picking.timestamp()*1000),
        "completeBefore": int(end_picking.timestamp()*1000),
        "recipients": [{
            "name": f"{picking['contact']['firstName']} {picking['contact']['lastName']}",
            "phone": picking['contact']['phone']
        }],
        "destination": {
            "notes": "",
            "address": {
                "name": f"{prefix}{address}",
                "unparsed": f"{address}",
            }
        }
    }

    task['notes'] = f"commande : {delivery_input.get('referenceNumber', '')}\r\n"

    onfleet_task = None
    res = None
    try:
        res=requests.post(ONFLEET_URL_TASK, auth=(ONFLEET_APIKEY, ""), json=task)
        logger.info(res.status_code)

        if res.status_code>=300:
            logger.info(res)


        onfleet_task = res.json()
    except:
        logger.error('unable to create task in onfleet')
    
    return onfleet_task