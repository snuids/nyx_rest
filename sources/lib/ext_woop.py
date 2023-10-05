import json
import requests
import os,logging
from pytz import timezone
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
            # try:
                data=json.loads(request.data.decode("utf-8"))

                task_type = 'woop'

                picking_task = create_picking(data, task_type)
                picking_task_id = picking_task.get('id', None)
                delivery_task = create_delivery(data, task_type, pickup_id=picking_task_id)


                logger.info(data)
                return make_response(jsonify({
                    "error": "",
                    "deliveryId": delivery_task['id'],
                    "trackingPageUrl":  delivery_task['trackingURL']
                }), 201)
            # except Exception as e:
            #     logger.error(e)
            #     return make_response(jsonify({
            #                 "reasons": [
            #                     f"Unhandled error"
            #                 ]
            #             }), 400)




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

def create_delivery(delivery_input, task_type, pickup_id=None):
    delivery = delivery_input['delivery']
    
    start_delivery = tz.localize(datetime.strptime(delivery['interval'][0]['start'], '%Y-%m-%dT%H:%M:%S+0000'))
    end_delivery   = tz.localize(datetime.strptime(delivery['interval'][0]['end'], '%Y-%m-%dT%H:%M:%S+0000'))
    
    
    address = f"{delivery['address']['addressLine1']} {delivery['address']['postalCode']} {delivery['address']['city']}"
    
    task = {
        "organization": 'CGwZVvB16KfI43EcCR3TqGV~',
        'container': {'type': 'TEAM', 'team': 'zVabf5TnN2er7uvPd9jSUyxJ'},
        "state": 0,
        "pickupTask": False,
        "metadata": [{
                        "name": "task_type",
                        "type": "string",
                        "value": task_type
                    }],
        "completeAfter": int(start_delivery.timestamp()*1000),
        "completeBefore": int(end_delivery.timestamp()*1000),
        "recipients": [{
            "name": f"{delivery['contact']['firstName']} {delivery['contact']['lastName']}",
            "phone": delivery['contact']['phone']
        }],
        "destination": {
            "notes": "",
            "address": {
                "name": f"{address}",
                "unparsed": f"{address}",
            }
        }
    }
    
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


def create_picking(delivery_input, task_type):
    picking = delivery_input['picking']
    
    start_picking = tz.localize(datetime.strptime(picking['interval'][0]['start'], '%Y-%m-%dT%H:%M:%S+0000'))
    end_picking   = tz.localize(datetime.strptime(picking['interval'][0]['end'], '%Y-%m-%dT%H:%M:%S+0000'))
    
    
    address = f"{picking['address']['addressLine1']} {picking['address']['postalCode']} {picking['address']['city']}"
    
    task = {
        "organization": 'CGwZVvB16KfI43EcCR3TqGV~',
        'container': {'type': 'TEAM', 'team': 'zVabf5TnN2er7uvPd9jSUyxJ'},
        "state": 0,
        "pickupTask": True,
        "metadata": [{
                        "name": "task_type",
                        "type": "string",
                        "value": task_type
                    }],
        "completeAfter": int(start_picking.timestamp()*1000),
        "completeBefore": int(end_picking.timestamp()*1000),
        "recipients": [{
            "name": f"{picking['contact']['firstName']} {picking['contact']['lastName']}",
            "phone": picking['contact']['phone']
        }],
        "destination": {
            "notes": "",
            "address": {
                "name": f"{address}",
                "unparsed": f"{address}",
            }
        }
    }
    onfleet_task = None
    res = None
    try:
        res=requests.post(ONFLEET_URL_TASK, auth=(ONFLEET_APIKEY, ""), json=task)
        logger.info(res.status_code)
        onfleet_task = res.json()
    except:
        logger.error('unable to create task in onfleet')
    
    return onfleet_task