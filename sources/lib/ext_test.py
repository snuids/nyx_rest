import logging
from flask_restplus import Namespace,Api, Resource, fields


print("---"*100)

logger=logging.getLogger()

def config(api,conn,es,redis,token_required):
    #---------------------------------------------------------------------------
    # API configRest
    #---------------------------------------------------------------------------
    @api.route('/api/v1/ext/version')
    @api.doc(description="External version.",params={'token': 'A valid token'})

    class extVersion(Resource):    
        @token_required()
        def get(self,user=None):
            logger.info("Config called")
            return {'error':"",'status':'ok','version':"1.0",'welcome':"TOTOTTOTO"}