from flask import Flask, jsonify, abort, make_response
from flask_restful import Api, Resource, reqparse, fields, marshal
from flask_httpauth import HTTPBasicAuth
from src.Utils import TimescaleUtil, Util
from datetime import datetime

app = Flask(__name__, static_url_path="")
api = Api(app)
auth = HTTPBasicAuth()


@auth.get_password
def get_password(username):
    if username == 'merlin':
        return 'arthur'
    else:
        return None


@auth.error_handler
def unauthorized():
    # return 403 instead of 401 to prevent browsers from displaying the default
    # auth dialog
    return make_response(jsonify({'message': 'Unauthorized access'}), 403)


class LocationListAPI(Resource):
    def __init__(self):
        self.reqparse = reqparse.RequestParser(bundle_errors=True)
        self.reqparse.add_argument('location', type=str, required=True,
                                   help='No location provided',
                                   location='json')
        self.reqparse.add_argument('breakdown', type=bool,
                                   help='True or False',
                                   location='json')
        self.reqparse.add_argument('historical', type=bool,
                                   help='True or False',
                                   location='json')
        self.reqparse.add_argument('limit', type=int,
                                   help='int, default is 1000', default=1000,
                                   location='json')
        self.reqparse.add_argument('fromtime', type=lambda x: datetime.strptime(x,'%Y-%m-%d'),
                                   help='UTC Time (format, YYYY-MM-DD)', default=None,
                                   location='json')
        self.reqparse.add_argument('totime', type=lambda x: datetime.strptime(x,'%Y-%m-%d'),
                                   help='UTC Time (format, YYYY-MM-DD)', default=None,
                                   location='json')

        super(LocationListAPI, self).__init__()

    def get(self):
        # Always returns the current records for all locations
        jsonObj = TimescaleUtil.getLocations()
        response = make_response(jsonObj)
        response.headers['content-type'] = 'application/json'
        return response

    def post(self):
        return self.process()

    def process(self):
        args = self.reqparse.parse_args()

        # Parse and build the argument list
        location = args['location']
        breakdown = False

        if args['breakdown']:
            breakdown = True

        historical = False
        if args['historical']:
            historical = True

        # limit = 1000
        # if 'limit' in args:
        #     limit = args['limit']


        limit = args['limit']
        fromtime = args['fromtime']
        totime = args['totime']

        # Call DB Util for executing the request
        jsonObj = TimescaleUtil.getLocations(location=location, breakdown=breakdown,
                                             historical=historical,limit=limit,fromtime=fromtime,totime=totime)

        # format the response
        response = make_response(jsonObj)
        response.headers['content-type'] = 'application/json'
        return response


api.add_resource(LocationListAPI, '/corona/api/v1.0/locations')

if __name__ == '__main__':
    app.run(debug=True)
