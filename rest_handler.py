import json
import time
import requests
from flask import abort
from google.cloud import firestore, exceptions


INSTALLED = "Installed"
INSTALLING = "Installing"
FAILED = "Failed"
DELETING = "Deleting"

class DataAccessLayer(object):
    def __init__(self, collection):
        self.db = firestore.Client()
        self.collection = collection

    def list(self,limit=None, start_time= None):
        # Then query for documents
        users_ref = self.db.collection(self.collection)
        query = users_ref

        if start_time:
            query = query.where('timestamp', '>', start_time)
        else:
            query = users_ref

        if limit:
            print("limit:", limit)
            query = query.order_by('timestamp').limit(limit)
        else:
            query = users_ref
        
        docs = query.get()
        response = []
        for doc in docs:
            doc_dict = {}
            doc_dict = doc.to_dict().copy()
            doc_dict["serial"] = doc.id
            response.append(doc_dict)
            print(u'{} => {}'.format(doc.id,doc_dict))
        return response
    
    def get(self,docid):
        users_ref = self.db.collection(self.collection).document(docid)
        doc = users_ref.get()
        doc_dict = {}
        doc_dict = doc.to_dict().copy()
        doc_dict["serial"] = doc.id
        print(u'{} => {}'.format(doc.id,doc_dict))
        return doc_dict

    
    def update(self,message,doc_id,field_name):
        users_ref = self.db.collection(self.collection)
        print("field:{}, tpid:{}".format(field_name,doc_id))
        users_ref.document(doc_id).set({field_name:message}, merge=True)
        return

    def get_docs_by_field(self,field_key,field_value):
        users_ref = self.db.collection(self.collection)
        docs = users_ref.where(field_key, '==', field_value).limit(20).get()
        return docs

def get_thingpointlist(args):
    dal_obj = DataAccessLayer("thingpoint_db")
    thingpoint_list = dal_obj.list()
    resp_list = []
    for thingpoint in thingpoint_list:
        resp_dict = {}
        resp_dict["mac_address"] = thingpoint.get("mac","NA")
        resp_dict["hostname"] = thingpoint.get("hostname","NA")
        resp_dict["location"] = "location1"
        resp_dict["bundlename"] = thingpoint.get("bundle_name","NA")
        resp_dict["status"] = thingpoint.get("conn_status","NA")
        if resp_dict["status"]:
            resp_dict["health"] = "green"
        else:
            resp_dict["health"] = "red"
        resp_dict["serial"] = thingpoint.get("serial","NA")
        resp_list.append(resp_dict)

    print("response list : {}".format(str(resp_list)))
    return json.dumps(resp_list)

def fill_snap_info(snap,del_enable):
    snap_info_dict = {}
    snap_info_dict['channel'] = snap.get('channel',"")
    snap_info_dict['name'] = snap['name']
    snap_info_dict['revision'] = snap.get('revision',"")
    snap_info_dict['version'] = snap.get('version',"")
    snap_info_dict['status'] = snap['status']
    snap_info_dict['devmode'] = snap.get('devmode',"")
    snap_info_dict['del_enable'] = del_enable
    return snap_info_dict

def get_snaplist(serial):
    dal_obj = DataAccessLayer("thingpoint_db")
    thingpoint = dal_obj.get(serial)
    resp_list = []
    system_snap_list = thingpoint['system_snap_list']
    print("system_snap_list:{}".format(system_snap_list))
    for snap in system_snap_list:
        #if snap["status"] != DELETING:
        resp_dict = fill_snap_info(snap,False)
        resp_list.append(resp_dict)
    
    user_snap_list = thingpoint['user_snap_list']
    print("user_snap_list:{}".format(user_snap_list))
    for snap in user_snap_list:
        #if snap["status"] != DELETING:
        resp_dict = fill_snap_info(snap,True)
        resp_list.append(resp_dict)
    print("response snap list : {}".format(str(resp_list)))
    return json.dumps(resp_list)

def get_snapbundle_info(args):
    if "serial" in args.keys():
        serial = args["serial"]
        return get_snaplist(serial)
    else:
        return "ERROR"

def get_health_info(args):
    pass

def get_snapstore_list(serial):
    dal_obj = DataAccessLayer("snap_store_db")
    snap_list = dal_obj.list()
    dal_obj = DataAccessLayer("thingpoint_db")
    thingpoint = dal_obj.get(serial)
    user_snap_list = thingpoint.get('user_snap_list',[])
    for snap in snap_list:
        snap["status"] = "install"
        for user_snap in user_snap_list:
            if snap["name"] == user_snap["name"]:
                snap["status"] = user_snap["status"]
    print("store snap list : {}".format(str(snap_list)))
    #snap_store_list = {}
    #snap_store_list['name'] = 
    return json.dumps(snap_list)

def get_thingpoint_status_count(args):
    dal_obj = DataAccessLayer("thingpoint_db")
    thingpoint_list = dal_obj.list()
    online = 0
    offline = 0
    total = 0
    for thingpoint in thingpoint_list:
        total = total + 1
        if thingpoint["conn_status"] is True:
            online = online + 1
        else:
            offline = offline + 1
    resp_dict = {}
    resp_dict["online"] = online
    resp_dict["offline"] = offline
    resp_dict["total"] = total
    print("thingpoint status count : {}".format(str(resp_dict)))
    return json.dumps(resp_dict)

def send_req_to_thingpoint(serial,snap_name, action):
    url = "https://us-central1-sage-buttress-230707.cloudfunctions.net/Thingpoint_handler"
    data = {"snap_name": snap_name, "message":"user/action","action":action, 'tpid':serial}

    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    r = requests.post(url, data=json.dumps(data), headers=headers)
    print(r.status_code)
    return


def delete_snap(serial,snap_name):
    dal_obj = DataAccessLayer("thingpoint_db")
    thingpoint = dal_obj.get(serial)
    system_snap_list = thingpoint['system_snap_list']
    for snap in system_snap_list:
        if snap["name"] == snap_name:
            snap["status"] = DELETING
            print("updated snaplist with status delete:{}".format(user_snap_list))
            dal_obj.update(system_snap_list,serial,'system_snap_list')
            send_req_to_thingpoint(serial,snap_name,"remove")
            return "Success"
        
    user_snap_list = thingpoint['user_snap_list']
    for snap in user_snap_list:
        if snap["name"] == snap_name:
            snap["status"] = DELETING
            print("updated snaplist with status delete:{}".format(user_snap_list))
            dal_obj.update(user_snap_list,serial,'user_snap_list')
            send_req_to_thingpoint(serial,snap_name,"remove")
            return "Success"
    return "snap is not installed"

def add_snap(serial,snap_name):
    dal_obj = DataAccessLayer("thingpoint_db")
    thingpoint = dal_obj.get(serial)
    user_snap_list = thingpoint['user_snap_list']
    for snap in user_snap_list:
        if snap["name"] == snap_name:
            print("already installed")
            return "already installed"
    new_snap = {}
    new_snap["status"] = INSTALLING
    new_snap["name"] = snap_name
    new_snap["type"] = "app"
    user_snap_list.append(new_snap)
    print("updated snaplist with status Installing:{}".format(user_snap_list))
    dal_obj.update(user_snap_list,serial,'user_snap_list')
    send_req_to_thingpoint(serial,snap_name,"install")
    return "Success"

def get_timeseries_list(doc_list, attribute):
    resp_list = []
    for doc in doc_list:
        item_dict = {}
        item_dict["ts"] = doc['timestamp']
        item_dict['value'] = doc[attribute]
        resp_list.append(item_dict)
    return resp_list


def get_timeseries_data(serial, attribute):
    tpid = serial.strip()
    collection = "thingpoint_"+tpid
    dal_obj = DataAccessLayer(collection)
    print("before start time")
    start_time = int(time.time()) - int(600)
    print("start_time : ", start_time)
    doc_list = dal_obj.list(limit=20, start_time=start_time)
    response = get_timeseries_list(doc_list,attribute)
    return json.dumps(response)

def delete_thingpointsnap(serial,snap_name):
    print("serial:{}, snap_name:{}".format(serial,snap_name))
    return delete_snap(serial,snap_name)

def add_thingpointsnap(serial,snap_name):
    print("serial:{}, snap_name:{}".format(serial,snap_name))
    return add_snap(serial,snap_name)

def get_att_timeseries(serial, attribute):
    print("serial:{}, attribute:{}".format(serial,attribute))
    return get_timeseries_data(serial, attribute)

def process_request(request_object):
    try:
        req = request_object.get_json()
        args = request_object.args.to_dict()
        # Set CORS headers for the preflight request
            # Allows GET requests from any origin with the Content-Type
            # header and caches preflight response for an 3600s
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'get,post,options',
            'Access-Control-Allow-Headers': 'x-requested-with, Content-Type, origin, authorization, accept, client-security-token',
            'Access-Control-Max-Age': '3600',
            'Access-Control-Allow-Credentials': 'true'
        }

        print("ARGS {}".format(args))
        if request_object.method == 'GET':
            if args['type'] == 'thingpointlist':
                response = get_thingpointlist(args)
                return(response, 200, headers)
            if args['type'] == 'snapbundleinfo':
                response =  get_snapbundle_info(args)
                return(response, 200, headers)
            if args['type'] == 'healthinfo':
                response =  get_health_info(args)
                return(response, 200, headers)
            if args['type'] == 'statuscount':
                response =  get_thingpoint_status_count(args)
                return(response, 200, headers)
            if args['type'] == 'snapstorelist':
                serial = args['serial']
                response =  get_snapstore_list(serial)
                return(response, 200, headers)
            #adding delete request on Get Method temporarily
            if args['type'] == 'deletesnap':
                serial = args['serial']
                snap_name = args['name']
                response =  delete_thingpointsnap(serial, snap_name)
                return(response, 200, headers)
            #adding delete request on Get Method temporarily
            if args['type'] == 'addsnap':
                serial = args['serial']
                snap_name = args['name']
                response =  add_thingpointsnap(serial, snap_name)
                return(response, 200, headers)
            #adding delete request on Get Method temporarily
            if args['type'] == 'healthchart': 
                serial = args['serial']
                attribute = args['attribute']
                response =  get_att_timeseries(serial, attribute)
                return(response, 200, headers)
            else:
                return (f'Invalid Request', 500, headers)
    
        elif request_object.method == 'DELETE':
            if args['type'] == 'deletesnap':
                content_type = request_object.headers['content-type']
                if content_type == 'application/json':
                    request_json = request_object.get_json(silent=True)
                    print("inside content type passed json :{}".format(request_json))
                    if request_json and 'serial' in request_json:
                        serial = request_json['serial']
                        snap_name = request_json['name']
                        response =  delete_thingpointsnap(serial, snap_name)
                        return(response, 200, headers)
            else:
                print("invalid arguement type")

        elif request_object.method == 'POST':
            if args['type'] == 'addsnap':
                print("debug-print called addsnap")
                content_type = request_object.headers['content-type']
                if content_type == 'application/json':
                    request_json = request_object.get_json(silent=True)
                    print("inside content type passed json :{}".format(request_json))
                    if request_json and 'serial' in request_json:
                        serial = request_json['serial']
                        snap_name = request_json['name']
                        response =  add_thingpointsnap(serial, snap_name)
                        return(response, 200, headers)

            if args['type'] == 'deletesnap':
                print("debug-print called deletesnap")
                content_type = request_object.headers['content-type']
                if content_type == 'application/json':
                    request_json = request_object.get_json(silent=True)
                    print("inside content type passed json :{}".format(request_json))
                    if request_json and 'serial' in request_json:
                        serial = request_json['serial']
                        snap_name = request_json['name']
                        response =  delete_thingpointsnap(serial, snap_name)
                        return(response, 200, headers)
            else:
                print("invalid arguement type for POST")

        elif request_object.method == 'OPTIONS':
            if args['type'] == 'addsnap':
                print("debug-print called addsnap:{}",request_object)
                print("debug-print called addsnap header:{}",request_object.headers)
                content_type = request_object.headers['content-type']
                if content_type == 'application/json':
                    request_json = request_object.get_json(silent=True)
                    print("inside content type passed json :{}".format(request_json))
                    if request_json and 'serial' in request_json:
                        serial = request_json['serial']
                        snap_name = request_json['name']
                        response =  add_thingpointsnap(serial, snap_name)
                        return(response, 200, headers)

            if args['type'] == 'deletesnap':
                print("debug-print called addsnap:{}",request_object)
                print("debug-print called addsnap header:{}",request_object.headers)
                content_type = request_object.headers['content-type']
                if content_type == 'application/json':
                    request_json = request_object.get_json(silent=True)
                    print("inside content type passed json :{}".format(request_json))
                    if request_json and 'serial' in request_json:
                        serial = request_json['serial']
                        snap_name = request_json['name']
                        response =  delete_thingpointsnap(serial, snap_name)
                        return(response, 200, headers)
            else:
                print("invalid arguement type for POST")
            return(response, 204, headers)
        

        return (f'Invalid Request', 500, headers)

    except Exception as e:
        print("Exception {}".format(e))
