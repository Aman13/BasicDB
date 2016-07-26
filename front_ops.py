'''
   Ops for the frontend of Assignment 3, Summer 2016 CMPT 474.
'''

# Standard library packages
import json
import os

# Installed packages
import boto.sqs

# Imports of unqualified names
from bottle import post, get, put, delete, request, response

import boto.dynamodb2
import boto.dynamodb2.table
import boto.sqs

# Local modules
import SendMsg
import create_ops
import retrieve_ops
import delete_ops
import update_ops

# Constants
AWS_REGION = "us-west-2"

Q_IN_NAME_BASE = 'a3_in'
Q_OUT_NAME = 'a3_out'

# Respond to health check
@get('/')
def health_check():
    response.status = 200
    return "Healthy"

'''
# EXTEND:
# Define all the other REST operations here ...
@post('/users')
def create_route():
    pass
'''
@post('/users')
def create_route():
    ct = request.get_header('content-type')
    if ct != 'application/json':
        return abort(response, 400, [
            "request content-type unacceptable:  body must be "
            "'application/json' (was '{0}')".format(ct)])
    msg = request.json
    print request.urlparts.netloc
    msg.update({'METHOD':'POST'})
    msg.update({'ROUTE':'users'})
    json_body_A = json.dumps(msg)
    print json_body_A
    msg_a.set_body(json_body_A)
    result = send_msg_ob.send_msg(msg_a, msg_a)
    print "RESULT: "
    print result


    # Pass the called routine the response object to construct a response from

@get('/users/<id>')
def get_id_route(id):
    id = int(id) # In URI, id is a string and must be made int
    print "Retrieving id {0}\n".format(id)

    msg = {'METHOD' : 'GET', 'ROUTE' : 'users/id', 'ID' : id}
    json_body = json.dumps(msg)
    msg_a.set_body(json_body)
    result = send_msg_ob.send_msg(msg_a, msg_a)
    return result

@get('/names/<name>')
def get_name_route(name):
    name = str(name) # In URI, id is a string and must be made int
    print "Retrieving name {0}\n".format(name)

    msg = {'METHOD' : 'GET', 'ROUTE' : 'users/names', 'NAME' : name}
    json_body = json.dumps(msg)
    msg_a.set_body(json_body)
    result = send_msg_ob.send_msg(msg_a, msg_a)
    return result

@get('/users')
def get_users_route():

    msg = {'METHOD' : 'GET', 'ROUTE' : 'users'}
    json_body = json.dumps(msg)
    msg_a.set_body(json_body)
    result = send_msg_ob.send_msg(msg_a, msg_a)
    return result

@delete('/users/<id>')
def delete_id_route(id):
    id = int(id)
    print "Deleting id {0}\n".format(id)
    msg = {'METHOD':'delete', 'ROUTE':'users/id', 'id':id}
    json_body_A = json.dumps(msg)
    print json_body_A
    msg_a.set_body(json_body_A)
    result = send_msg_ob.send_msg(msg_a, msg_a)

@delete('/names/<name>')
def delete_name_route(name):
    name = str(name)
    print "Deleting name {0}\n".format(name)
    msg = {'METHOD':'delete', 'ROUTE':'users/name', 'name':name}
    json_body_A = json.dumps(msg)
    print json_body_A
    msg_a.set_body(json_body_A)
    result = send_msg_ob.send_msg(msg_a, msg_a)

@delete('/users/<id>/activities/<activity>')
def delete_activity_route(id, activity):
    id = int(id)
    print "deleting activity for id {0}, activity {1}\n".format(id, activity)

    return update_ops.delete_activity(table, id, activity, response)

@put('/users/<id>/activities/<activity>')
def add_activity_route(id, activity):
    id = int(id)
    print "adding activity for id {0}, activity {1}\n".format(id, activity)

    return update_ops.add_activity(table, id, activity, response)


'''
   Boilerplate: Do not modify the following function. It
   is called by frontend.py to inject the names of the two
   routines you write in this module into the SendMsg
   object.  See the comments in SendMsg.py for why
   we need to use this awkward construction.

   This function creates the global object send_msg_ob.

   To send messages to the two backend instances, call

       send_msg_ob.send_msg(msg_a, msg_b)

   where

       msg_a is the boto.message.Message() you wish to send to a3_in_a.
       msg_b is the boto.message.Message() you wish to send to a3_in_b.

       These must be *distinct objects*. Their contents should be identical.
'''
def set_send_msg(send_msg_ob_p):
    global send_msg_ob
    send_msg_ob = send_msg_ob_p.setup(write_to_queues, set_dup_DS)
'''
   EXTEND:
   Set up the input queues and output queue here
   The output queue reference must be stored in the variable q_out
'''

try:
    conn = boto.sqs.connect_to_region(AWS_REGION)
    if conn == None:
        sys.stderr.write("Could not connect to AWS region '{0}'\n".format(AWS_REGION))
        sys.exit(1)

  # create global message variables
    global msg_a
    global msg_b
    actions = []
    responsechecks = []
    msg_a = boto.sqs.message.Message()
    msg_b = boto.sqs.message.Message()

    # create_queue is idempotent---if queue exists, it simply connects to it
    global a3_in_a
    global a3_in_b
    global q_out
    a3_in_a = conn.create_queue(Q_IN_NAME_BASE+"_a")
    a3_in_b = conn.create_queue(Q_IN_NAME_BASE+"_b")
    q_out = conn.create_queue(Q_OUT_NAME)
except Exception as e:
    sys.stderr.write("Exception connecting to SQS\n")
    sys.stderr.write(str(e))
    sys.exit(1)

def write_to_queues(msg_a, msg_b):
    # EXTEND:
    # Send msg_a to a3_in_a and msg-b to a3_in_b

    a3_in_a.write(msg_a)
    a3_in_b.write(msg_b)

'''
   EXTEND:
   Manage the data structures for detecting the first and second
   responses and any duplicate responses.
'''

# Define any necessary data structures globally here

actions = []
responsechecks = []

def is_first_response(id):
    # EXTEND:
    # Return True if this message is the first response to a request
    for i in responsechecks:
        if i['id1'] == id or i['id2'] == id:
            if i['first_response'] == False:
                return True
            else:
                return False
    return False

def is_second_response(id):
    # EXTEND:
    # Return True if this message is the second response to a request
    for i in responsechecks:
        if i['id1'] == id or i['id2'] == id:
            if i['second_response'] == False:
                return True
            else:
                return False
    return False

def get_response_action(id):
    # EXTEND:
    # Return the action for this message
    for i in actions:
        if i['id1'] == id or i['id2'] == id:
            return i['action']
    #May need a default return statement for the function to compile

def get_partner_response(id):
    # EXTEND:
    # Return the id of the partner for this message, if any
    for i in responsechecks:
        if i['id1'] == id:
            return i['id2']
        elif i['id2'] == id:
            return i['id1']
    return id

def mark_first_response(id):
    global responsechecks
	# EXTEND:
    # Update the data structures to note that the first response has been received
    for i in responsechecks:
        if i['id1'] == id or i['id2'] == id:
            i['first_response'] = True

def mark_second_response(id):
    global responsechecks
	# EXTEND:
    # Update the data structures to note that the second response has been received
    for i in responsechecks:
        if i['id1'] == id or i['id2'] == id:
            i['second_response'] = True

def clear_duplicate_response(id):
    # EXTEND:
    # Do anything necessary (if at all) when a duplicate response has been received
    pass

def set_dup_DS(action, sent_a, sent_b):
    global actions
	global responsechecks
	
	'''
       EXTEND:
       Set up the data structures to identify and detect duplicates
       action: The action to perform on receipt of the response.
               Opaque data type: Simply save it, do not interpret it.
       sent_a: The boto.sqs.message.Message() that was sent to a3_in_a.
       sent_b: The boto.sqs.message.Message() that was sent to a3_in_b.

               The .id field of each of these is the message ID assigned
               by SQS to each message.  These ids will be in the
               msg_id attribute of the JSON object returned by the
               response from the backend code that you write.
    '''

    dict = {'id1': sent_a.id, 'id2': sent_b.id, 'action': action}
    actions.append(dict)
    dict_2 = {'id1': sent_a.id, 'id2': sent_b.id, 'first_response': False, 'second_response': False,}
    responsechecks.append(dict_2)
    pass
