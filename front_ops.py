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

a = 0

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
    msg_a = boto.sqs.message.Message()
    msg_b = boto.sqs.message.Message()

    # msg_a.set_body(msg)
    # print "msg_a_body"
    # print msg_a.get_body()
    # tempA = {'op' : 'write', 'favNumber' : 42, 'name' : 'brad'}
    # tempB = {'op' : 'write', 'favNumber' : 13, 'name' : 'aman'}

    # temp_json_A = json.dumps(tempA)
    # temp_json_B = json.dumps(tempB)
    temp_json_A = json.dumps(msg)
    temp_json_B = json.dumps(msg)
    msg_b.set_body(temp_json_B)
    msg_a.set_body(temp_json_A)
    result = send_msg_ob.send_msg(msg_a, msg_b)

    # Pass the called routine the response object to construct a response from

@get('/users/<id>')
def get_id_route(id):
    id = int(id) # In URI, id is a string and must be made int
    print "Retrieving id {0}\n".format(id)

    return retrieve_ops.retrieve_by_id(table, id, response)

@get('/names/<name>')
def get_name_route(name):
    name = str(name) # In URI, id is a string and must be made int
    print "Retrieving name {0}\n".format(name)

    return retrieve_ops.retrieve_by_name(table, name, response)

@get('/users')
def get_users_route():
   
    print "retrieve_users "
    
    return retrieve_ops.retrieve_by_users(table,response)

@delete('/users/<id>')
def delete_id_route(id):
    id = int(id)

    print "Deleting id {0}\n".format(id)

    return delete_ops.delete_by_id(table, id, response)

@delete('/names/<name>')
def delete_name_route(name):
    name = str(name)

    print "Deleting name {0}\n".format(name)

    return delete_ops.delete_by_name(table, name, response)

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

  # create_queue is idempotent---if queue exists, it simply connects to it
  global a3_in_a
  global a3_in_b
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

def is_first_response(id):
    # EXTEND:
    # Return True if this message is the first response to a request
    pass

def is_second_response(id):
    # EXTEND:
    # Return True if this message is the second response to a request
    pass

def get_response_action(id):
    # EXTEND:
    # Return the action for this message
    pass

def get_partner_response(id):
    # EXTEND:
    # Return the id of the partner for this message, if any
    pass

def mark_first_response(id):
    # EXTEND:
    # Update the data structures to note that the first response has been received
    pass

def mark_second_response(id):
    # EXTEND:
    # Update the data structures to note that the second response has been received
    pass

def clear_duplicate_response(id):
    # EXTEND:
    # Do anything necessary (if at all) when a duplicate response has been received
    pass

def set_dup_DS(action, sent_a, sent_b):
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
    pass
