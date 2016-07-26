#!/usr/bin/env python

'''
  Back end DB server for Assignment 3, CMPT 474.
'''

# Local Imports
import create_ops

# Imports of unqualified names
from bottle import post, get, put, delete, request, response

# Library packages
import argparse
import json
import sys
import time

# Installed packages
import boto.dynamodb2
import boto.dynamodb2.table
import boto.sqs

AWS_REGION = "us-west-2"
TABLE_NAME_BASE = "activities"
Q_IN_NAME_BASE = "a3_back_in"
Q_OUT_NAME = "a3_out"

MAX_TIME_S = 3600 # One hour
MAX_WAIT_S = 20 # SQS sets max. of 20 s
DEFAULT_VIS_TIMEOUT_S = 60

def open_conn(region):
    conn = boto.sqs.connect_to_region(AWS_REGION)
    if conn == None:
        sys.stderr.write("Could not connect to AWS region '{0}'\n".format(AWS_REGION))
        sys.exit(1)
    return conn

def open_q(name, conn):
    return conn.create_queue(name)

def handle_args():
    argp = argparse.ArgumentParser(
        description="Backend for simple database")
    argp.add_argument('suffix', help="Suffix for queue base ({0}) and table base ({1})".format(Q_IN_NAME_BASE, TABLE_NAME_BASE))
    return argp.parse_args()

if __name__ == "__main__":
    args = handle_args()
    '''
       EXTEND:

       After the above statement, args.suffix holds the suffix to use
       for the input queue and the DynamoDB table.

       This main routine must be extended to:
       1. Connect to the appropriate queues
       2. Open the appropriate table
       3. Go into an infinite loop that
          - reads a requeat from the SQS queue, if available
          - handles the request idempotently if it is a duplicate
          - if this is the first time for this request
            * do the requested database operation
            * record the message id and response
            * put the response on the output queue
    '''

    conn = open_conn(AWS_REGION)
    q_in = open_q(Q_IN_NAME_BASE+sys.argv[1], conn)
    q_out = open_q(Q_OUT_NAME, conn)

    global table
    try:
        conn = boto.dynamodb2.connect_to_region(AWS_REGION)
        if conn == None:
            sys.stderr.write("Could not connect to AWS region '{0}'\n".format(AWS_REGION))
            sys.exit(1)

        table = boto.dynamodb2.table.Table(TABLE_NAME_BASE+sys.argv[1], connection=conn)
    except Exception as e:
        sys.stderr.write("Exception connecting to DynamoDB table {0}\n".format(TABLE_NAME_BASE+sys.argv[1]))
        sys.stderr.write(str(e))
        sys.exit(1)

    wait_start = time.time()
    while True:
        msg_in = q_in.read(wait_time_seconds=MAX_WAIT_S, visibility_timeout=DEFAULT_VIS_TIMEOUT_S)
        if msg_in:
            body = json.loads(msg_in.get_body())
            msg_id = body['msg_id']
            print "Message ID: "
            print msg_id
            if (body['METHOD'] == 'POST' and body['ROUTE'] == 'users'):
                result = create_ops.do_create(request, table, body['id'], body['name'], response)
                q_in.delete_message(msg_in)
                result.update({'msg_id':body['msg_id']})

                msg_res = json.dumps(result)
                json_res = boto.sqs.message.Message()
                json_res.set_body(msg_res)
                print json_res.get_body()
                q_out.write(json_res)

            wait_start = time.time()
        elif time.time() - wait_start > MAX_TIME_S:
            print "\nNo messages on input queue for {0} seconds. Server no longer reading response queue {1}.".format(MAX_TIME_S, q_out.name)
            break
        else:
            pass
