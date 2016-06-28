#!/usr/bin/env python

'''
  Template for front end Web server for BasicDB assignment
'''

# Library packages
import sys

# Installed packages
import boto.dynamodb2
import boto.dynamodb2.table

from bottle import post, get, put, delete, run, request, response, default_app

# Local imports
import create_ops
import retrieve_ops
import delete_ops
import update_ops

# Configuration constants
AWS_REGION = "us-west-2"
TABLE_NAME = "activities"
DEFAULT_PORT = 8080

def abort(response, status, errors):
    response.status = status
    return {"errors": errors}

@post('/users')
def create_route():
    ct = request.get_header('content-type')
    if ct != 'application/json':
        return abort(response, 400, [
            "request content-type unacceptable:  body must be "
            "'application/json' (was '{0}')".format(ct)])
    id = request.json["id"] # In JSON, id is already an integer
    name = request.json["name"]

    print "creating id {0}, name {1}\n".format(id, name)

    # Pass the called routine the response object to construct a response from
    return create_ops.do_create(request, table, id, name, response)

@get('/users/<id>')
def get_id_route(id):
    id = int(id) # In URI, id is a string and must be made int
    print "Retrieving id {0}\n".format(id)

    return retrieve_ops.retrieve_by_id(table, id, response)

@delete('/users/<id>')
def delete_id_route(id):
    id = int(id)

    print "Deleting id {0}\n".format(id)

    return delete_ops.delete_by_id(table, id, response)
	
@delete('/users/</name>')
def delete_name_route(name):
    name = string(name)

    print "Deleting name {0}\n".format(name)

    return delete_ops.delete_by_name(table, name, response)

@put('/users/<id>/activities/<activity>')
def add_activity_route(id, activity):
    id = int(id)
    print "adding activity for id {0}, activity {1}\n".format(id, activity)
    
    return update_ops.add_activity(table, id, activity, response)

#  You can use the following without modification
def main():
    global table
    try:
        conn = boto.dynamodb2.connect_to_region(AWS_REGION)
        if conn == None:
            sys.stderr.write("Could not connect to AWS region '{0}'\n".format(AWS_REGION))
            sys.exit(1)

        table = boto.dynamodb2.table.Table(TABLE_NAME, connection=conn)
    except Exception as e:
        sys.stderr.write("Exception connecting to DynamoDB table {0}\n".format(TABLE_NAME))
        sys.stderr.write(str(e))
        sys.exit(1)

    if len(sys.argv) >= 2:
        port = int(sys.argv[1])
    else:
        port = DEFAULT_PORT

    app = default_app()
    run(app, host="localhost", port=port)

if __name__ == "__main__":
    main()
    
