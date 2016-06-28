''' Retrieve operations for CMPT 474 Assignment 2 '''
from boto.dynamodb2.items import Item
from boto.dynamodb2.exceptions import ItemNotFound

def retrieve_by_id(table, id, response):
  try:
    item = table.get_item(id=id)
    # itemdb = table.scan()
    response.status = 200

    if item['activities'] == None:
      return {"data": {
        "type": "person",
        "id": id,
        "name": item['name'],
        "activities": []
        }
      }
    else:
      activityList = list(item['activities'])
      return {"data": {
        "type": "person",
        "id": id,
        "name": item['name'],
        "activities": activityList
        }
      }

  except ItemNotFound as inf:
    response.status = 404
    return {"errors": [{
        "retrieve by id not implemented": {"id": id}
        }]}
