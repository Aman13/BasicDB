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

def retrieve_by_name(table, name, response):

    itemdb = table.scan()
    for i in itemdb:
        iname = str(i["name"])

        if iname==name:
            itemid = int(i["id"]) 
            response.status = 200 # Found

            if i['activities'] == None:
              return {"data": {
                      "type": "person",
                      "id": itemid,
                      "name": name,
                      "activities": []
                      }
                  }
            else:
              activityList = list(i["activities"])
              return {"data": {
                      "type": "person",
                      "id": itemid,
                      "name": name,
                      "activities": activityList
                      }
                  }
  
    response.status = 404 #Not Found
    return {"errors": [{
                "not_found": {
                    "name": name
                }
            }]
        }
