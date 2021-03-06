''' Retrieve operations for CMPT 474 Assignment 2 '''

from boto.dynamodb2.items import Item
from boto.dynamodb2.exceptions import ItemNotFound

def add_activity(table, id, activity, response):
    try:
        item = table.get_item(id=id)
        response.status = 200
        p = set()
        if item['activities'] == None:
            p.add(str(activity))
            item['activities'] = p
            item.save()
            return {"data": {
                "type": "person",
                "id": id,
                "added": [str(activity)]
                }
            }
        for i in item['activities']:
            if i == activity:
                return {"data": {
                    "type": "person",
                    "id": id,
                    "added": []
                    }
                }
        for i in item['activities']:
            p.add(str(i))
        p.add(str(activity))
        item['activities'] = p
        print(item['activities'])
        item.save()
        return {"data": {
            "type": "person",
            "id": id,
            "added": [str(activity)]
            }
        }

    except ItemNotFound as inf:
        response.status = 404
        return {"errors": [{
            "not_found": {
                "id" : id
                }
            }]
        }

def delete_activity(table, id, activity, response):
    try:
        item = table.get_item(id=id)
        difSet = set()
        difSet.add(str(activity))
        originalSet = set()
        if item['activities'] == None:
            response.status = 404
            return {"errors": [{
                "not_found": {
                    "id" : id
                    }
                }]
            }
        for i in item['activities']:
            originalSet.add(str(i))
        newSet = originalSet.difference(difSet)
        if newSet == originalSet:
            return {"errors": [{
                "not_found": {
                    "id" : id
                    }
                }]
            }
        item['activities'] = newSet
        item.save()
        return {"data": {
            "type": "person",
            "id": id,
            "deleted": [str(activity)]
        }}

    except ItemNotFound as inf:
        response.status = 404
        return {"errors": [{
            "not_found": {
                "id" : id
                }
            }]
        }
