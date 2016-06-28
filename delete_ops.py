''' Delete operations for Assignment 2 of CMPT 474 '''

# Installed packages

from boto.dynamodb2.items import Item
from boto.dynamodb2.exceptions import ItemNotFound

def delete_by_id(table, id, response):
	try:
		item = table.get_item(id=id)
		response.status = 200 #Found
		itemid = id
		item.delete()
		return {"data": {
					"type": "person",
					"id": itemid
				}
			}
		
	except ItemNotFound as inf:
		response.status = 404 #Not Found
		return {"errors": [{
					"not_found": {
						"id": id
					}
				}]
			}

def delete_by_name(table, name, response):

	itemdb = table.scan()
	print name
	for i in itemdb:
		iname = str(i["name"])
		
		if iname==name:
			itemid = int(i["id"]) 
			theitem = table.get_item(id=itemid)
			response.status = 200 # Found
			theitem.delete()
			return {"data": {
						"type": "person",
						"id": itemid
					}
				}
	
	reponse.status = 404 #Not Found
	return {"errors": [{
				"not_found": {
					"name": name
				}
			}]
		}