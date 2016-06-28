''' Delete operations for Assignment 2 of CMPT 474 '''

# Installed packages

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
