
from connections import getConnCliente

db = getConnCliente().big_data

collection = db.Afonso_vieira

all = collection.find( { "id" : 3 } ) 
for  obj in all:
    print(obj)

    one = collection.find_one( { "id":3  } )
    print(one)

# insert_obj = collection.insert( { "id" : 6, "name": "Name 1" } )
# print(insert_obj)

# objs = [ { " id": 7, " name": " Name 7"} , { "id":8, "name": "Name 8" } ]
# insert_obj = collection.insert( objs )

# updates = collection.update(
#      {"id": 1},

#      {
#          "$set":{
#              "name": "Junior"
#          }

#      }
#  )

# print(updates)
# 
#collection.delete_many( {"id":6})