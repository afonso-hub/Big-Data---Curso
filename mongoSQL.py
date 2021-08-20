from pymongo import MongoClient
import pandas as pd
from  connections import getConnCliente
from connections import getConnMySQL


    
connMySQL = getConnMySQL()
dfStocksFgv = pd .read_sql("select * from  stockfgv.stocks limit 15", connMySQL)
print(dfStocksFgv)

connMySQL.close()



db = getConnCliente().big_data

collection = db.Afonso_vieira

all = collection.find( {  } ) 

one = collection.find_one( { "id":3  } )
print(one)
   
for row in dfStocksFgv.iterrows():
    data= str(row[1]['date_'])
    volume=str(row[1]['volume'])
    open=str(row[1]['open'])
    close=str(row[1]['close'])
    high=str(row[1]['high'])
    low=str(row[1]['low'])
    adjclose= str(row[1]['adjclose'])

    collection.insert({
        "data" : data,
        "volume" : volume,
        "open" : open, 
        "close" : close,
        "high" : high,
        "low" : low,
        "adjclose" : adjclose
    })  
