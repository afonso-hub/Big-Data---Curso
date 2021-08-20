import MySQLdb
from cassandra.cluster import Cluster
from pymongo import MongoClient
from pyhive import hive

def getConnMySQL():
     print(__name__)
     ConnMySQL = MySQLdb.connect(
         host='ffborelli.ddns.net',
         #?allowPublicKeyRetrieval=true&useSSL=false
         user='root',
         passwd='root',
         db='Afonso_junior',
         use_unicode=True,
         charset='utf8'
     )

     return ConnMySQL

def getConnCassandra():
    cluster = Cluster(['ffborelli.ddns.net'])
    session = cluster.connect()

    return session


def getConnCliente():
   client = MongoClient('ffborelli.ddns.net', 27017)
   
   return client

def getConnHive():
    
    #local
    connHive = hive.Connection(host='ffborelli.ddns.net', 
    port=10000, 
        username='hive',password='hive', auth='CUSTOM')
    return connHive  