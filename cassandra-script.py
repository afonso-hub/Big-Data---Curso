from os import close, name
from datetime import date
from os import close
# from cassandra.cluster import Cluster
import pandas as pd
# import MySQLdb
#importanto modulo inteiro 
import connections
#from connections import getConnMYSQL

# csvfile = open('/home/virtual/Downloads/ABT.csv', "r")
# csvreader = csv.reader(csvfile)

# def getConnMySQL():
#      ConnMySQL = MySQLdb.connect(
#          host='177.104.61.65',
#          user='fgv',
#          passwd='fgv',
#          db='stockfgv'

#      )

#      return ConnMySQL

def readFromMysql():

    connMySQL = connections.getConnMySQL()
    dfAcoes= pd .read_sql("""SELECT stocks.date_, stocks.close, users.username, portfolio.shares, portfolio.symbol FROM stockfgv.portfolio 
	                       RIGHT JOIN users  ON stockfgv.portfolio.user_id = users.id INNER JOIN stocks ON portfolio.symbol = stocks.symbol 
                            WHERE stocks.date_ = '2020-05-22'""",connMySQL)
    return dfAcoes

    # print(dfAcoes)

    # print(connMySQL)

# cluster = Cluster(['ffborelli.ddns.net'])
# session = cluster.connect()
# print(session)

def insertCassandra(dfAcoes):
    
   session = connections.getConnCassandra()

   for row in dfAcoes.iterrows():
      date= (str(row[1]['date_']))
      username=(str(row[1]['username']))
      symbol=(str(row[1]['symbol']))
      shares =(str(row[1]['shares']))
      preco =(str(row[1]['close']))
    
      query = "INSERT INTO afonso_junior.portfolio_user_value (id, name, symbol, shares, preco, date) VALUES (uuid(), '{}', '{}', {}, {}, '{}')".format(username,symbol,shares,preco, date)
    
      print(query)  
      session.execute(query)

 
dfAcoesMysql = readFromMysql()
insertCassandra( dfAcoesMysql )
print(__name__)



# for row in rows:
#     print(row)

# session.execute(
#     """INSERT INTO big_data.Afonso_junior (id, date, volume, open, 
# 	    high, low, close, adjclose) VALUES (uuid(), '2020-07-02', 7690600 , 185.0, 186.119995117, 183.86000061 , 184.460006714 , 184.460006714)
#     """
# ) 
