from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('revisao').\
getOrCreate()

df=spark.read.csv('/home/virtual/Downloads/ABT.csv', \
                  inferSchema=True, header=True)

df.printSchema()


df.select('date','volume').show()

from pyspark.sql.functions import *

df = df.withColumn('new_column', df['volume'] * 2)
df = df.withColumn('new_column', lit(1))

#agrupamento(group by)

dfGroupBy = df.groupBy('date').sum()
dfGroupBy.show(3)

#ordenação (sort)

dfGroupBy.sort('sum(volume)').show(3)

dfGroupBy.sort(col('sum(volume)').desc()).show(3)

#valores nulos

df.na.drop( subset='volume' )
df.na.fill('S', subset=['volume','date'] )

#Spark SQL

from pyspark.sql import SQLContext

#mapeamento do Dataframe para uma 'tabela' SQL

df.createOrReplaceTempView('financialTable')
spark.sql ( "SELECT * FROM financialTable limit 10" ).\
    show(3)
    
dfMysql = df.toPandas()

import connections 

connMySQL = connections.getConnMySQL()
print(connMySQL)

df=spark.read.csv('/home/virtual/Downloads/ABT.csv', \
                  inferSchema=True, header=True)

df.printSchema()

dfAbt = df.toPandas()

for row in dfAbt.iterrows():
    data= str(row[1]['date'])
    volume=int(row[1]['volume'])
    open=float(row[1]['open'])
    close=str(row[1]['close'])
    high=float(row[1]['high'])
    low= float(row[1]['low'])
    adjclose= float(row[1]['adjclose'])

    insert = "INSERT INTO Afonso_junior.ABT VALUES ('{}', {},{}, {}, {},{}, {})".format(data, volume, open, high, low, close, adjclose)
    print(insert)
    cur = connMySQL.cursor()
    cur.execute(query)
    connMySQL.commit()  
    
