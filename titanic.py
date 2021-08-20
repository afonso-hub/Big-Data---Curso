from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import connections 
from pyspark.sql.functions import *
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName('revisao').\
getOrCreate()

dfMysql2 = df.toPandas()

connMySQL = connections.getConnMySQL()
print(connMySQL)


spark = SparkSession.builder.appName('titanic-ingest').\
    getOrCreate()
    
def convertNanToNull(val):
    print(val)
    if (val == None):
        return 'NULL'
    
    if val == 'nan':
        return 'NULL'
    
    return str(int(val))

convertNanToNullUDF = udf( lambda x:convertNanToNull(x), \
                          StringType()  )

def checkColsWithNullValues( df ):
    for row in df.columns:
        #print(row)
        isNull = df.filter( col(row).isNull() ).count()
        print( row , str(isNull))
    
df = spark.read.\
    csv('/home/virtual/Downloads/titanic_train.csv', \
        inferSchema=True, header=True)
  
# checkColsWithNullValues()

# tratando aspas da coluna nome
df = df.withColumn('Name', regexp_replace( 'Name', """['"]""" , '' ))

# colocando valor NULL quando nao existe a idade
df = df.withColumn('Age', convertNanToNullUDF(df['Age']))

# colocando valor SN quando nao existe Embarque, Cabine
df = df.na.fill('SN', subset=['Embarked', 'Cabin']  )


dfPandas = df.toPandas()
#df.show(3)

insert = " INSERT INTO Afonso_junior.titanic VALUES "
values_list = []

for row in dfPandas.iterrows():
    #print(row)
    values = "( {},{},{},'{}','{}',{},{},{},'{}',{},'{}','{}' )".format( str(row[1][0]), str(row[1][1]),str(row[1][2]), str(row[1][3]), str(row[1][4]),   row[1][5]  ,str(row[1][6]), str(row[1][7]),str(row[1][8]), str(row[1][9]),str(row[1][10]), str(row[1][11]))
                
    values_list.append(values)

query = insert + ','.join(values_list)
print(query)

import connections
connMySQL = connections.getConnMySQL()

cursor = connMySQL.cursor()
cursor.execute(query)
connMySQL.commit()    
