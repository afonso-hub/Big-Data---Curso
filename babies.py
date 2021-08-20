from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('babies').\
getOrCreate()

path = '/home/virtual/Downloads/'

df= spark.read.csv(path + 'US_Baby_Names_right.csv', \
                       inferSchema=True, header=True)

#df = df.sample(false,0.1)

df.printSchema()

# 1 - Nasceram mais bebes do sexo masculino ou feminino

dfQ1 = df.groupBy('Gender').sum('Count')
dfQ1.columns

dfQ1 = dfQ1.\
    withColumn('percent', col('sum(Count)') / df.count() * 100)

dfQ1.show(2)

# 2-) para cada ano, nasceram mais bebes do sexo masculino ou feminino

dfQ2 = df.groupBy('Gender', 'Year').sum('Count')
dfQ2.sort(col('Year').desc() ).show(10)

# 3-) Quantos nomes diferentes existem na base?

df.select('Name').distinct().count()

# Quantas vezes aparece a menor quantidade de um nome?

dfQ4 = df.groupBy('Name').sum('Count').\
    sort( col('sum(Count)').asc() )
    
dfQ4.show()
    
dfQ4.where(col('sum(Count)') == 5).count()

dfQ4.where(col('Name') == 'Francella').show(10)

df.where(col('Name') == 'Francella').show()

