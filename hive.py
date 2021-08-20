from pyhive import hive
import connections
from datetime import datetime
import pandas as pd






# def getConnHive():
    
#     #local
#     connHive = hive.Connection(host='ffborelli.ddns.net', 
#     port=10000, 
#         username='hive',password='hive', auth='CUSTOM')
#     return connHive

#def main(args):
    
#print('a')
connHive = connections.getConnHive()
#print(connHive)

# dfHive = pd.read_sql("SELECT * FROM big_data.afonso_junior limit 3", connHive)
    
# print (datetime.now().strftime('%Y-%m-%d %H:%M:%S') + " --- {} Registros Consultados do Apache Hive ".format(dfHive.size) )

# query = 'INSERT INTO big_data.afonso_junior( `date`, volume, `open`, high, low, `close`, adjclose ) VAlUES ("2021-08-03", 47,25,31,22,30,30.55)'

# aula dia 4 de agosto 
# cur = connHive.cursor()
# cur.execute(query)
# connHive.commit()

#pegar os dados da tabela stock_opcoes
# dfStockOpcoes = pd.read_sql("SELECT * FROM big_data.stock_opcoes limit 1", connHive)

# print( dfStockOpcoes )

# processar os dados -> dobrar os valores das colunas, com excecao da data 
# for row in dfStockOpcoes.iterrows():
    #print(row[1] ['stock_opcoes.volume'])
    #print(row[1] ['stock_opcoes.data'])
    
    #ins = "INSERT INTO big_data.afonso_junior VALUES ('"+row[1]['stock_opcoes.data']+"', " + str(10 * row[1]['stock_opcoes.volume'])  + " , " + str(10 * row[1]['stock_opcoes.abertura']) + ", " + str(10* row[1]['stock_opcoes.maxima'])+ ", " +str( 10 *row[1]['stock_opcoes.minima'])+ " , " + str (10*  row[1]['stock_opcoes.fechamento'])+ " , " +str(10*row[1]['stock_opcoes.ajuste'])+ ")"
    #print (ins)

    #cria o cursor e insere os dados no hive
    #cur = connHive.cursor()
    #cur.execute(ins)
    #connHive.commit()

#Correcao exercicio

# dfUserData = pd.read_sql ( "SELECT * FROM big_data.afonso_junior limit 10", connHive )
# print(dfUserData)

#primeiro jeito: solucao bracal
#try:
    #codigo python
#except:
    #codigo python

# se a quantidade de linhas for igual a 0
# if( dfUserData.shape[0] == 0):
#     print("Nao ha linhas no dataframe")
#     import sys
#     sys.exit(0)

# vol_min = dfUserData['afonso_junior.volume'][0]
# vol_max = dfUserData['afonso_junior.volume'][0]
# abert_min = dfUserData['afonso_junior.open'][0]
# abert_max = dfUserData['afonso_junior.open'][0]
# fecha_min = dfUserData['afonso_junior.close'][0]
# fecha_max = dfUserData['afonso_junior.close'][0]
# aluno='afonso_junior'

# for row in dfUserData.iterrows():
#     if (row[1]['afonso_junior.volume'] < vol_min):
#         vol_min = row[1]['afonso_junior.volume']
    
#     if ( row[1]['afonso_junior.open'] < abert_min ):
#         abert_min = row[1]['afonso_junior.open']

#     if ( row[1]['afonso_junior.close'] < fecha_min):
#         fecha_min = row[1]['afonso_junior.close']

#     if ( row[1]['afonso_junior.volume'] > vol_max):
#         vol_max = row[1]['afonso_junior.volume']
    
#     if( row[1]['afonso_junior.open'] > abert_max):
#         abert_max = row[1]['afonso_junior.open']

#     if ( row[1]['afonso_junior.close'] > fecha_max):
#         fecha_max = row[1]['afonso_junior.close']
    
# #insert = "INSERT INTO big_data.estatisticas VALUES ( 'Fabrizio Borelli', {vol_min}, {vol_max}, {abert_min}, {abert_max}, {fecha_min}, {fecha_max} )".format( fecha_max=fecha_max, fecha_min=fecha_min, vol_min= vol_min, abert_max=abert_max, abert_min=abert_min )
# insert = "INSERT INTO big_data.estatisticas VALUES ( '{aluno}', {vol_min}, {vol_max}, {abert_min}, {abert_max}, {fecha_min}, {fecha_max} )".format( fecha_max=fecha_max, fecha_min=fecha_min, vol_min=vol_min, vol_max=vol_max, abert_max=abert_max, abert_min=abert_min, aluno=aluno )
# print (insert)
# cur = connHive.cursor()
# cur.execute(insert)
# connHive.commit()

dfUserData = pd.read_sql ( "SELECT * FROM big_data.afonso_junior limit 10", connHive )
print(dfUserData)

# segunda forma usando Frimework

vol_min = str ( dfUserData['afonso_junior.volume'].min() )
vol_max = str ( dfUserData['afonso_junior.volume'].max() )
abert_min = str ( dfUserData['afonso_junior.open'].min() )
abert_max = str ( dfUserData['afonso_junior.open'].max() )
fecha_min = str ( dfUserData['afonso_junior.close'].min() ) 
fecha_max = str ( dfUserData['afonso_junior.close'].max() )
aluno = 'afonso_junior'
#insert = "INSERT INTO big_data.estatisticas VALUES ( 'Fabrizio Borelli', {vol_min}, {vol_max}, {abert_min}, {abert_max}, {fecha_min}, {fecha_max} )".format( fecha_max=fecha_max, fecha_min=fecha_min, vol_min= vol_min, abert_max=abert_max, abert_min=abert_min )
insert = "INSERT INTO big_data.estatisticas VALUES ( '{aluno}', {vol_min}, {vol_max}, {abert_min}, {abert_max}, {fecha_min}, {fecha_max} )".format( fecha_max=fecha_max, fecha_min=fecha_min, vol_min=vol_min, vol_max=vol_max, abert_max=abert_max, abert_min=abert_min, aluno=aluno )
print (insert)
cur = connHive.cursor()
cur.execute(insert)
connHive.commit()