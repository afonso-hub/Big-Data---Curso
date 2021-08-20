import MySQLdb
import pandas as pd
import connections 

connMySQL =connections.getConnMySQL()

despachantes_csv ='/home/virtual/Downloads/despachantes.csv'
dfdespachantes= pd.read_csv(despachantes_csv)

for row in dfdespachantes.iterrows():
    ID_DESPACHANTE = int(row[1][0])
    NOME =str(row[1][1])
    STATUS =str(row[1][2])
    FILIAL =str(row[1][3])

    query = "INSERT INTO Afonso_junior.DESPACHANTES(ID_DESPACHANTE, NOME, STATUS, FILIAL) VALUES({},'{}','{}','{}')".format(ID_DESPACHANTE, NOME, STATUS, FILIAL)
    
    
    cur = connMySQL.cursor()
    cur.execute(query)
    connMySQL.commit()    
