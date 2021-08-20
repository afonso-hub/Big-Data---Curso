import MySQLdb
import pandas as pd
import connections 

connMySQL =connections.getConnMySQL()

locacao_csv ='/home/virtual/Downloads/locacao.csv'
dfLocacao = pd.read_csv(locacao_csv)

for row in dfLocacao.iterrows():
    ID_LOCACAO = int(row[1][0])
    ID_CLIENTE =str(row[1][1])
    ID_DESPACHANTE =str(row[1][2])
    ID_VEICULO =str(row[1][3])
    DATALOCACAO =str(row[1][4])
    DATAENTREGA =str(row[1][5])
    TOTAL = str(row[1][6])
    
    query = "INSERT INTO Afonso_junior.LOCACAO(ID_LOCACAO, ID_CLIENTE, ID_DESPACHANTE, ID_VEICULO, DATALOCACAO, DATAENTREGA, TOTAL) VALUES ( {}, '{}', '{}', '{}', '{}', '{}', '{}')".format(ID_LOCACAO, ID_CLIENTE, ID_DESPACHANTE, ID_VEICULO, DATALOCACAO, DATAENTREGA, TOTAL)
    print (query)
    cur = connMySQL.cursor()
    cur.execute(query)
    connMySQL.commit()    