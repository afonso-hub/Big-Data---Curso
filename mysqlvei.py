import MySQLdb
import pandas as pd
import connections 

connMySQL =connections.getConnMySQL()

veiculos_csv ='/home/virtual/Downloads/veiculos.csv'
dfVeiculos = pd.read_csv(veiculos_csv)

for row in dfVeiculos.iterrows():
    ID_VEICULO = int(row[1][0])
    DATA_AQUISICAO =str(row[1][1])
    ANO =str(row[1][2])
    MODELO =str(row[1][3])
    PLACA =str(row[1][4])
    STATUS =str(row[1][5])
    DIARIA = str(row[1][6])

    query = "INSERT INTO Afonso_junior.VEICULOS(ID_VEICULO, DATA_AQUISICAO, ANO, MODELO, PLACA, STATUS, DIARIA) VALUES ( {}, '{}', '{}', '{}', '{}', '{}', '{}')".format(ID_VEICULO, DATA_AQUISICAO, ANO, MODELO, PLACA, STATUS, DIARIA)
    print (query)
    cur = connMySQL.cursor()
    cur.execute(query)
    connMySQL.commit()    
