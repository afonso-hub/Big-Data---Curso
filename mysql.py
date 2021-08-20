import MySQLdb
import pandas as pd
import connections 

connMySQL =connections.getConnMySQL()

clientes_csv ='/home/virtual/Downloads/clientes.csv'
dfClientes = pd.read_csv(clientes_csv)

for row in dfClientes.iterrows():
    ID_CLIENTE = int(row[1][0])
    CPF =str(row[1][1])
    CNH =str(row[1][2])
    VALIDADECNH =str(row[1][3])
    NOME =str(row[1][4])
    DATACADASTRO =str(row[1][5])
    DATANASCIMENTO = str(row[1][6])
    TELEFONE = str(row[1][7])
    STATUS = str(row[1][8])

    query = "INSERT INTO Afonso_junior.CLIENTES(ID_CLIENTE, CPF, CNH, VALIDADECNH, NOME, DATACADASTRO, DATANASCIMENTO, TELEFONE, STATUS) VALUES ( {}, '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')".format(ID_CLIENTE, CPF, CNH, VALIDADECNH, NOME, DATACADASTRO, DATANASCIMENTO, TELEFONE, STATUS)
    print (query)
    cur = connMySQL.cursor()
    cur.execute(query)
    connMySQL.commit()    
