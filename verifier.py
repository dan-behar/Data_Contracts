import yaml
from datetime import datetime
import sys
import pyodbc
import os

CON_SERVER = os.environ['server']
CON_DATABASE = os.environ['database']
CON_USERNAME = os.environ['username']
CON_PASS = os.environ['password']

arg = 'DRIVER={SQL Server};SERVER='+CON_SERVER+';DATABASE='+CON_DATABASE+';uid='+CON_USERNAME+';pwd='+CON_PASS+';'

# Stablishing connection with DB (using pyodbc)
conn = pyodbc.connect(arg)

# Contract enforcer
def enforcerSQL(yaml):

    # Defining returns
    now = datetime.now()
    categ_n = ""
    numer_n = ""
    nulls_n = ""
    nonexist = ""
    exists = True

    # Cycle to move in all the columns that the DC defines
    for i in range (len(yaml['columns'])):

        columna = yaml['columns'][i]['column']
        valores = yaml['columns'][i]['values']
        nva = []

        # Validator for categorical columns
        if yaml['columns'][i]['isCategorical']:
            try:
                qry = pyodbc.query(f'SELECT DISTINCT {columna} FROM {yaml["tableName"]}').fetchall()
                for i in range(len(qry)):
                    nva.append(qry[i][0])
                if len(list(set(nva).difference(valores))) == 0:
                    pass
                else:
                    categ_n = categ_n + f"{columna}, "
            except:
                nonexist = nonexist + f"{columna}, "
                exists = False
        
        # Validator for non categorical columns
        else:
            try:
                qry = pyodbc.query(f'''SELECT {columna} FROM {yaml["tableName"]}
                                    WHERE {columna} < {valores[0]} OR {columna} > {valores[1]}''').fetchall()
                if len(qry) != 0:
                    numer_n = numer_n + f"{columna}, "
                else:
                    pass
            except:
                nonexist = nonexist + f"{columna}, "
                exists = False

        # Checking for nulls
        if exists:
            nulls = pyodbc.query(f'''select {columna} from {yaml["tableName"]} 
                        WHERE {columna} IS NULL''').fetchall()
            if len(nulls) != 0:
                nulls_n = nulls_n + f"{columna}, "
            else:
                pass
        else:
            exists = True
    

    if len(categ_n) == 0:
        categ_n = "All good"
    if len(numer_n) == 0:
        numer_n = "All good"
    if len(nulls_n) == 0:
        nulls_n = "All good"
    if len(nonexist) == 0:
        nonexist = "All good"

    return now, yaml["tableName"], categ_n, numer_n, nulls_n, nonexist

# Getting the details of the contract in the YAML
try:
    with open(sys.argv[1], 'rb') as f:
        conf = yaml.safe_load(f.read())
except:
    raise TypeError("No contract received")

# Executing the contract and saving it in the result table
lista = enforcerSQL(conf)
insert_query = f"""
    INSERT INTO resultados
    (fecha_val_contrato, table_name, categorical, numerical, nulls, doesnt_exist)
    VALUES
    ('{lista[0]}', '{lista[1]}', 
    '{lista[2]}', '{lista[3]}', 
    '{lista[4]}', '{lista[5]}')
"""

conn.execute(insert_query)
conn.close()
print("Process concluded...")