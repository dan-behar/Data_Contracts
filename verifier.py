import yaml
from datetime import datetime
import sys
import pandas as pd
import logging
import pyodbc
import os
from dotenv import load_dotenv
load_dotenv()

# Stablishing connection with DB using .env file
CON_SERVER = os.environ['server']
CON_DATABASE = os.environ['database']
CON_USERNAME = os.environ['username']
CON_PASS = os.environ['password']

arg = 'DRIVER={SQL Server};SERVER='+CON_SERVER+';DATABASE='+CON_DATABASE+';uid='+CON_USERNAME+';pwd='+CON_PASS+';'
conn = pyodbc.connect(arg)
cursor = conn.cursor()

# Log file basic configuration
logging.basicConfig(level = logging.INFO, filename = 'ContractFiles.log')
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# Data base instert executer
def SQLWriter(contract, table, problem, column):
    """
    Writes into the table RESULTADOS when something is wrong in the contract
    Args:
        contract (int): The contract ID that failed
        table (str): The table that have a problem
        problem (str): The type of problem detected
        column (str): The column that is being affected by the problem
    Returns:
        1 when the execution is over
    """

    now = datetime.now()
    formatted_date = now.strftime('%Y-%m-%d %H:%M:%S')
    
    insert_query = f"""
    INSERT INTO resultados
    (fecha_val_contrato, contract_id, table_name, type_error, column_name)
    VALUES
    ('{formatted_date}', '{contract}', 
    '{table}', '{problem}', 
    '{column}'),
    """

    cursor.execute(insert_query)
    conn.commit()
    return 1

# Logical type verifier
def tipo(arr, typ):
    """
    Receives an array and verifies that all the values in
    the array have the type they are expected to have
     Args:
        arr (array/pd array): The array to verify. Array for none str verifications
            and pd array for str verifications
        typ (str): the type that the whole array needs to have
    Returns:
        True if all the values are correct or false if someone is wrong
    """
    
    if typ == 'str':
        indexer = arr.str.isnumeric()
        res = [i for i, val in enumerate(indexer) if val]
        if len(res) == 0:
            return True
        else:
            return False
    elif typ == 'int':
        return all(map(lambda x: isinstance(x, int), arr))
    elif typ == 'float':
        return all(map(lambda x: isinstance(x, float), arr))
    elif typ == 'bool':
        return all(map(lambda x: isinstance(x, bool), arr))

# Contract enforcer
def verificador(yaml):
    """
    Receives a YAML file with the table and columns to verify in the contract. 
    The general findings are stored in a table called RESULTADOS (i.e. if a 
    column has atypical values or doesnt exist) and the specific problems in a 
    .log file (i.e. the atypical values per column). The logging is done here
    and the INSERT is done in another function called from here
     Args:
        yaml (list): All the information of the contract
    Returns:
        1 when the execution is over
    """

    # Holder to store if a table exists
    exists = True

    logger.info("Contract ID: %s", yaml["contractId"])
    logger.info("Contract for %s ", yaml["tableName"])

    # Cycle to move in all the columns that the DC defines
    for i in range (len(yaml['columns'])):

        columna = yaml['columns'][i]['column']
        valores = yaml['columns'][i]['values']
        tipos = yaml['columns'][i]['logicalType']
        listData = []

        # Validator for categorical columns
        if yaml['columns'][i]['isCategorical']:
            try:
                # Types verifier
                qry = cursor.execute(f'SELECT DISTINCT {columna} FROM {yaml["tableName"]}').fetchall()
                for i in range(len(qry)):
                    listData.append(qry[i][0])
                typesData = pd.Series(listData)
                if tipo(typesData, tipos) == False:
                    # Saving the result if it fails the verification
                    logger.warning(f"Col %s values distinct type from f{tipos}", columna)
                    SQLWriter(yaml['contractId'], yaml["tableName"], "Different type vals", columna)
                
                # Unique values verifier
                if len(list(set(listData).difference(valores))) == 0:
                    logger.info("Column %s correct", columna)
                else:
                    # Saving the result if it fails the verification
                    logger.warning(f"Col %s unknown values: {list(set(listData).difference(valores))}", columna)
                    SQLWriter(yaml['contractId'], yaml["tableName"], "Unidentified categorical vals", columna)
            except:
                # Saving the result if it fails the verification
                logger.error("Column %s doesnt exist", columna)
                SQLWriter(yaml['contractId'], yaml["tableName"], "Column doesnt exist", columna)
                exists = False
        
        # Validator for non categorical columns
        else:
            try:
                # Types verifier
                qry = cursor.execute(f'SELECT DISTINCT {columna} FROM {yaml["tableName"]}').fetchall()
                for i in range(len(qry)):
                    listData.append(qry[i][0])
                typesData = pd.Series(listData)
                if tipo(typesData, tipos) == False:
                    # Saving the result if it fails the verification
                    logger.warning(f"Col %s values distinct type from f{tipos}", columna)
                    SQLWriter(yaml['contractId'], yaml["tableName"], "Different type vals", columna)

                # Min/Max values verifier
                qry = cursor.execute(f'''SELECT {columna} FROM {yaml["tableName"]}
                                    WHERE {columna} < {valores[0]} OR {columna} > {valores[1]}''').fetchall()
                if len(qry) != 0:
                    # Saving the result if it fails the verification
                    logger.warning(f"Col %s wrong vals: {qry}", columna)
                    SQLWriter(yaml['contractId'], yaml["tableName"], "Values out of bounds", columna)
                else:
                    logger.info("Column %s correct", columna)
            except:
                # Saving the result if it fails the verification
                logger.error("Column %s doesnt exist", columna)
                SQLWriter(yaml['contractId'], yaml["tableName"], "Column doesnt exist", columna)
                exists = False

        # Checking for nulls
        if exists and yaml['columns'][0]['isNullable'] == False:
            nulls = cursor.execute(f'''select {columna} from {yaml["tableName"]} 
                        WHERE {columna} IS NULL''').fetchall()
            if len(nulls) != 0:
                # Saving the result if it fails the verification
                logger.warning("Column %s have nulls", columna)
                SQLWriter(yaml['contractId'], yaml["tableName"], "Null values present", columna)
            else:
                logger.info("No Null values in %s", columna)
        else:
            exists = True
    
    logger.info("---------------------------------------------------------")

    return 1


# Getting the details of the contract in the YAML
try:
    with open(sys.argv[1], 'rb') as f:
        conf = yaml.safe_load(f.read())
except:
    raise TypeError("No contract received or invalid format")


# Executing the contract and saving it in the result table
verificador(conf)
print("Process concluded...")