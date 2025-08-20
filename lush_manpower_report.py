import pandas as pd
import os
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime

from sqlalchemy.dialects.oracle import VARCHAR2, DATE 

from sqlalchemy.types import VARCHAR, Integer, Date





def read_excel_file(file_path: str) -> pd.DataFrame:
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    return pd.read_excel(file_path)





def create_oracle_connection(schema_name: str, password: str, host: str, port: str, sid: str = None) -> 'sqlalchemy.engine.Engine':

    if sid:
        connection_string = f"oracle+cx_oracle://{schema_name}:{password}@{host}:{port}/{sid}"
    else:
        raise ValueError("Either service_name or sid must be provided for Oracle connection.")
    engine = create_engine(connection_string, echo=False)
    return engine






def check_and_create_table(engine: 'sqlalchemy.engine.Engine', table_name: str):
    inspector = inspect(engine)

    with engine.connect() as conn:
        if inspector.has_table(table_name):
            print(f"Table '{table_name}' already exists. Skipping creation.")
            return
            # conn.execute(text(f'DROP TABLE {table_name} CASCADE CONSTRAINTS'))
            # conn.commit()
            # print(f"Table '{table_name}' dropped successfully.")

        print(f"Creating new table '{table_name}'...")

        dtype_mapping = {
            'ID': 'INTEGER',
            'NAME': 'VARCHAR2(100)',
            'CADRE': 'VARCHAR2(50)',
            'DEPT': 'VARCHAR2(50)',
            'DIV': 'VARCHAR2(50)',
            'RMK': 'VARCHAR2(200)',
            'SAPID': 'VARCHAR2(20)',
            'DOB': 'DATE',
            'NIN': 'VARCHAR2(20)',
            'BANK_NAME': 'VARCHAR2(100)',
            'ACC_NO': 'VARCHAR2(50)',
            'MEAL': 'VARCHAR2(50)',
            'GENDER': 'VARCHAR2(10)',
            'ADDRESS': 'VARCHAR2(200)',
            'GROUP': 'VARCHAR2(50)',
            'CURRENT_SHIFT': 'VARCHAR2(50)',
            'OT1': 'VARCHAR2(50)',
            'OT2': 'VARCHAR2(50)',
            'OLD_ID': 'VARCHAR2(20)',
            'ACCESS_GROUP': 'VARCHAR2(50)',
            'START_DATE': 'DATE',
            'END_DATE': 'DATE',
            'STATUS': 'VARCHAR2(20)',
            'LEVEL': 'VARCHAR2(20)'
        }
        columns = ', '.join([f'"{col}" {dtype}' for col, dtype in dtype_mapping.items()])

        create_table_sql = f"""
        CREATE TABLE {table_name} (
            {columns},
            PRIMARY KEY ("ID")
        )
        """
        conn.execute(text(create_table_sql))
        conn.commit()
        print(f"Table '{table_name}' created successfully.")






def load_data_to_oracle(df: pd.DataFrame, engine: 'sqlalchemy.engine.Engine', table_name: str):
    try:
        df.columns = df.columns.str.strip().str.replace(' ', '_').str.upper()
        dtype_mapping = {
            'ID': Integer(),
            'NAME': VARCHAR(100),
            'CADRE': VARCHAR(50),
            'DEPT': VARCHAR(50),
            'DIV': VARCHAR(50),
            'RMK': VARCHAR(200),
            'SAPID': VARCHAR(20),
            'DOB': Date(),
            'NIN': VARCHAR(20),
            'BANK_NAME': VARCHAR(100),
            'ACC_NO': VARCHAR(50),
            'MEAL': VARCHAR(50),
            'GENDER': VARCHAR(10),
            'ADDRESS': VARCHAR(200),
            'GROUP': VARCHAR(50),
            'CURRENT_SHIFT': VARCHAR(50),
            'OT1': VARCHAR(50),
            'OT2': VARCHAR(50),
            'OLD_ID': VARCHAR(20),
            'ACCESS_GROUP': VARCHAR(50),
            'START_DATE': Date(),
            'END_DATE': Date(),
            'STATUS': VARCHAR(20),
            'LEVEL': VARCHAR(20)
        }

        df.to_sql(table_name, engine, if_exists='append', index=False, chunksize=1000, dtype=dtype_mapping)
        print(f"Successfully loaded {len(df)} rows to table '{table_name}'.")
    except SQLAlchemyError as e:
        print(f"Error loading data to Oracle: {str(e)}")
        raise






def main(config):
    print(config)
    table_name = "lush_employee_data"
    if not config["oracle_sid"]:
        raise ValueError("oracle_sid is missing")
    if not config["excel_file_path"]:
        raise ValueError("excel_file_path is missing")
    if not config["schema_name"]:
        raise ValueError("schema_name is missing")
    if not config["oracle_password"]:
        raise ValueError("oracle_password is missing")
    if not config["oracle_host"]:
        raise ValueError("oracle_host is missing")
    if not config["oracle_port"]:
        raise ValueError("oracle_port is missing")

    excel_file_path = config["excel_file_path"]
    schema_name = config["schema_name"]
    oracle_password = config["oracle_password"]
    oracle_host = config["oracle_host"]
    oracle_port = config["oracle_port"]
    oracle_sid = config["oracle_sid"]



    try:
        df = read_excel_file(excel_file_path)
    except Exception as e:
        print(f"Error reading Excel file: {str(e)}")
        return

    try:
        engine = create_oracle_connection(schema_name, oracle_password, oracle_host, oracle_port, oracle_sid)
    except Exception as e:
        print(f"Error connecting to Oracle DB: {str(e)}")
        return

    try:
        check_and_create_table(engine, table_name)
    except Exception as e:
        print(f"Error during table creation: {str(e)}")
        return
    try:
        load_data_to_oracle(df, engine, table_name)
    except Exception as e:
        print(f"Error during data load: {str(e)}")
        return

    print("Process completed successfully.")






    
if __name__ == "__main__":
    config = {
    "excel_file_path" : r"C:\Users\egoba.kelvin\RPA\lush_data1.xlsx" ,
    "schema_name" :"A172083",
    "oracle_password" : "MurbM!yqBpA5t" ,
    "oracle_host" : "revion-aws-eu-uk-ldb2.revion.com" , 
    "oracle_port" : "15210" ,
    "oracle_sid" : "KT" 
    }
    main(config)
