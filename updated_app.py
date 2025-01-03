import streamlit as st
import pyodbc
import pandas as pd
from sqlalchemy import create_engine, text
from snowflake.sqlalchemy import URL
import json
import logging

# Set up logging
logging.basicConfig(filename='app.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Load SQL Server credentials from sql_server_config.json
with open('sql_server_config.json') as sql_config_file:
    sql_server_config = json.load(sql_config_file)

# Load Snowflake credentials from snowflake_config.json
with open('snowflake_config.json') as snowflake_config_file:
    snowflake_config = json.load(snowflake_config_file)

# SQL Server Connection String using pyodbc
sql_server_conn_str = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={sql_server_config['server']};"
    f"DATABASE={sql_server_config['database']};"
    f"UID={sql_server_config['username']};"
    f"PWD={sql_server_config['password']}"
)

# Snowflake Connection String using SQLAlchemy URL
snowflake_conn_str = URL(
    account=snowflake_config['account'],
    user=snowflake_config['user'],
    password=snowflake_config['password'],
    role=snowflake_config['role'],
    warehouse=snowflake_config['warehouse'],
    database=snowflake_config['database'],
    schema=snowflake_config['schema']
)

# Function to get a list of databases from SQL Server
def get_sql_server_databases():
    logging.info('Fetching SQL Server databases.')
    try:
        conn = pyodbc.connect(sql_server_conn_str)
        databases = pd.read_sql("SELECT name FROM sys.databases", conn)
        conn.close()
        logging.info('SQL Server databases fetched successfully.')
        return databases['name'].tolist()
    except Exception as e:
        logging.error(f'Error fetching SQL Server databases: {e}')
        return []

# Function to get a list of tables from SQL Server
def get_sql_server_tables(database_name):
    logging.info(f'Fetching SQL Server tables from database: {database_name}.')
    try:
        conn = pyodbc.connect(sql_server_conn_str.replace(sql_server_config['database'], database_name))
        tables = pd.read_sql("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'", conn)
        conn.close()
        logging.info('SQL Server tables fetched successfully.')
        return tables['TABLE_NAME'].tolist()
    except Exception as e:
        logging.error(f'Error fetching SQL Server tables: {e}')
        return []

# Function to get a list of databases from Snowflake
def get_snowflake_databases():
    logging.info('Fetching Snowflake databases.')
    try:
        engine = create_engine(snowflake_conn_str)
        databases = pd.read_sql("SHOW DATABASES", engine)
        engine.dispose()
        logging.info('Snowflake databases fetched successfully.')
        return databases['name'].tolist()
    except Exception as e:
        logging.error(f'Error fetching Snowflake databases: {e}')
        return []

# Function to get a list of schemas from Snowflake
def get_snowflake_schemas(database_name):
    logging.info(f'Fetching Snowflake schemas from database: {database_name}.')
    try:
        engine = create_engine(snowflake_conn_str)
        with engine.connect() as connection:
            connection.execute(text(f'USE DATABASE "{database_name}"'))
            schemas = pd.read_sql("SHOW SCHEMAS", connection)
        engine.dispose()
        logging.info('Snowflake schemas fetched successfully.')
        return schemas['name'].tolist()
    except Exception as e:
        logging.error(f'Error fetching Snowflake schemas: {e}')
        return []

# Function to get a list of tables from Snowflake
def get_snowflake_tables(database_name, schema_name):
    logging.info(f'Fetching Snowflake tables from schema: {schema_name} in database: {database_name}.')
    try:
        engine = create_engine(snowflake_conn_str)
        with engine.connect() as connection:
            connection.execute(text(f'USE DATABASE "{database_name}"'))
            connection.execute(text(f'USE SCHEMA "{schema_name}"'))
            tables = pd.read_sql("SHOW TABLES", connection)
        engine.dispose()
        logging.info('Snowflake tables fetched successfully.')
        return tables['name'].tolist()
    except Exception as e:
        logging.error(f'Error fetching Snowflake tables: {e}')
        return []

# Function to get data from SQL Server
def get_data_from_sql_server(database_name, table_name):
    logging.info(f'Fetching data from SQL Server table: {table_name} in database: {database_name}.')
    try:
        conn = pyodbc.connect(sql_server_conn_str.replace(sql_server_config['database'], database_name))
        df = pd.read_sql(f'SELECT COUNT(*) AS record_count FROM {table_name}', conn)
        value = df.iloc[0, 0]  # Extract the count value
        conn.close()
        logging.info('Data fetched from SQL Server successfully.')
        return value
    except Exception as e:
        logging.error(f'Error fetching data from SQL Server: {e}')
        return 0  # Return 0 on error
    

def get_sql_server_schema(database_name, table_name):
    logging.info(f'Fetching schema for SQL Server table: {table_name}')
    try:
        conn = pyodbc.connect(sql_server_conn_str.replace(sql_server_config['database'], database_name))
        query = f"""
        SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = '{table_name}'
        """
        schema = pd.read_sql(query, conn)
        conn.close()
        logging.info('SQL Server schema fetched successfully.')
        return schema
    except Exception as e:
        logging.error(f'Error fetching SQL Server schema: {e}')
        return pd.DataFrame()

def get_snowflake_schema(database_name, schema_name, table_name):
    logging.info(f'Fetching schema for Snowflake table: {table_name}')
    try:
        engine = create_engine(snowflake_conn_str)
        with engine.connect() as connection:
            connection.execute(text(f'USE DATABASE "{database_name}"'))
            connection.execute(text(f'USE SCHEMA "{schema_name}"'))
            query = f"""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = '{table_name}'
            """
            schema = pd.read_sql(query, connection)
        engine.dispose()
        logging.info('Snowflake schema fetched successfully.')
        return schema
    except Exception as e:
        logging.error(f'Error fetching Snowflake schema: {e}')
        return pd.DataFrame()


# Function to get data from Snowflake (fetch record count)
def get_data_from_snowflake(database_name, schema_name, table_name):
    logging.info(f'Fetching data from Snowflake table: {table_name} in schema: {schema_name} and database: {database_name}.')
    try:
        engine = create_engine(snowflake_conn_str)
        with engine.connect() as connection:
            connection.execute(text(f'USE DATABASE "{database_name}"'))
            connection.execute(text(f'USE SCHEMA "{schema_name}"'))
            df_snowflake = pd.read_sql(f'SELECT COUNT(*) AS record_count FROM {table_name}', connection)
            value = df_snowflake.iloc[0, 0]  # Extract the count value
        engine.dispose()
        logging.info('Data fetched from Snowflake successfully.')
        return value
    except Exception as e:
        logging.error(f'Error fetching data from Snowflake: {e}')
        return 0  # Return 0 on error

# Function to compare two record counts
def compare_counts(count_sql_server, count_snowflake):
    logging.info('Comparing record counts between SQL Server and Snowflake.')
    if count_sql_server == count_snowflake:
        logging.info('Record counts match.')
        return True, None
    else:
        logging.info('Record counts do not match.')
        return False, {"SQL Server Count": count_sql_server, "Snowflake Count": count_snowflake}
    

def compare_schemas(sql_server_schema, snowflake_schema):
    logging.info('Comparing schemas between SQL Server and Snowflake.')
    
    # Standardize SQL Server and Snowflake schema column names
    sql_server_schema.columns = ['COLUMN_SQL', 'DATA_TYPE_sql', 'IS_NULLABLE_sql']
    snowflake_schema.columns = ['COLUMN_SNOW', 'DATA_TYPE_snow', 'IS_NULLABLE_snow']
    
    # Normalize column names to lowercase to avoid case mismatches
    sql_server_schema['COLUMN_SQL'] = sql_server_schema['COLUMN_SQL'].str.lower()
    snowflake_schema['COLUMN_SNOW'] = snowflake_schema['COLUMN_SNOW'].str.lower()

    # Merge schemas on column names
    merged_schema = pd.merge(sql_server_schema, snowflake_schema, left_on='COLUMN_SQL', right_on='COLUMN_SNOW', how='outer')

    # Create comparison results
    merged_schema['COLUMN_MATCH'] = merged_schema.apply(
        lambda row: 'Matching' if pd.notna(row['COLUMN_SQL']) and pd.notna(row['COLUMN_SNOW']) else 'Not Matching', axis=1
    )
    
    merged_schema['DATA_TYPE_MATCH'] = merged_schema.apply(
        lambda row: 'Matching' if row['DATA_TYPE_sql'] == row['DATA_TYPE_snow'] else 'Not Matching', axis=1
    )
    
    # Compare nullability, treating 'YES' from SQL Server as matching with 'Y' from Snowflake
    merged_schema['IS_NULLABLE_MATCH'] = merged_schema.apply(
        lambda row: 'Matching' if (row['IS_NULLABLE_sql'] == row['IS_NULLABLE_snow']) or
                                  (row['IS_NULLABLE_sql'] == 'YES' and row['IS_NULLABLE_snow'] == 'Y') or (row['IS_NULLABLE_sql'] == 'NO' and row['IS_NULLABLE_snow'] == 'N') else 'Not Matching',
        axis=1
    )

    # Check if all columns, data types, and nullability match
    overall_match = (
        (merged_schema['COLUMN_MATCH'] == 'Matching') &
        (merged_schema['DATA_TYPE_MATCH'] == 'Matching') &
        (merged_schema['IS_NULLABLE_MATCH'] == 'Matching')
    ).all()

    if overall_match:
        logging.info('Schemas match.')
        return True, None
    else:
        logging.info('Schemas do not match.')
        return False, merged_schema[['COLUMN_SQL', 'COLUMN_SNOW', 'DATA_TYPE_sql', 'DATA_TYPE_snow', 'IS_NULLABLE_sql', 'IS_NULLABLE_snow', 'COLUMN_MATCH', 'DATA_TYPE_MATCH', 'IS_NULLABLE_MATCH']]

# Streamlit App Layout
st.set_page_config(layout="wide")
st.markdown("<h1 style='text-align: center;'>QA Insight</h1>", unsafe_allow_html=True)

# Layout with two columns: SQL Server (left) and Snowflake (right)
col1, col2 = st.columns(2)

# SQL Server Section
with col1:
    st.header("SQL Server")
    sql_server_databases = get_sql_server_databases()
    selected_sql_server_db = st.selectbox('Select SQL Server Database', sql_server_databases)
    if selected_sql_server_db:
        sql_server_tables = get_sql_server_tables(selected_sql_server_db)
        selected_sql_server_table = st.selectbox('Select SQL Server Table', sql_server_tables)

# Snowflake Section
with col2:
    st.header("Snowflake")
    snowflake_databases = get_snowflake_databases()
    selected_snowflake_db = st.selectbox('Select Snowflake Database', snowflake_databases)
    if selected_snowflake_db:
        snowflake_schemas = get_snowflake_schemas(selected_snowflake_db)
        selected_snowflake_schema = st.selectbox('Select Snowflake Schema', snowflake_schemas)
        if selected_snowflake_schema:
            snowflake_tables = get_snowflake_tables(selected_snowflake_db, selected_snowflake_schema)
            selected_snowflake_table = st.selectbox('Select Snowflake Table', snowflake_tables)

# Buttons for Count Validation and Schema Validation
st.markdown("<div style='text-align: center;'>", unsafe_allow_html=True)

if st.button('Count Validation'):
    if selected_sql_server_table and selected_snowflake_table:
        st.write("Fetching data from SQL Server...")
        sql_server_count = get_data_from_sql_server(selected_sql_server_db, selected_sql_server_table)
        st.write(f"SQL Server Record Count: {sql_server_count}")

        st.write("Fetching data from Snowflake...")
        snowflake_count = get_data_from_snowflake(selected_snowflake_db, selected_snowflake_schema, selected_snowflake_table)
        st.write(f"Snowflake Record Count: {snowflake_count}")

        st.write("Comparing record counts...")
        is_match, diff_info = compare_counts(sql_server_count, snowflake_count)
        if is_match:
            st.success("Record counts match between SQL Server and Snowflake!")
        else:
            st.error("Record counts do not match between SQL Server and Snowflake.")
    else:
        st.warning("Please select both SQL Server and Snowflake tables for comparison.")

if st.button('Schema Validation'):
    if selected_sql_server_table and selected_snowflake_table:
        # Fetch SQL Server and Snowflake schemas
        st.write("Fetching SQL Server Schema...")
        sql_server_schema = get_sql_server_schema(selected_sql_server_db, selected_sql_server_table)
        st.dataframe(sql_server_schema)
        st.write("Fetching Snowflake Schema...")
        snowflake_schema = get_snowflake_schema(selected_snowflake_db, selected_snowflake_schema, selected_snowflake_table)
        st.dataframe(snowflake_schema)
        sql_server_schema = get_sql_server_schema(selected_sql_server_db, selected_sql_server_table)
        snowflake_schema = get_snowflake_schema(selected_snowflake_db, selected_snowflake_schema, selected_snowflake_table)
        
        if not sql_server_schema.empty and not snowflake_schema.empty:
            st.write("Performing schema comparison...")
            match, schema_diff = compare_schemas(sql_server_schema, snowflake_schema)
            
            if match:
                st.success("Schemas match between SQL Server and Snowflake!")
            else:
                st.error("Schemas do not match.")
                st.write("Differences in schema:")
                st.dataframe(schema_diff)
        else:
            st.error("Could not fetch schemas. Please check configurations or table selections.")
    else:
        st.warning("Please select tables from both SQL Server and Snowflake.")


st.markdown("</div>", unsafe_allow_html=True)
