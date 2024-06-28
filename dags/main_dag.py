# Import necessary modules from Airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta

# Import SQLAlchemy modules for database connection and schema definition
from sqlalchemy.engine import URL, create_engine
from sqlalchemy.types import Integer, Float, String, Boolean, TIMESTAMP, Text

# Import data processing libraries
import fastparquet
import pandas as pd
import fastavro
import psycopg2
import json
from pprint import pprint

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 6, 23),
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Function to create a connection to the PostgreSQL database
def postgres_connection():
    connection_string = URL.create(
        'postgresql',
        username='data_warehouse_owner',
        password='b59hqfNxdVwr',
        host='ep-wild-surf-a5em82m5.us-east-2.aws.neon.tech',
        database='data_warehouse',
        port=5432,
        query={'sslmode': 'require'}
    )
    # Create and return the SQLAlchemy engine
    engine = create_engine(connection_string)
    return engine

# Function to insert data from Avro file into PostgreSQL database
def insert_to_postgres():
    # Establishing connection to PostgreSQL database
    engine = postgres_connection()

    # Path to the Avro file containing order item data
    path = 'data/order_item.avro'

    # Reading data from Avro file using fastavro
    with open(path, 'rb') as f:
        avro_reader = fastavro.reader(f)
        data = list(avro_reader)

    # Creating a DataFrame from the Avro data
    df = pd.DataFrame(data)

    # Defining the schema for the PostgreSQL table
    df_schema = {
        'id': Integer,
        'order_id': Integer,
        'product_id': Integer,
        'amount': Integer,
        'coupon_id': Integer
    }

    # Writing the DataFrame to PostgreSQL table 'order_items'
    df.to_sql('order_items', con=engine, schema='public', if_exists='replace', index=False, dtype=df_schema)

# Function to insert coupon data from JSON file into PostgreSQL database
def insert_coupon_to_postgres():
    # Establishing connection to PostgreSQL database
    engine = postgres_connection()

    # Reading coupon data from JSON file
    with open('data/coupons.json') as file_open:
        json_open = json.load(file_open)

    # Creating a DataFrame from the JSON data
    df = pd.DataFrame(json_open)

    # Defining the schema for the PostgreSQL table
    df_schema = {
        'id': Integer,
        'discount_percent': Float
    }

    # Writing the DataFrame to PostgreSQL table 'coupons'
    df.to_sql('coupons', con=engine, schema='public', if_exists='replace', index=False, dtype=df_schema)

# Function to insert customer data from multiple CSV files into PostgreSQL database
def insert_customer_csv_to_postgres():
    # Establishing connection to PostgreSQL database
    engine = postgres_connection()

    # Defining the schema for the PostgreSQL table
    schema = {
        'id': Integer,
        'first_name': String,
        'last_name': String,
        'gender': String,
        'address': String,
        'zip_code': String
    }

    # Initializing an empty DataFrame to store combined data
    data = pd.DataFrame(columns=['id', 'first_name', 'last_name', 'gender', 'address', 'zip_code'])

    # Iterating over each CSV file
    for i in range(10):
        print(f'Extracting CSV {i}')
        # Reading data from each CSV file
        data_csv = pd.read_csv(f'data/customer_{i}.csv')
        
        # Dropping 'Unnamed: 0' column if present
        if 'Unnamed: 0' in data_csv.columns:
            data_csv.drop('Unnamed: 0', axis=1, inplace=True)
        
        # Concatenating data into the main DataFrame
        data = pd.concat([data, data_csv])
        print('Data extraction complete')

    # Writing the combined DataFrame to PostgreSQL table 'customers'
    data.to_sql('customers', con=engine, schema='public', if_exists='replace', index=False, dtype=schema)
    print(f'SQL execution complete, {len(data)} records inserted')

# Function to insert login attempt data from multiple JSON files into PostgreSQL database
def insert_login_attempts():
    # Establishing connection to PostgreSQL database
    engine = postgres_connection()

    # Initialize variables
    nomer = 0
    all_data = []

    # Iterating over each JSON file
    for i in range(10):
        data = []
        with open(f'data/login_attempts_{i}.json') as file_open:
            print(f'Opening file {i}')
            json_open = json.load(file_open)
            jumlah = len(json_open['id'])
            
            # Iterating over each entry in the JSON data
            for j in range(jumlah):
                index = f'{nomer}'
                id = json_open['id'][index]
                customer_id = json_open['customer_id'][index]
                login_successful = json_open['login_successful'][index]
                timestamp = json_open['attempted_at'][index]
                attempted_at = datetime.fromtimestamp(timestamp / 1000)
                tupple = (id, customer_id, login_successful, attempted_at)
                data.append(tupple)
                nomer += 1
            print("Data extraction complete")
            
            # Creating DataFrame from extracted data
            df = pd.DataFrame(data, columns=['id', 'customer_id', 'login_successful', 'attempted_at'])
            all_data.append(df)

    # Concatenating all DataFrames into one
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        
        # Writing combined DataFrame to PostgreSQL table 'login_attempt_history'
        combined_df.to_sql('login_attempt_history', con=engine, schema='public', if_exists='replace', index=False, dtype={
            'id': Integer,
            'customer_id': Integer,
            'login_successful': Boolean,
            'attempted_at': TIMESTAMP
        })

# Function to insert product category data from an Excel file into PostgreSQL database
def insert_product_category():
    # Establishing connection to PostgreSQL database
    engine = postgres_connection()

    # Reading product category data from Excel file
    sheet = pd.read_excel('data/product_category.xls')
    sheet.convert_dtypes()

    # Defining the schema for the PostgreSQL table
    schema = {
        'id': Integer,
        'name': String
    }

    # Writing the DataFrame to PostgreSQL table 'product_categories'
    sheet.to_sql('product_categories', con=engine, schema='public', if_exists='replace', index=False, dtype=schema)

# Function to insert product data from an Excel file into PostgreSQL database
def insert_products():
    # Establishing connection to PostgreSQL database
    engine = postgres_connection()

    # Reading product data from Excel file
    source = pd.read_excel('data/product.xls')
    source.convert_dtypes()

    # Defining the schema for the PostgreSQL table
    schema = {
        'id': Integer,
        'name': String,
        'price': Float,
        'category_id': Integer,
        'supplier_id': Integer
    }

    # Writing the DataFrame to PostgreSQL table 'products'
    source.to_sql('products', con=engine, schema='public', if_exists='replace', index=False, dtype=schema)

# Function to insert supplier data from an Excel file into PostgreSQL database
def insert_supplier_data():
    # Establishing connection to PostgreSQL database
    engine = postgres_connection()

    # Reading supplier data from Excel file
    source = pd.read_excel('data/supplier.xls')
    source.convert_dtypes()

    # Defining the schema for the PostgreSQL table
    schema = {
        'id': Integer,
        'name': String,
        'country': String
    }

    # Writing the DataFrame to PostgreSQL table 'suppliers'
    source.to_sql('suppliers', con=engine, schema='public', if_exists='replace', index=False, dtype=schema)

# Function to insert order data from a Parquet file into PostgreSQL database
def insert_order_data():
    # Establishing connection to PostgreSQL database
    engine = postgres_connection()

    # Reading order data from Parquet file
    file = 'data/order.parquet'
    df = pd.read_parquet(file)

    # Defining the schema for the PostgreSQL table
    schema = {
        'id': Integer,
        'customer_id': Integer,
        'status': Text,
        'created_at': TIMESTAMP
    }

    # Writing the DataFrame to PostgreSQL table 'orders'
    df.to_sql('orders', con=engine, schema='public', if_exists='replace', index=False, dtype=schema)

# Create the main DAG (Directed Acyclic Graph) object
with DAG(
    dag_id='main_dag',
    default_args=default_args,
    schedule_interval='@once',
) as dag:

    # Define task to ingest Avro data to PostgreSQL
    task_ingest_avro_to_postgres = PythonOperator(
        task_id='avro_to_postgres',
        python_callable=insert_to_postgres,
        dag=dag
    )

    # Define task to ingest coupons JSON data to PostgreSQL
    task_ingest_coupons_json_to_postgres = PythonOperator(
        task_id='coupons_to_postgres',
        python_callable=insert_coupon_to_postgres,
        provide_context=True,
        dag=dag
    )

    # Define task to ingest customer CSV data to PostgreSQL
    task_ingest_customer_to_postgres = PythonOperator(
        task_id='customer_to_postgres',
        python_callable=insert_customer_csv_to_postgres,
        dag=dag
    )

    # Define task to ingest login attempts JSON data to PostgreSQL
    task_ingest_login_attempts_to_postgres = PythonOperator(
        task_id='login_attempts_to_postgres',
        python_callable=insert_login_attempts,
        dag=dag
    )

    # Define task to ingest product category data from Excel to PostgreSQL
    task_ingest_product_category_to_postgres = PythonOperator(
        task_id='product_category_to_postgres',
        python_callable=insert_product_category,
        dag=dag
    )

    # Define task to ingest products data from Excel to PostgreSQL
    task_ingest_products_to_postgres = PythonOperator(
        task_id='products_to_postgres',
        python_callable=insert_products,
        dag=dag
    )

    # Define task to ingest suppliers data from Excel to PostgreSQL
    task_ingest_suppliers_to_postgres = PythonOperator(
        task_id='suppliers_to_postgres',
        python_callable=insert_supplier_data,
        dag=dag
    )

    # Define task to ingest orders data from Parquet to PostgreSQL
    task_ingest_orders_to_postgres = PythonOperator(
        task_id='orders_to_postgres',
        python_callable=insert_order_data,
        dag=dag
    )

    # Set task dependencies to define the order of execution
    task_ingest_avro_to_postgres >> task_ingest_coupons_json_to_postgres >> task_ingest_customer_to_postgres >> task_ingest_login_attempts_to_postgres >> task_ingest_product_category_to_postgres >> task_ingest_products_to_postgres >> task_ingest_suppliers_to_postgres >> task_ingest_orders_to_postgres
