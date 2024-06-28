# Import necessary libraries and modules
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from sqlalchemy.engine import URL, create_engine
import pandas as pd
from airflow.hooks.postgres_hook import PostgresHook

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 6, 23),
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Function to create a PostgreSQL connection using SQLAlchemy
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
    engine = create_engine(connection_string)
    return engine

# Function to fetch supplier data from PostgreSQL
def fetch_supplier_data():
    engine = postgres_connection()
    df = pd.read_sql('SELECT * FROM suppliers', con=engine)
    return df

# Function to transform supplier data
def transform_supplier_data(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='fetch_supplier_data')
    # Removing the 'Unnamed: 0' column and retaining only the most recent duplicates based on 'id'
    df_transformed = df.drop(columns=["Unnamed: 0"]).drop_duplicates(keep='last', subset=['id'])
    return df_transformed

# Function to load transformed supplier data into PostgreSQL
def load_supplier_data(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='transform_supplier_data')
    engine = postgres_connection()
    df.to_sql(name='transf_suppliers', con=engine, if_exists='replace', index=False)

# Function to fetch product data from PostgreSQL
def fetch_product_data():
    engine = postgres_connection()
    df = pd.read_sql('SELECT * FROM products', con=engine)
    return df

# Function to transform product data
def transform_product_data(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='fetch_product_data')
    # Removing the 'Unnamed: 0' column and retaining only the most recent duplicates based on 'id'
    df_transformed = df.drop(columns=["Unnamed: 0"]).drop_duplicates(keep='last', subset=['id'])
    return df_transformed

# Function to load transformed product data into PostgreSQL
def load_product_data(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='transform_product_data')
    engine = postgres_connection()
    df.to_sql(name='transf_products', con=engine, if_exists='replace', index=False)

# Function to fetch product category data from PostgreSQL
def fetch_product_category_data():
    engine = postgres_connection()
    df = pd.read_sql('SELECT * FROM product_categories', con=engine)
    return df

# Function to transform product category data
def transform_product_category_data(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='fetch_product_category_data')
    # Removing the 'Unnamed: 0' column and retaining only the most recent duplicates based on 'id'
    df_transformed = df.drop(columns=["Unnamed: 0"]).drop_duplicates(keep='last', subset=['id'])
    return df_transformed

# Function to load transformed product category data into PostgreSQL
def load_product_category_data(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='transform_product_category_data')
    engine = postgres_connection()
    df.to_sql(name='transf_product_categories', con=engine, if_exists='replace', index=False)

# Function to fetch order data from PostgreSQL
def fetch_order_data():
    engine = postgres_connection()
    df = pd.read_sql('SELECT * FROM "orders"', con=engine)
    return df

# Function to transform order data
def transform_order_data(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='fetch_order_data')
    
    # Convert 'customer_id' to float if necessary
    df['customer_id'] = df['customer_id'].astype(float)
    
    # Ensure 'created_at' is treated as a string
    df['created_at'] = df['created_at'].astype(str)
    
    # Drop duplicates based on 'id'
    df.drop_duplicates(keep='last', inplace=True, subset=['id'])
    
    # Split 'created_at' into 'created_at' and 'period_time'
    try:
        df[['created_at', 'period_time']] = df['created_at'].str.split(' ', n=1, expand=True)
    except ValueError:
        # Handle cases where split fails
        df['period_time'] = '00:00:00'  # Assign a default value
    
    return df

# Function to load transformed order data into PostgreSQL
def load_order_data(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='transform_order_data')
    engine = postgres_connection()
    # Convert 'created_at' to datetime
    df['created_at'] = pd.to_datetime(df['created_at'])  
    # Convert 'period_time' to time
    df['period_time'] = pd.to_datetime(df['period_time'], format='%H:%M:%S').dt.time  
    df.to_sql(name='transf_orders', con=engine, if_exists='replace', index=False)

# Function to fetch login attempts data from PostgreSQL
def fetch_login_attempts_data():
    engine = postgres_connection()
    df = pd.read_sql('SELECT * FROM login_attempt_history', con=engine)
    return df

# Function to transform login attempts data
def transform_login_attempts_data(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='fetch_login_attempts_data')
    
    # Convert 'customer_id' to float if necessary
    df['customer_id'] = df['customer_id'].astype(float)
    
    # Ensure 'attempted_at' is treated as a string
    df['attempted_at'] = df['attempted_at'].astype(str)
    
    # Drop duplicates based on 'id'
    df.drop_duplicates(keep='last', inplace=True, subset=['id'])
    
    # Split 'attempted_at' into 'attempted_at' and 'period'
    df[['attempted_at', 'period']] = df['attempted_at'].str.split(' ', n=1, expand=True)
    
    return df

# Function to load transformed login attempts data into PostgreSQL
def load_login_attempts_data(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='transform_login_attempts_data')
    engine = postgres_connection()
    # Convert 'attempted_at' to datetime
    df['attempted_at'] = pd.to_datetime(df['attempted_at'])  
    # Convert 'period' to time
    df['period'] = pd.to_datetime(df['period'], format='%H:%M:%S').dt.time  
    df.to_sql(name='transf_login_attempt_history', con=engine, if_exists='replace', index=False)

# Function to fetch customer data from PostgreSQL
def fetch_customer_data():
    engine = postgres_connection()
    df = pd.read_sql('SELECT * FROM customers', con=engine)
    return df

# Function to transform customer data
def transform_customer_data(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='fetch_customer_data')
    # Converting 'zip_code' column to string type
    df['zip_code'] = df['zip_code'].astype(str)
    # Creating a new 'full_name' column by combining 'first_name' and 'last_name'
    df['full_name'] = df['first_name'] + ' ' + df['last_name']
    return df

# Function to load transformed customer data into PostgreSQL
def load_customer_data(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='transform_customer_data')
    engine = postgres_connection()
    df.to_sql(name='transf_customers', con=engine, if_exists='replace', index=False)

# Function to fetch coupon data from PostgreSQL
def fetch_coupon_data():
    engine = postgres_connection()
    df = pd.read_sql('SELECT * FROM coupons', con=engine)
    return df

# Function to transform coupon data
def transform_coupon_data(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='fetch_coupon_data')
    if 'discount_percent' in df.columns:
        df['discount_percent'] = df['discount_percent'].astype(str).str.replace('%', '') + '%'
    # Add a column 'numeric_value' that contains the numeric value of the discount percentage
    df['numeric_value'] = df['discount_percent'].str.rstrip('%').astype(float) / 100
    return df

# Function to load transformed coupon data into PostgreSQL
def load_coupon_data(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='transform_coupon_data')
    engine = postgres_connection()
    df.to_sql(name='transf_coupons', con=engine, if_exists='replace', index=False)

# Function to fetch order item data from PostgreSQL
def fetch_order_item_data():
    engine = postgres_connection()
    df = pd.read_sql('SELECT * FROM order_items', con=engine)
    return df

# Function to transform order item data
def transform_order_item_data(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='fetch_order_item_data')
    # Keeping only the most recent duplicate rows based on 'id'
    df.drop_duplicates(keep='last', inplace=True, subset=['id'])
    return df

# Function to load transformed order item data into PostgreSQL
def load_order_item_data(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='transform_order_item_data')
    engine = postgres_connection()
    df.to_sql(name='transf_order_items', con=engine, if_exists='replace', index=False)

# Define the DAG and its schedule
with DAG(
    dag_id='transform_dag',
    default_args=default_args,
    schedule_interval='@once',
) as dag:

    # Define tasks for suppliers
    task_fetch_supplier = PythonOperator(
        task_id='fetch_supplier_data',
        python_callable=fetch_supplier_data,
        dag=dag
    )

    task_transform_supplier = PythonOperator(
        task_id='transform_supplier_data',
        python_callable=transform_supplier_data,
        provide_context=True,
        dag=dag
    )

    task_load_supplier = PythonOperator(
        task_id='load_supplier_data',
        python_callable=load_supplier_data,
        provide_context=True,
        dag=dag
    )

    # Define tasks for products
    task_fetch_product = PythonOperator(
        task_id='fetch_product_data',
        python_callable=fetch_product_data,
        dag=dag
    )

    task_transform_product = PythonOperator(
        task_id='transform_product_data',
        python_callable=transform_product_data,
        provide_context=True,
        dag=dag
    )

    task_load_product = PythonOperator(
        task_id='load_product_data',
        python_callable=load_product_data,
        provide_context=True,
        dag=dag
    )

    # Define tasks for product categories
    task_fetch_product_category = PythonOperator(
        task_id='fetch_product_category_data',
        python_callable=fetch_product_category_data,
        dag=dag
    )

    task_transform_product_category = PythonOperator(
        task_id='transform_product_category_data',
        python_callable=transform_product_category_data,
        provide_context=True,
        dag=dag
    )

    task_load_product_category = PythonOperator(
        task_id='load_product_category_data',
        python_callable=load_product_category_data,
        provide_context=True,
        dag=dag
    )

    # Define tasks for orders
    task_fetch_order = PythonOperator(
        task_id='fetch_order_data',
        python_callable=fetch_order_data,
        dag=dag
    )

    task_transform_order = PythonOperator(
        task_id='transform_order_data',
        python_callable=transform_order_data,
        provide_context=True,
        dag=dag
    )

    task_load_order = PythonOperator(
        task_id='load_order_data',
        python_callable=load_order_data,
        provide_context=True,
        dag=dag
    )

    # Define tasks for login attempts
    task_fetch_login_attempts = PythonOperator(
        task_id='fetch_login_attempts_data',
        python_callable=fetch_login_attempts_data,
        dag=dag
    )

    task_transform_login_attempts = PythonOperator(
        task_id='transform_login_attempts_data',
        python_callable=transform_login_attempts_data,
        provide_context=True,
        dag=dag
    )

    task_load_login_attempts = PythonOperator(
        task_id='load_login_attempts_data',
        python_callable=load_login_attempts_data,
        provide_context=True,
        dag=dag
    )

    # Define tasks for customers
    task_fetch_customer = PythonOperator(
        task_id='fetch_customer_data',
        python_callable=fetch_customer_data,
        dag=dag
    )

    task_transform_customer = PythonOperator(
        task_id='transform_customer_data',
        python_callable=transform_customer_data,
        provide_context=True,
        dag=dag
    )

    task_load_customer = PythonOperator(
        task_id='load_customer_data',
        python_callable=load_customer_data,
        provide_context=True,
        dag=dag
    )

    # Define tasks for coupons
    task_fetch_coupon = PythonOperator(
        task_id='fetch_coupon_data',
        python_callable=fetch_coupon_data,
        dag=dag
    )

    task_transform_coupon = PythonOperator(
        task_id='transform_coupon_data',
        python_callable=transform_coupon_data,
        provide_context=True,
        dag=dag
    )

    task_load_coupon = PythonOperator(
        task_id='load_coupon_data',
        python_callable=load_coupon_data,
        provide_context=True,
        dag=dag
    )

    # Define tasks for order items
    task_fetch_order_item = PythonOperator(
        task_id='fetch_order_item_data',
        python_callable=fetch_order_item_data,
        dag=dag
    )

    task_transform_order_item = PythonOperator(
        task_id='transform_order_item_data',
        python_callable=transform_order_item_data,
        provide_context=True,
        dag=dag
    )

    task_load_order_item = PythonOperator(
        task_id='load_order_item_data',
        python_callable=load_order_item_data,
        provide_context=True,
        dag=dag
    )

    # Setting up dependencies
    task_fetch_supplier >> task_transform_supplier >> task_load_supplier
    task_fetch_product >> task_transform_product >> task_load_product
    task_fetch_product_category >> task_transform_product_category >> task_load_product_category
    task_fetch_order >> task_transform_order >> task_load_order
    task_fetch_login_attempts >> task_transform_login_attempts >> task_load_login_attempts
    task_fetch_customer >> task_transform_customer >> task_load_customer
    task_fetch_coupon >> task_transform_coupon >> task_load_coupon
    task_fetch_order_item >> task_transform_order_item >> task_load_order_item
