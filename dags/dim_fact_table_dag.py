# Import required libraries and modules
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from sqlalchemy.engine import URL, create_engine
from datetime import datetime, timedelta
import pandas as pd
import logging

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 6, 23),
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Function to create a connection to the source PostgreSQL database
def source_postgres_connection():
    connection_string = URL.create(
        drivername='postgresql',
        username='data_warehouse_owner',
        password='b59hqfNxdVwr',
        host='ep-wild-surf-a5em82m5.us-east-2.aws.neon.tech',
        port=5432,
        database='data_warehouse',
        query={'sslmode': 'require'}
    )
    engine = create_engine(connection_string)
    return engine

# Function to create a connection to the target PostgreSQL database
def target_postgres_connection():
    connection_string = URL.create(
        drivername='postgresql',
        username='data_warehouse_owner',
        password='gkZW8NFvC0mG',
        host='ep-mute-wind-a5di8srb.us-east-2.aws.neon.tech',
        port=5432,
        database='data_warehouse',
        query={'sslmode': 'require'}
    )
    engine = create_engine(connection_string)
    return engine

# Function to create the 'dim_coupons' table in the target database
def create_dim_coupons_table():
    logging.info("Creating dim_coupons table in the target database.")
    try:
        engine = target_postgres_connection()
        with engine.connect() as connection:
            connection.execute("""
                CREATE TABLE IF NOT EXISTS dim_coupons (
                    coupon_id SERIAL PRIMARY KEY,
                    discount_percent TEXT,
                    numeric_value FLOAT
                );
            """)
        logging.info("Table 'dim_coupons' has been successfully created in the new database.")
    except Exception as e:
        logging.error(f"Failed to create table 'dim_coupons': {e}")
        raise

# Function to transfer data from the 'transf_coupons' table in the source database to the 'dim_coupons' table in the target database
def transfer_coupons_data():
    logging.info("Transferring data from transf_coupons to dim_coupons.")
    try:
        source_engine = source_postgres_connection()
        target_engine = target_postgres_connection()
        
        # Read data from the source database
        with source_engine.connect() as source_conn:
            query = "SELECT id AS coupon_id, discount_percent, numeric_value FROM transf_coupons"
            coupons_df = pd.read_sql(query, source_conn)
        
        # Write data to the target database
        with target_engine.connect() as target_conn:
            coupons_df.to_sql('dim_coupons', target_conn, if_exists='append', index=False)
        
        logging.info("Data has been successfully transferred from 'transf_coupons' to 'dim_coupons'.")
    except Exception as e:
        logging.error(f"Failed to transfer data: {e}")
        raise

# Function to create the 'dim_customers' table in the target database
def create_dim_customers_table():
    logging.info("Creating dim_customers table in the target database.")
    try:
        engine = target_postgres_connection()
        with engine.connect() as connection:
            connection.execute("""
                CREATE TABLE IF NOT EXISTS dim_customers (
                    customer_id SERIAL PRIMARY KEY,
                    first_name VARCHAR,
                    last_name VARCHAR,
                    gender VARCHAR,
                    address VARCHAR,
                    zip_code VARCHAR,
                    full_name VARCHAR
                );
            """)
        logging.info("Table 'dim_customers' has been successfully created in the new database.")
    except Exception as e:
        logging.error(f"Failed to create table 'dim_customers': {e}")
        raise

# Function to transfer data from the 'transf_customers' table in the source database to the 'dim_customers' table in the target database
def transfer_customers_data():
    logging.info("Transferring data from transf_customers to dim_customers.")
    try:
        source_engine = source_postgres_connection()
        target_engine = target_postgres_connection()
        
        # Read data from the source database
        with source_engine.connect() as source_conn:
            query = "SELECT id AS customer_id, first_name, last_name, gender, address, zip_code, full_name FROM transf_customers"
            customers_df = pd.read_sql(query, source_conn)
        
        # Write data to the target database
        with target_engine.connect() as target_conn:
            customers_df.to_sql('dim_customers', target_conn, if_exists='append', index=False)
        
        logging.info("Data has been successfully transferred from 'transf_customers' to 'dim_customers'.")
    except Exception as e:
        logging.error(f"Failed to transfer data: {e}")
        raise

# Function to create the 'dim_login_attempt_history' table in the target database
def create_dim_login_attempt_history_table():
    logging.info("Creating dim_login_attempt_history table in the target database.")
    try:
        engine = target_postgres_connection()
        with engine.connect() as connection:
            connection.execute("""
                CREATE TABLE IF NOT EXISTS dim_login_attempt_history (
                    attempt_id SERIAL PRIMARY KEY,
                    customer_id INT,
                    login_succesful BOOLEAN,
                    attempted_at DATE,
                    period TIME
                );
            """)
        logging.info("Table 'dim_login_attempt_history' has been successfully created in the new database.")
    except Exception as e:
        logging.error(f"Failed to create table 'dim_login_attempt_history': {e}")
        raise

# Function to transfer data from the 'transf_login_attempt_history' table in the source database to the 'dim_login_attempt_history' table in the target database
def transfer_login_attempt_history_data():
    logging.info("Transferring data from transf_login_attempt_history to dim_login_attempt_history.")
    try:
        source_engine = source_postgres_connection()
        target_engine = target_postgres_connection()
        
        # Read data from the source database
        with source_engine.connect() as source_conn:
            query = " SELECT id AS attempt_id, customer_id, login_succesful, attempted_at, period FROM transf_login_attempt_history"
            login_attempt_history_df = pd.read_sql(query, source_conn)
        
        # Write data to the target database
        with target_engine.connect() as target_conn:
            login_attempt_history_df.to_sql('dim_login_attempt_history', target_conn, if_exists='append', index=False)
        
        logging.info("Data has been successfully transferred from 'transf_login_attempt_history' to 'dim_login_attempt_history'.")
    except Exception as e:
        logging.error(f"Failed to transfer data: {e}")
        raise

# Function to create the 'dim_product_categories' table in the target database
def create_dim_product_categories_table():
    logging.info("Creating dim_product_categories table in the target database.")
    try:
        engine = target_postgres_connection()
        with engine.connect() as connection:
            connection.execute("""
                CREATE TABLE IF NOT EXISTS dim_product_categories (
                    category_id SERIAL PRIMARY KEY,
                    name VARCHAR
                );
            """)
        logging.info("Table 'dim_product_categories' has been successfully created in the new database.")
    except Exception as e:
        logging.error(f"Failed to create table 'dim_product_categories': {e}")
        raise

# Function to transfer data from the 'transf_product_categories' table in the source database to the 'dim_product_categories' table in the target database
def transfer_product_categories_data():
    logging.info("Transferring data from transf_product_categories to dim_product_categories.")
    try:
        source_engine = source_postgres_connection()
        target_engine = target_postgres_connection()
        
        # Read data from the source database
        with source_engine.connect() as source_conn:
            query = " SELECT id AS category_id, name FROM transf_product_categories"
            product_categories_df = pd.read_sql(query, source_conn)
        
        # Write data to the target database
        with target_engine.connect() as target_conn:
            product_categories_df.to_sql('dim_product_categories', target_conn, if_exists='append', index=False)
        
        logging.info("Data has been successfully transferred from 'transf_product_categories' to 'dim_product_categories'.")
    except Exception as e:
        logging.error(f"Failed to transfer data: {e}")
        raise

# Function to create the 'dim_products' table in the target database
def create_dim_products_table():
    logging.info("Creating dim_products table in the target database.")
    try:
        engine = target_postgres_connection()
        with engine.connect() as connection:
            connection.execute("""
                CREATE TABLE IF NOT EXISTS dim_products (
                    product_id SERIAL PRIMARY KEY,
                    name VARCHAR,
                    price FLOAT,
                    category_id INT,
                    supplier_id INT
                );
            """)
        logging.info("Table 'dim_products' has been successfully created in the new database.")
    except Exception as e:
        logging.error(f"Failed to create table 'dim_products': {e}")
        raise

# Function to transfer data from the 'transf_products' table in the source database to the 'dim_products' table in the target database
def transfer_products_data():
    logging.info("Transferring data from transf_products to dim_products.")
    try:
        source_engine = source_postgres_connection()
        target_engine = target_postgres_connection()
        
        # Read data from the source database
        with source_engine.connect() as source_conn:
            query = " SELECT id AS product_id, name, price, category_id, supplier_id FROM transf_products"
            products_df = pd.read_sql(query, source_conn)
        
        # Write data to the target database
        with target_engine.connect() as target_conn:
            products_df.to_sql('dim_products', target_conn, if_exists='append', index=False)
        
        logging.info("Data has been successfully transferred from 'transf_products' to 'dim_products'.")
    except Exception as e:
        logging.error(f"Failed to transfer data: {e}")
        raise

# Function to create the 'dim_orders' table in the target database
def create_dim_orders_table():
    logging.info("Creating dim_orders table in the target database.")
    try:
        engine = target_postgres_connection()
        with engine.connect() as connection:
            connection.execute("""
                CREATE TABLE IF NOT EXISTS dim_orders (
                    orders_id SERIAL PRIMARY KEY,
                    customer_id INT,
                    status TEXT,
                    created_at DATE,
                    period_time TIME
                );
            """)
        logging.info("Table 'dim_orders' has been successfully created in the new database.")
    except Exception as e:
        logging.error(f"Failed to create table 'dim_orders': {e}")
        raise

# Function to transfer data from the 'transf_orders' table in the source database to the 'dim_orders' table in the target database
def transfer_orders_data():
    logging.info("Transferring data from transf_orders to dim_orders.")
    try:
        source_engine = source_postgres_connection()
        target_engine = target_postgres_connection()
        
        # Read data from the source database
        with source_engine.connect() as source_conn:
            query = " SELECT id AS orders_id, customer_id, status, created_at, period_time FROM transf_orders"
            orders_df = pd.read_sql(query, source_conn)
        
        # Write data to the target database
        with target_engine.connect() as target_conn:
            orders_df.to_sql('dim_orders', target_conn, if_exists='append', index=False)
        
        logging.info("Data has been successfully transferred from 'transf_orders' to 'dim_orders'.")
    except Exception as e:
        logging.error(f"Failed to transfer data: {e}")
        raise

# Function to create the 'dim_suppliers' table in the target database
def create_dim_suppliers_table():
    logging.info("Creating dim_suppliers table in the target database.")
    try:
        engine = target_postgres_connection()
        with engine.connect() as connection:
            connection.execute("""
                CREATE TABLE IF NOT EXISTS dim_suppliers (
                    supplier_id SERIAL PRIMARY KEY,
                    name VARCHAR,
                    country VARCHAR
                );
            """)
        logging.info("Table 'dim_suppliers' has been successfully created in the new database.")
    except Exception as e:
        logging.error(f"Failed to create table 'dim_suppliers': {e}")
        raise

# Function to transfer data from the 'transf_suppliers' table in the source database to the 'dim_suppliers' table in the target database
def transfer_suppliers_data():
    logging.info("Transferring data from transf_suppliers to dim_suppliers.")
    try:
        source_engine = source_postgres_connection()
        target_engine = target_postgres_connection()
        
        # Read data from the source database
        with source_engine.connect() as source_conn:
            query = "SELECT id AS supplier_id, name, country FROM transf_suppliers"
            suppliers_df = pd.read_sql(query, source_conn)
        
        # Write data to the target database
        with target_engine.connect() as target_conn:
            suppliers_df.to_sql('dim_suppliers', target_conn, if_exists='append', index=False)
        
        logging.info("Data has been successfully transferred from 'transf_suppliers' to 'dim_suppliers'.")
    except Exception as e:
        logging.error(f"Failed to transfer data: {e}")
        raise

# Function to create the 'dim_order_items' table in the target database
def create_dim_order_items_table():
    logging.info("Creating dim_order_items table in the target database.")
    try:
        engine = target_postgres_connection()
        with engine.connect() as connection:
            connection.execute("""
                CREATE TABLE IF NOT EXISTS dim_order_items (
                    order_items_id SERIAL PRIMARY KEY,
                    order_id INT,
                    product_id INT,
                    amount INT,
                    coupon_id INT
                );
            """)
        logging.info("Table 'dim_order_items' has been successfully created in the new database.")
    except Exception as e:
        logging.error(f"Failed to create table 'dim_order_items': {e}")
        raise

# Function to transfer data from the 'transf_order_items' table in the source database to the 'dim_order_items' table in the target database
def transfer_order_items_data():
    logging.info("Transferring data from transf_order_items to dim_order_items.")
    try:
        source_engine = source_postgres_connection()
        target_engine = target_postgres_connection()
        
        # Read data from the source database
        with source_engine.connect() as source_conn:
            query = "SELECT id AS order_items_id, order_id, product_id, amount, coupon_id FROM transf_order_items"
            order_items_df = pd.read_sql(query, source_conn)
        
        # Write data to the target database
        with target_engine.connect() as target_conn:
            order_items_df.to_sql('dim_order_items', target_conn, if_exists='append', index=False)
        
        logging.info("Data has been successfully transferred from 'transf_order_items' to 'dim_order_items'.")
    except Exception as e:
        logging.error(f"Failed to transfer data: {e}")
        raise

# Function to create the 'dim_shipping' table in the target database
def create_dim_shipping_table():
    logging.info("Creating dim_shipping table in the target database.")
    try:
        engine = target_postgres_connection()
        with engine.connect() as connection:
            connection.execute("""
                CREATE TABLE IF NOT EXISTS dim_shipping (
                    shipping_id SERIAL PRIMARY KEY,
                    order_id INT,
                    customer_id INT,
                    full_name VARCHAR,
                    address TEXT,
                    product_name VARCHAR
                );
            """)
        logging.info("Table 'dim_shipping' has been successfully created in the new database.")
    except Exception as e:
        logging.error(f"Failed to create table 'dim_shipping': {e}")
        raise

# Function to transfer data from the 'shipping' table in the source database to the 'dim_shipping' table in the target database
def transfer_shipping_data():
    logging.info("Transferring data from shipping to dim_shipping.")
    try:
        source_engine = source_postgres_connection()
        target_engine = target_postgres_connection()
        
        # Read data from the source database
        with source_engine.connect() as source_conn:
            query = " SELECT id AS shipping_id, order_id, customer_id, full_name, address, product_name FROM shipping"
            shipping_df = pd.read_sql(query, source_conn)
        
        # Write data to the target database
        with target_engine.connect() as target_conn:
            shipping_df.to_sql('dim_shipping', target_conn, if_exists='append', index=False)
        
        logging.info("Data has been successfully transferred from 'shipping' to 'dim_shipping'.")
    except Exception as e:
        logging.error(f"Failed to transfer data: {e}")
        raise

# Function to create the 'dim_sales_per_category' table in the target database
def create_dim_sales_per_category_table():
    logging.info("Creating dim_sales_per_category table in the target database.")
    try:
        engine = target_postgres_connection()
        with engine.connect() as connection:
            connection.execute("""
                CREATE TABLE IF NOT EXISTS dim_sales_per_category (
                    order_id SERIAL PRIMARY KEY,
                    customer_id INT,
                    category_id INT,
                    product_name VARCHAR,
                    price FLOAT,
                    created_at DATE
                );
            """)
        logging.info("Table 'dim_sales_per_category' has been successfully created in the new database.")
    except Exception as e:
        logging.error(f"Failed to create table 'dim_sales_per_category': {e}")
        raise

# Function to transfer data from the 'sales_per_category' table in the source database to the 'dim_sales_per_category' table in the target database
def transfer_sales_per_category_data():
    logging.info("Transferring data from sales_per_category to dim_sales_per_category.")
    try:
        source_engine = source_postgres_connection()
        target_engine = target_postgres_connection()
        
        # Read data from the source database
        with source_engine.connect() as source_conn:
            query = "SELECT order_id, customer_id, category_id, product_name, price, created_at FROM sales_per_category"
            sales_per_category_df = pd.read_sql(query, source_conn)
        
        # Drop duplicates based on order_id
        sales_per_category_df.drop_duplicates(subset=['order_id'], inplace=True)
        
        # Write deduplicated data to the target database
        with target_engine.connect() as target_conn:
            sales_per_category_df.to_sql('dim_sales_per_category', target_conn, if_exists='append', index=False)
        
        logging.info("Data has been successfully transferred from 'sales_per_category' to 'dim_sales_per_category'.")
    except Exception as e:
        logging.error(f"Failed to transfer data: {e}")
        raise

# Function to create the 'fact_order_details' table in the target database
def create_fact_order_details_table():
    logging.info("Creating fact_order_details table in the target database.")
    try:
        engine = target_postgres_connection()
        with engine.connect() as connection:
            connection.execute("""
                CREATE TABLE IF NOT EXISTS fact_order_details (
                    order_id SERIAL PRIMARY KEY,
                    customer_id INT,
                    status TEXT,
                    created_at DATE,
                    product_id INT,
                    amount INT,
                    coupon_id INT
                );
            """)
        logging.info("Table 'fact_order_details' has been successfully created in the new database.")
    except Exception as e:
        logging.error(f"Failed to create table 'fact_order_details': {e}")
        raise

# Function to transfer data from the 'order_details' table in the source database to the 'fact_order_details' table in the target database
def transfer_order_details_data():
    logging.info("Transferring data from order_details to fact_order_details.")
    try:
        source_engine = source_postgres_connection()
        target_engine = target_postgres_connection()
        
        # Read data from the source database
        with source_engine.connect() as source_conn:
            query = """
                WITH orders AS (
                    SELECT
                        oi.id AS order_id,
                        o.customer_id,
                        o.status,
                        o.created_at,
                        oi.product_id,
                        oi.amount,
                        oi.coupon_id
                    FROM transf_orders o
                    JOIN transf_order_items oi ON o.id = oi.order_id
                )
                SELECT * FROM orders
            """
            order_details_df = pd.read_sql(query, source_conn)
        
        # Write data to the target database
        with target_engine.connect() as target_conn:
            order_details_df.to_sql('fact_order_details', target_conn, if_exists='append', index=False)
        
        logging.info("Data has been successfully transferred from 'order_details' to 'fact_order_details'.")
    except Exception as e:
        logging.error(f"Failed to transfer data: {e}")
        raise

# Define the DAG
dag = DAG(
    'dim_fact_table_dag',
    default_args=default_args,
    description='DAG untuk membuat tabel baru dan mentransfer data',
    schedule_interval='@once',
)

# Define tasks for creating dim_coupons tables
create_dim_coupons = PythonOperator(
    task_id='create_dim_coupons',
    python_callable=create_dim_coupons_table,
    dag=dag,
)

# Define tasks for transferring data to dim_coupons
transfer_coupons = PythonOperator(
    task_id='transfer_coupons',
    python_callable=transfer_coupons_data,
    dag=dag,
)

# Define tasks for creating dim_customers tables
create_dim_customers = PythonOperator(
    task_id='create_dim_customers',
    python_callable=create_dim_customers_table,
    dag=dag,
)

# Define tasks for transferring data to dim_customers
transfer_customers = PythonOperator(
    task_id='transfer_customers',
    python_callable=transfer_customers_data,
    dag=dag,
)

# Define tasks for creating dim_login_attempt_history tables
create_dim_login_attempt_history = PythonOperator(
    task_id='create_dim_login_attempt_history',
    python_callable=create_dim_login_attempt_history_table,
    dag=dag,
)

# Define tasks for transferring data to dim_login_attempt_history
transfer_login_attempt_history = PythonOperator(
    task_id='transfer_login_attempt_history',
    python_callable=transfer_login_attempt_history_data,
    dag=dag,
)

# Define tasks for creating dim_product_categories tables
create_dim_product_categories = PythonOperator(
    task_id='create_dim_product_categories',
    python_callable=create_dim_product_categories_table,
    dag=dag,
)

# Define tasks for transferring data to dim_product_categories
transfer_product_categories = PythonOperator(
    task_id='transfer_product_categories',
    python_callable=transfer_product_categories_data,
    dag=dag,
)

# Define tasks for creating dim_products tables
create_dim_products = PythonOperator(
    task_id='create_dim_products',
    python_callable=create_dim_products_table,
    dag=dag,
)

# Define tasks for transferring data to dim_products
transfer_products = PythonOperator(
    task_id='transfer_products',
    python_callable=transfer_products_data,
    dag=dag,
)

# Define tasks for creating dim_orders tables
create_dim_orders = PythonOperator(
    task_id='create_dim_orders',
    python_callable=create_dim_orders_table,
    dag=dag,
)

# Define tasks for transferring data to dim_orders
transfer_orders = PythonOperator(
    task_id='transfer_orders',
    python_callable=transfer_orders_data,
    dag=dag,
)

# Define tasks for creating dim_suppliers tables
create_dim_suppliers = PythonOperator(
    task_id='create_dim_suppliers',
    python_callable=create_dim_suppliers_table,
    dag=dag,
)

# Define tasks for transferring data to dim_suppliers
transfer_suppliers = PythonOperator(
    task_id='transfer_suppliers',
    python_callable=transfer_suppliers_data,
    dag=dag,
)

# Define tasks for creating dim_order_items tables
create_dim_order_items = PythonOperator(
    task_id='create_dim_order_items',
    python_callable=create_dim_order_items_table,
    dag=dag,
)

# Define tasks for transferring data to dim_order_items
transfer_order_items = PythonOperator(
    task_id='transfer_order_items',
    python_callable=transfer_order_items_data,
    dag=dag,
)

# Define tasks for creating dim_shipping tables
create_dim_shipping = PythonOperator(
    task_id='create_dim_shipping',
    python_callable=create_dim_shipping_table,
    dag=dag,
)

# Define tasks for transferring data to dim_shipping
transfer_shipping = PythonOperator(
    task_id='transfer_shipping',
    python_callable=transfer_shipping_data,
    dag=dag,
)

# Define tasks for creating dim_sales_per_category tables
create_dim_sales_per_category = PythonOperator(
    task_id='create_dim_sales_per_category',
    python_callable=create_dim_sales_per_category_table,
    dag=dag,
)

# Define tasks for transferring data to dim_sales_per_category
transfer_sales_per_category = PythonOperator(
    task_id='transfer_sales_per_category',
    python_callable=transfer_sales_per_category_data,
    dag=dag,
)

# Define tasks for creating fact_order_details tables
create_fact_order_details = PythonOperator(
    task_id='create_fact_order_details',
    python_callable=create_fact_order_details_table,
    dag=dag,
)

# Define tasks for transferring data to fact_order_details
transfer_order_details = PythonOperator(
    task_id='transfer_order_details',
    python_callable=transfer_order_details_data,
    dag=dag,
)

# Define task dependencies
create_dim_coupons >> transfer_coupons
create_dim_customers >> transfer_customers
create_dim_login_attempt_history >> transfer_login_attempt_history
create_dim_product_categories >> transfer_product_categories
create_dim_products >> transfer_products
create_dim_orders >> transfer_orders
create_dim_suppliers >> transfer_suppliers
create_dim_order_items >> transfer_order_items
create_dim_shipping >> transfer_shipping
create_dim_sales_per_category >> transfer_sales_per_category
create_fact_order_details >> transfer_order_details
