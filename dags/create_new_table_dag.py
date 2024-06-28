# Import necessary libraries and modules
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from sqlalchemy.engine import URL, create_engine

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime.datetime(2024, 6, 23),
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Function to establish a connection to the PostgreSQL database
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

# Function to create the shipping table
def create_shipping_table():
    engine = postgres_connection()
    with engine.connect() as connection:
        connection.execute("""
            CREATE TABLE IF NOT EXISTS shipping (
                id SERIAL PRIMARY KEY,
                order_id INT,
                customer_id INT,
                full_name VARCHAR(100),
                address TEXT,
                product_name VARCHAR(100)
            );

            INSERT INTO shipping (order_id, customer_id, full_name, address, product_name)
            SELECT 
                oi.order_id,
                o.customer_id,
                c.full_name,
                c.address,
                p.name AS product_name
            FROM transf_order_items oi
            JOIN transf_orders o ON oi.order_id = o.id
            JOIN transf_customers c ON o.customer_id = c.id
            JOIN transf_products p ON oi.product_id = p.id;
        """)
    print("Table 'shipping' has been successfully created.")

# Function to create the sales_per_category table
def create_sales_per_category_table():
    engine = postgres_connection()
    with engine.connect() as connection:
        connection.execute("""
            CREATE TABLE IF NOT EXISTS sales_per_category AS
            SELECT 
                oi.order_id,  
                o.customer_id,  
                p.category_id,  
                p.name AS product_name,
                p.price,  
                o.created_at  
            FROM transf_order_items oi
            JOIN transf_orders o ON oi.order_id = o.id
            JOIN transf_products p ON oi.product_id = p.id
        """)
    print("Table 'sales_per_category' has been successfully created.")

# Defining the DAG
with DAG(
    dag_id='create_new_table_dag',
    default_args=default_args,
    schedule_interval='@once',
) as dag:

    # Defining the tasks
    create_shipping_table_task = PythonOperator(
        task_id='create_shipping_table_task',
        python_callable=create_shipping_table
    )

    create_sales_per_category_table_task = PythonOperator(
        task_id='create_sales_per_category_table_task',
        python_callable=create_sales_per_category_table
    )

    # Defining task dependencies
    create_shipping_table_task >> create_sales_per_category_table_task
