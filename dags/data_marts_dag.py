# Import necessary libraries and modules
import datetime
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
        password='gkZW8NFvC0mG',
        host='ep-mute-wind-a5di8srb.us-east-2.aws.neon.tech',
        database='data_warehouse',
        port=5432,
        query={'sslmode': 'require'}
    )
    engine = create_engine(connection_string)
    return engine

# Function to create the customer CLV table
def create_customer_clv_table():
    engine = postgres_connection()
    with engine.connect() as connection:
        connection.execute("""
            CREATE TABLE IF NOT EXISTS marts_cust_highest_clv AS
            WITH customer_orders AS (
                SELECT
                    o.customer_id,
                    COUNT(DISTINCT o.order_id) AS total_orders,
                    SUM(p.price * o.amount) AS total_spending
                FROM fact_order_details o
                JOIN dim_products p ON o.product_id = p.product_id
                WHERE o.status = 'FINISHED'
                GROUP BY o.customer_id
            ),
            customer_clv AS (
                SELECT
                    c.customer_id,
                    c.full_name,
                    co.total_orders,
                    co.total_spending,
                    co.total_spending / NULLIF(co.total_orders, 0) AS average_order_value,
                    co.total_spending * 10 AS customer_lifetime_value
                FROM dim_customers c
                JOIN customer_orders co ON c.customer_id = co.customer_id
            )
            SELECT
                customer_id,
                full_name,
                total_orders,
                total_spending,
                average_order_value,
                customer_lifetime_value
            FROM customer_clv
            ORDER BY customer_lifetime_value DESC
            LIMIT 7;
        """)
    print("Table 'marts_cust_highest_clv' has been successfully created.")

# Function to create the burn rate table
def create_burn_rate_table():
    engine = postgres_connection()
    with engine.connect() as connection:
        connection.execute("""
            CREATE TABLE IF NOT EXISTS marts_burn_rate AS
            SELECT
                DATE_TRUNC('day', o.created_at) AS day,
                SUM(p.price * o.amount * COALESCE(c.numeric_value, 0)) AS total_discount_given
            FROM fact_order_details o
            JOIN dim_products p ON o.product_id = p.product_id
            LEFT JOIN dim_coupons c ON o.coupon_id = c.coupon_id
            GROUP BY DATE_TRUNC('day', o.created_at)
            ORDER BY day;
        """)
    print("Table 'marts_burn_rate' has been successfully created.")

# Function to create the quartiles CLV table
def create_quartiles_clv_table():
    engine = postgres_connection()
    with engine.connect() as connection:
        connection.execute("""
            CREATE TABLE IF NOT EXISTS marts_quartiles_clv AS
            WITH customer_orders AS (
                SELECT
                    o.customer_id,
                    SUM(p.price * o.amount) AS total_spending
                FROM fact_order_details o
                JOIN dim_products p ON o.product_id = p.product_id
                WHERE o.status = 'FINISHED'
                GROUP BY o.customer_id
            ),
            customer_clv AS (
                SELECT
                    c.customer_id,
                    c.full_name,
                    co.total_spending,
                    CASE
                        WHEN NTILE(4) OVER (ORDER BY co.total_spending) <= 1 THEN 'Low CLV'
                        WHEN NTILE(4) OVER (ORDER BY co.total_spending) > 1 AND NTILE(4) OVER (ORDER BY co.total_spending) <= 3 THEN 'Medium CLV'
                        ELSE 'High CLV'
                    END AS clv_segment
                FROM dim_customers c
                JOIN customer_orders co ON c.customer_id = co.customer_id
            )
            SELECT
                clv_segment,
                COUNT(*) AS total_customers,
                COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS percentage_of_customers
            FROM customer_clv
            GROUP BY clv_segment
            ORDER BY 
                CASE 
                    WHEN clv_segment = 'Low CLV' THEN 1
                    WHEN clv_segment = 'Medium CLV' THEN 2
                    WHEN clv_segment = 'High CLV' THEN 3
                    ELSE 4
                END;
        """)
    print("Table 'marts_quartiles_clv' has been successfully created.")

# Function to create the sales performance table
def create_sales_performance_table():
    engine = postgres_connection()
    with engine.connect() as connection:
        connection.execute("""
            CREATE TABLE IF NOT EXISTS marts_sales_performance AS
            WITH daily_sales AS (
                SELECT
                    DATE(o.created_at) AS sales_date,
                    COUNT(DISTINCT o.order_id) AS total_orders,
                    SUM(p.price * oi.amount) AS total_sales,
                    SUM(oi.amount) AS total_units_sold
                FROM fact_order_details o
                JOIN dim_order_items oi ON o.order_id = oi.order_id
                JOIN dim_products p ON oi.product_id = p.product_id
                WHERE o.status = 'FINISHED'
                GROUP BY DATE(o.created_at)
            )
            SELECT
                sales_date,
                total_orders,
                total_sales,
                total_units_sold,
                total_sales / NULLIF(total_units_sold, 0) AS average_price_per_unit
            FROM daily_sales
            ORDER BY sales_date;
        """)
    print("Table 'marts_sales_performance' has been successfully created.")

# Function to create the top products sales table
def create_top_products_sales_table():
    engine = postgres_connection()
    with engine.connect() as connection:
        connection.execute("""    
        CREATE TABLE IF NOT EXISTS marts_top_products_sales AS
            WITH product_sales AS (
                SELECT
                    p.product_id,
                    p.name AS product_name,
                    SUM(o.amount * p.price) AS total_sales_amount
                FROM fact_order_details o
                JOIN dim_products p ON o.product_id = p.product_id
                WHERE o.status = 'FINISHED'
                GROUP BY p.product_id, p.name
            )
            SELECT
                product_id,
                product_name,
                total_sales_amount
            FROM product_sales
            ORDER BY total_sales_amount DESC
            LIMIT 5;
        """)
    print("Table 'marts_top_products_sales' has been successfully created.")

# Function to create the category sales customers table
def create_category_sales_customers_table():
    engine = postgres_connection()
    with engine.connect() as connection:
        connection.execute("""
            CREATE TABLE IF NOT EXISTS marts_category_sales_customers AS
            WITH category_sales_customers AS (
                SELECT
                    c.category_id,
                    c.name AS category_name,
                    COUNT(DISTINCT od.customer_id) AS total_customers,
                    SUM(od.amount * p.price) AS total_sales
                FROM dim_product_categories c
                JOIN dim_products p ON c.category_id = p.category_id
                JOIN fact_order_details od ON p.product_id = od.product_id
                WHERE od.status = 'FINISHED'
                GROUP BY c.category_id, c.name
            )
            SELECT
                category_id,
                category_name,
                total_customers,
                total_sales
            FROM category_sales_customers
            ORDER BY total_sales DESC;
        """)
    print("Table 'marts_category_sales_customers' has been successfully created.")

# Defining the DAG
with DAG(
    dag_id='data_marts_dag',
    default_args=default_args,
    schedule_interval='@once',
) as dag:

    # Defining the tasks
    create_customer_clv_table_task = PythonOperator(
        task_id='create_customer_clv_table_task',
        python_callable=create_customer_clv_table
    )

    create_burn_rate_table_task = PythonOperator(
        task_id='create_burn_rate_table_task',
        python_callable=create_burn_rate_table
    )

    create_quartiles_clv_table_task = PythonOperator(
        task_id='create_quartiles_clv_table_task',
        python_callable=create_quartiles_clv_table
    )

    create_sales_performance_table_task = PythonOperator(
        task_id='create_sales_performance_table_task',
        python_callable=create_sales_performance_table
    )

    create_top_products_sales_table_task = PythonOperator(
        task_id='create_top_products_sales_table_task',
        python_callable=create_top_products_sales_table
    )

    create_category_sales_customers_table_task = PythonOperator(
        task_id='create_category_sales_customers_table_task',
        python_callable=create_category_sales_customers_table
    )

    # Defining task dependencies
    create_customer_clv_table_task >> create_burn_rate_table_task >> create_quartiles_clv_table_task >> create_sales_performance_table_task >> create_top_products_sales_table_task >> create_category_sales_customers_table_task
