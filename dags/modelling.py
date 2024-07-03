from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import pandas as pd

# Database connection
DW_POSTGRES_URI = "postgresql://data_warehouse_owner:A0UBgOZqdx4P@ep-withered-block-a52quxm0.us-east-2.aws.neon.tech/finalproject?sslmode=require"
engine = create_engine(DW_POSTGRES_URI)

# Define the ETL function for creating orders_fact
def create_order_fact2():
    # Read tables
    order = pd.read_sql_table('order', engine)
    order_item = pd.read_sql_table('order_item', engine)
    product = pd.read_sql_table('product', engine)
    product_category = pd.read_sql_table('product_category', engine)
    supplier = pd.read_sql_table('supplier', engine)
    coupons = pd.read_sql_table('coupons', engine)

    # Debugging: Log the column names
    print(f"Order columns: {order.columns}")
    print(f"Order Item columns: {order_item.columns}")
    print(f"Product columns: {product.columns}")
    print(f"Product category columns: {product_category.columns}")
    print(f"Supplier columns: {supplier.columns}")
    print(f"Coupons columns: {coupons.columns}")

    # Remove duplicates and handle missing values
    order = order.drop_duplicates().dropna()
    order_item = order_item.drop_duplicates().dropna()
    product = product.drop_duplicates().dropna()
    product_category = product_category.drop_duplicates().dropna()
    supplier = supplier.drop_duplicates().dropna()
    coupons = coupons.drop_duplicates().dropna()

    # Merge orders with order_items
    order_fact2 = pd.merge(order, order_item, left_on='id', right_on='order_id')

    # Merge with products to get product details
    order_fact2 = pd.merge(order_fact2, product, left_on='product_id', right_on='id', suffixes=('', '_product'))
   
    # Merge with coupons to get discount information
    order_fact2 = pd.merge(order_fact2, coupons, left_on='coupon_id', right_on='id', suffixes=('', '_coupons'))

    # Remove duplicates and handle missing values in order_fact
    order_fact2 = order_fact2.drop_duplicates().dropna()

    # Load into the new fact table
    order_fact2.to_sql('order_fact2', engine, if_exists='replace', index=False)

# Define the function to get sales by supplier country
def get_sales_by_supplier_country():
    # Read tables
    order = pd.read_sql_table('order', engine)
    order_item = pd.read_sql_table('order_item', engine)
    product = pd.read_sql_table('product', engine)
    supplier = pd.read_sql_table('supplier', engine)

    # Remove duplicates and handle missing values
    order = order.drop_duplicates().dropna()
    order_item = order_item.drop_duplicates().dropna()
    product = product.drop_duplicates().dropna()
    supplier = supplier.drop_duplicates().dropna()

    # Merge orders with order_items
    merged_data = pd.merge(order, order_item, left_on='id', right_on='order_id')

    # Merge with products to get product details
    merged_data = pd.merge(merged_data, product, left_on='product_id', right_on='id', suffixes=('', '_product'))

    # Merge with suppliers to get supplier details
    merged_data = pd.merge(merged_data, supplier, left_on='supplier_id', right_on='id', suffixes=('', '_supplier'))

    # Group by supplier country and calculate the number of products ordered
    sales_by_country = merged_data.groupby('country')['amount'].sum().reset_index()

    # Sort the results by the number of products ordered in descending order
    sales_by_country = sales_by_country.sort_values(by='amount', ascending=False)

    # Save the result to a table in the database
    sales_by_country.to_sql('sales_by_country', engine, if_exists='replace', index=False)

# Define the function to get sales by product category
def get_sales_by_product_category():
    # Read tables
    order = pd.read_sql_table('order', engine)
    order_item = pd.read_sql_table('order_item', engine)
    product = pd.read_sql_table('product', engine)
    product_category = pd.read_sql_table('product_category', engine)

    # Remove duplicates and handle missing values
    order = order.drop_duplicates().dropna()
    order_item = order_item.drop_duplicates().dropna()
    product = product.drop_duplicates().dropna()
    product_category = product_category.drop_duplicates().dropna()

    # Merge orders with order_items
    merged_data = pd.merge(order, order_item, left_on='id', right_on='order_id')

    # Merge with products to get product details
    merged_data = pd.merge(merged_data, product, left_on='product_id', right_on='id', suffixes=('', '_product'))

    # Merge with product_category to get category details
    merged_data = pd.merge(merged_data, product_category, left_on='category_id', right_on='id', suffixes=('', '_category'))

    # Group by product category and calculate the number of products sold
    sales_by_category = merged_data.groupby('name_category')['amount'].sum().reset_index()

    # Sort the results by the number of products sold in descending order
    sales_by_category = sales_by_category.sort_values(by='amount', ascending=False)

    # Save the result to a table in the database
    sales_by_category.to_sql('sales_by_category', engine, if_exists='replace', index=False)

def get_sales_by_month():
    # Read tables
    order = pd.read_sql_table('order', engine)
    order_item = pd.read_sql_table('order_item', engine)

    # Remove duplicates and handle missing values
    order = order.drop_duplicates().dropna()
    order_item = order_item.drop_duplicates().dropna()

    # Merge orders with order_items
    merged_data = pd.merge(order, order_item, left_on='id', right_on='order_id')

    # Convert created_at to datetime
    merged_data['created_at'] = pd.to_datetime(merged_data['created_at'])

    # Extract month and year from created_at
    merged_data['order_month'] = merged_data['created_at'].dt.to_period('M').astype(str)  # Convert Period to string

    # Group by month and calculate total sales
    monthly_sales = merged_data.groupby('order_month')['amount'].sum().reset_index()

    # Save the result to a table in the database
    monthly_sales.to_sql('monthly_sales', engine, if_exists='replace', index=False)


def get_sales_trend_analysis():
    # Load necessary tables from the database
    order_fact2 = pd.read_sql_table('order_fact2', engine)

    # Aggregate sales metrics over time
    sales_trend = order_fact2.groupby(pd.Grouper(key='created_at', freq='M')).agg(
        total_sales=('amount', 'sum'),
        total_revenue=('price', lambda x: (x * order_fact2.loc[x.index, 'amount']).sum()),
        average_order_value=('order_id', lambda x: (x * order_fact2.loc[x.index, 'amount']).mean()),
        number_of_orders=('order_id', 'count')
    ).reset_index()

    # Calculate growth rate
    sales_trend['growth_rate'] = sales_trend['total_sales'].pct_change()

    # Load into a new table
    sales_trend.to_sql('sales_trend_analysis', engine, if_exists='replace', index=False)

def product_sales_performance():
    # Load the order_fact table from the database
    order_fact2 = pd.read_sql_table('order_fact2', engine)

    # Aggregate sales metrics by product category
    product_sales = order_fact2.groupby('name').agg(
        total_sales=('amount', 'sum'),
        total_orders=('order_id', 'nunique')
    ).reset_index()

    # Calculate additional metrics if needed, e.g., average sales per order

    # Determine which category has the highest and lowest sales
    most_popular_category = product_sales.loc[product_sales['total_sales'].idxmax()]
    least_popular_category = product_sales.loc[product_sales['total_sales'].idxmin()]

    # Print or log the results
    print("Most popular category:", most_popular_category['name'], "with total sales:", most_popular_category['total_sales'])
    print("Least popular category:", least_popular_category['name'], "with total sales:", least_popular_category['total_sales'])

    # Optionally, save the results to a new table in the database
    product_sales.to_sql('product_sales_performance', engine, if_exists='replace', index=False)

def create_customer_purchase_behavior():
    # Load the order_fact table from the database
    order_fact2 = pd.read_sql_table('order_fact2', engine)

    # Remove duplicates and handle missing values in order_fact
    order_fact2 = order_fact2.drop_duplicates().dropna()

    # Calculate purchase count, average order value, and total spending
    customer_purchase_behavior = order_fact2.groupby('customer_id').agg(
        purchase_count=('order_id', 'nunique'),
        avg_order_value=('price', lambda x: (x * order_fact2.loc[x.index, 'amount']).mean()),
        total_spending=('price', lambda x: (x * order_fact2.loc[x.index, 'amount']).sum())
    ).reset_index()

    # Load the result into a new table
    customer_purchase_behavior.to_sql('customer_purchase_behavior', engine, if_exists='replace', index=False)


def get_customer_order_status():
    order = pd.read_sql_table('order', engine)
    order_item = pd.read_sql_table('order_item', engine)
    product = pd.read_sql_table('product', engine)
    customers = pd.read_sql_table('customers', engine)

    # Remove duplicates and handle missing values
    order = order.drop_duplicates().dropna()
    order_item = order_item.drop_duplicates().dropna()
    product = product.drop_duplicates().dropna()
    customers = customers.drop_duplicates().dropna()

    # Merge orders with order_items
    merged_data = pd.merge(order, order_item, left_on='id', right_on='order_id')

    # Merge with products to get product details
    merged_data = pd.merge(merged_data, product, left_on='product_id', right_on='id', suffixes=('', '_product'))

    # Merge with customers to get customer details
    merged_data = pd.merge(merged_data, customers, left_on='customer_id', right_on='id', suffixes=('', '_customers'))

    # Selecting necessary columns after merging
    merged_data = merged_data[['customer_id', 'status', 'created_at']]

    # Group by customer and calculate the total amount ordered
    customer_order_status = merged_data.groupby('customer_id').agg({'created_at': 'max', 'status': 'count'}).reset_index()

    # Sort the results by the 'created_at' in descending order
    customer_order_status = customer_order_status.sort_values(by='created_at', ascending=False)



# Define the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'data_modelling',
    default_args=default_args,
    description='A DAG for modelling',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

create_order_fact2_task = PythonOperator(
    task_id='create_order_fact2',
    python_callable=create_order_fact2,
    dag=dag,
)

get_sales_by_supplier_country_task = PythonOperator(
    task_id='get_sales_by_supplier_country',
    python_callable=get_sales_by_supplier_country,
    dag=dag,
)

get_sales_by_product_category_task = PythonOperator(
    task_id='get_sales_by_product_category',
    python_callable=get_sales_by_product_category,
    dag=dag,
)

get_sales_by_month_task = PythonOperator(
    task_id='get_sales_by_month',
    python_callable=get_sales_by_month,
    dag=dag,
)

get_sales_trend_analysis_task = PythonOperator(
    task_id='get_sales_trend_analysis',
    python_callable=get_sales_trend_analysis,
    dag=dag,
)

product_sales_performance_task = PythonOperator(
    task_id='product_sales_performance',
    python_callable=product_sales_performance,
    dag=dag,
)

create_customer_purchase_behavior_task = PythonOperator(
    task_id='create_customer_purchase_behavior',
    python_callable=create_customer_purchase_behavior,
    dag=dag,
)

get_customer_order_status_task = PythonOperator(
    task_id='get_customer_order_status',
    python_callable=get_customer_order_status,
    dag=dag,
)

# Set task dependencies
create_order_fact2_task >> get_sales_by_supplier_country_task >> get_sales_by_product_category_task >> get_sales_trend_analysis_task >> product_sales_performance_task >> create_customer_purchase_behavior_task >> get_customer_order_status_task >> get_sales_by_month_task 
