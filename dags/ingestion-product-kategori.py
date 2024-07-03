# Impor modul dan library yang diperlukan
import pandas as pd  # Mengimpor library pandas untuk manipulasi data dalam bentuk dataframe
import xlrd  # Mengimpor modul xlrd untuk membaca file Excel
from airflow import DAG  # Mengimpor kelas DAG dari library Airflow untuk membuat DAG
from datetime import datetime  # Mengimpor datetime untuk manipulasi waktu
from airflow.operators.python_operator import PythonOperator   # Mengimpor operator PythonOperator untuk menjalankan fungsi Python dalam DAG
from airflow.hooks.postgres_hook import PostgresHook  # Mengimpor PostgresHook dari hooks Airflow untuk terhubung ke PostgreSQL
from openpyxl import Workbook   # Mengimpor Workbook dari openpyxl untuk manipulasi file Excel

# Fungsi yang akan dijalankan oleh task dalam DAG
def product_category_funnel():
    hook = PostgresHook(postgres_conn_id="postgres_dw")  # Menginisialisasi koneksi PostgreSQL menggunakan PostgresHook
    engine = hook.get_sqlalchemy_engine()  # Mendapatkan SQLAlchemy engine dari hook

     # Membaca file Excel 'product_category.xls' dan menyimpannya ke tabel 'product_category' di PostgreSQL,
    pd.read_excel("data/product_category.xls").to_sql("product_category", engine, if_exists="replace", index=False)

# Default arguments untuk DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

dag = DAG(
    "ingest_product_category",
    default_args=default_args,
    description="product category Data Ingestion",
    schedule_interval="@once",
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# Mendefinisikan task dalam DAG
task_load_product_category= PythonOperator(
     task_id="ingest_product_category",
     python_callable=product_category_funnel,
     dag=dag,
)

# Menjalankan tugas (task) load product category
task_load_product_category
