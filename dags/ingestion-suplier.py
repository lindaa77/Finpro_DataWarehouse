# Impor modul dan library yang diperlukan
from datetime import datetime   # Mengimpor datetime untuk manipulasi waktu
import pandas as pd   # Mengimpor library pandas untuk manipulasi data dalam bentuk dataframe
from airflow import DAG   # Mengimpor kelas DAG dari library Airflow untuk membuat DAG
from airflow.operators.python_operator import PythonOperator   # Mengimpor operator PythonOperator untuk menjalankan fungsi Python dalam DAG
from airflow.hooks.postgres_hook import PostgresHook  # Mengimpor PostgresHook dari hooks Airflow untuk terhubung ke PostgreSQL
import xlrd   #untuk membaca dan mengakses data dari file Excel (.xls dan .xlsx)
from openpyxl import Workbook   # Mengimpor Workbook dari openpyxl untuk manipulasi file Excel

# Fungsi yang akan dijalankan oleh task dalam DAG
def supplier_funnel():
    hook = PostgresHook(postgres_conn_id="postgres_dw")  # Menginisialisasi koneksi PostgreSQL menggunakan PostgresHook
    engine = hook.get_sqlalchemy_engine()  # Mendapatkan SQLAlchemy engine dari hook

     # Membaca file Excel 'supplier.xls' dan menyimpannya ke tabel 'suplier' di PostgreSQL
    pd.read_excel("data/supplier.xls").to_sql("supplier", engine, if_exists="replace", index=False)

# Default arguments untuk DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

# Mendefinisikan DAG
dag = DAG(
    "ingest_supplier",
    default_args=default_args,
    description="supplier Data Ingestion",
    schedule_interval="@once",
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# Mendefinisikan task dalam DAG
task_load_supplier = PythonOperator(
     task_id="ingest_supplier",
     python_callable=supplier_funnel,
     dag=dag,
)

# Menjalankan tugas (task) load supplier
task_load_supplier
