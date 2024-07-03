# Impor modul dan library yang diperlukan
import pyarrow.parquet as pq  # Import modul pyarrow untuk menangani file Parquet
import pyarrow as pa
from airflow import DAG  # Import modul Airflow untuk mendefinisikan dan mengelola DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator  # Import PythonOperator untuk menjalankan fungsi Python dalam DAG
from airflow.hooks.postgres_hook import PostgresHook  
# Import PostgresHook untuk menghubungkan Airflow dengan PostgreSQL
from sqlalchemy import create_engine  # Import create_engine dari SQLAlchemy untuk membuat engine SQL

# Default arguments untuk DAG
default_args = {
    "owner": "airflow",                  # Pemilik DAG
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,                       # Jumlah retry jika task gagal
}

# Mendefinisikan DAG
dag = DAG(
    "ingest_order",                     # ID DAG
    default_args=default_args,          # Default arguments untuk DAG
    description="Order Data Ingestion",  # Deskripsi dari DAG
    schedule_interval="@once",          # Jadwal eksekusi DAG, sekali
    start_date=datetime(2023, 1, 1),    # Tanggal mulai eksekusi DAG
    catchup=False,                      # Tidak menjalankan task lama 
)

# Fungsi yang akan dijalankan oleh task dalam DAG
def order_funnel():
    # Inisialisasi hook PostgreSQL dan engine SQLAlchemy
    hook = PostgresHook(postgres_conn_id="postgres_dw")
    engine = hook.get_sqlalchemy_engine()

    # Membaca file Parquet menggunakan pyarrow
    table = pq.read_table("data/order.parquet")
    df = table.to_pandas()  # Konversi pyarrow table ke pandas dataframe
    
    # Menulis DataFrame ke database PostgreSQL
    df.to_sql("order", engine, if_exists="replace", index=False)

# Mendefinisikan task dalam DAG
task_load_order = PythonOperator(
    task_id="load_order",               # ID dari task
    python_callable=order_funnel,       # Fungsi yang akan dijalankan oleh task
    dag=dag,                            # DAG tempat task ini akan dieksekusi
)

# Menjalankan tugas (task) load order
task_load_order
