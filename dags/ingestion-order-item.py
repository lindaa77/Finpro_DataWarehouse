
# Impor modul dan library yang diperlukan
import fastavro  # Mengimpor library fastavro untuk membaca file avro
import struct  # Mengimpor library struct untuk bekerja dengan data binary 
import json  # Mengimpor library json untuk bekerja dengan data JSON
import pandas as pd  # Mengimpor library pandas untuk manipulasi data dalam bentuk dataframe
from datetime import datetime  # Mengimpor datetime dari library datetime untuk bekerja dengan tanggal dan waktu
from sqlalchemy import create_engine  # Mengimpor create_engine dari SQLAlchemy untuk membuat engine koneksi database
from airflow import DAG  # Mengimpor DAG dari Airflow untuk membuat alur kerja (workflow)
from airflow.operators.python_operator import PythonOperator  # Mengimpor PythonOperator untuk menjalankan fungsi Python sebagai tugas di Airflow
from airflow.hooks.postgres_hook import PostgresHook  # Mengimpor PostgresHook untuk menghubungkan Airflow dengan PostgreSQL

# Fungsi untuk memuat data order item dari file Avro ke tabel PostgreSQL
def order_item_funnel():

    # Menginisialisasi hook PostgreSQL dan engine SQLAlchemy
    hook = PostgresHook(postgres_conn_id="postgres_dw")   # Membuat hook PostgreSQL dengan ID koneksi 'postgres_dw'
    engine = hook.get_sqlalchemy_engine()  # Mendapatkan engine SQLAlchemy dari hook PostgreSQL

    # Membuka file Avro dalam mode baca-biner
    with open("data/order_item.avro", 'rb') as f:
        reader = fastavro.reader(f)  # Membaca data dari file Avro
        pd.DataFrame(list(reader)).to_sql("order item", engine, if_exists="replace", index=False) # Mengubah data menjadi DataFrame pandas dan menyimpannya ke tabel 'order item'

# Menentukan argumen default untuk DAG
default_args = {
    "owner": "airflow",   # Pemilik DAG
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,   # Jumlah retry jika task gagal
}

# Mendefinisikan DAG dengan parameter yang ditentukan
dag = DAG(
    "ingest_order_item",                        # ID DAG
    default_args=default_args,                  # Argument default untuk DAG
    description="Order Item Data Ingestion",    # Deskripsi dari DAG
    schedule_interval="@once",                  # Jadwal DAG dijalankan sekali
    start_date=datetime(2023, 1, 1),            # Tanggal mulai DAG
    catchup=False,
)

# Mendefinisikan task PythonOperator dalam DAG
task_load_order_item = PythonOperator(
     task_id="load_order_item",                # ID task
     python_callable=order_item_funnel,        # Fungsi Python yang akan dieksekusi
     dag=dag,                                  # Menghubungkan tugas dengan DAG yang sudah didefinisikan
)

# Menjalankan tugas (task) load order item
task_load_order_item
