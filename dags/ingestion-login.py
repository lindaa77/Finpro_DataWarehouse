# Impor modul dan library yang diperlukan
import pandas as pd  # Mengimpor library pandas untuk manipulasi data
import glob  # Mengimpor modul glob untuk manipulasi path file
import os  # Mengimpor modul os untuk fungsi-fungsi sistem operasi
import psycopg2  # Mengimpor library psycopg2 untuk interaksi dengan database PostgreSQL
import json  # Mengimpor modul json untuk manipulasi data JSON
import csv  # Mengimpor modul csv untuk operasi file CSV
import pyarrow  # Mengimpor library pyarrow untuk manipulasi data dalam format Arrow
from datetime import datetime  # Mengimpor class datetime untuk operasi tanggal dan waktu
from airflow import DAG  # Mengimpor class DAG dari Airflow untuk mendefinisikan workflows
from airflow.operators.python_operator import PythonOperator  # Mengimpor PythonOperator untuk menjalankan fungsi Python dalam DAG Airflow
from airflow.hooks.postgres_hook import PostgresHook  # Mengimpor PostgresHook untuk koneksi ke database PostgreSQL
from sqlalchemy import create_engine  # Mengimpor fungsi create_engine dari SQLAlchemy untuk interaksi dengan database

# Fungsi untuk memproses data login attempts
def login_attempts_funnel():
    
    # Menggabungkan semua file JSON di direktori 'data' menjadi satu DataFrame dan disimpan sebagai JSON
    pd.concat((pd.read_json(file) for file in glob.glob(os.path.join("data", '*.json'))), ignore_index=True).to_json("/tmp/login_attempts.json", index=False)

    # Menginisialisasi PostgreSQL hook
    hook = PostgresHook(postgres_conn_id="postgres_dw")
    engine = hook.get_sqlalchemy_engine()  # Mendapatkan engine SQLAlchemy dari PostgreSQL hook

    # Membaca file JSON dan menulis isinya ke tabel 'login_attempts' di database PostgreSQL
    pd.read_json("/tmp/login_attempts.json").to_sql("login_attempts", engine, if_exists="replace", index=False)

# Menentukan argumen default untuk DAG
default_args = {
    "owner": "airflow",  # Pemilik DAG
    "depends_on_past": False,  
    "email_on_failure": False,  
    "email_on_retry": False,  
    "retries": 1,  # Jumlah retry jika task gagal
}

# Mendefinisikan DAG dengan parameter yang ditentukan
dag = DAG(
    "ingest_login_attempts",                  # ID DAG
    default_args=default_args,                # Argument default untuk DAG
    description="Ingestion percobaan login",  # Deskripsi dari DAG
    schedule_interval="@once",                # Jadwal DAG dijalankan sekali
    start_date=datetime(2023, 1, 1),          # Tanggal mulai DAG
    catchup=False,  
)

# Mendefinisikan task PythonOperator dalam DAG
task_load_login = PythonOperator(
     task_id="ingest_login_attempts",         # ID task
     python_callable=login_attempts_funnel,   # Fungsi Python yang akan dieksekusi
     dag=dag,                                 # Menghubungkan tugas dengan DAG yang sudah didefinisikan
)

# Menjalankan tugas (task) load login
task_load_login  
