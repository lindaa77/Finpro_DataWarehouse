# Impor modul dan library yang diperlukan
import pandas as pd  # Mengimpor pustaka pandas untuk manipulasi data
import glob  # Mengimpor modul glob untuk operasi path file
import os  # Mengimpor modul os untuk fungsionalitas sistem operasi
from datetime import datetime  # Mengimpor modul datetime untuk operasi tanggal dan waktu
from airflow import DAG  # Mengimpor kelas DAG dari Airflow untuk mendefinisikan DAG
from airflow.operators.python_operator import PythonOperator  # Mengimpor PythonOperator untuk tugas fungsi Python
from airflow.hooks.postgres_hook import PostgresHook  # Mengimpor PostgresHook untuk interaksi dengan PostgreSQL
from sqlalchemy import create_engine  # Mengimpor create_engine dari SQLAlchemy untuk interaksi dengan basis data

# Fungsi untuk memproses data pelanggan
def customers_funnel():
    # Menggabungkan semua file CSV di direktori 'data' menjadi satu DataFrame dan menyimpannya ke /tmp/customer_data.csv
    pd.concat((pd.read_csv(file) for file in glob.glob(os.path.join("data", '*.csv'))), ignore_index=True).to_csv("/tmp/customer_data.csv", index=False)

    # Menginisialisasi koneksi PostgreSQL dan mesin SQLAlchemy
    hook = PostgresHook(postgres_conn_id="postgres_dw")  # Membuat koneksi PostgreSQL dengan ID koneksi "postgres_dw"
    engine = hook.get_sqlalchemy_engine() # Mendapatkan SQLAlchemy dari hook


    # Membaca data pelanggan dari /tmp/customer_data.csv dan mengganti tabel 'customers'
    pd.read_csv("/tmp/customer_data.csv").to_sql("customers", engine, if_exists="replace", index=False)


# Menentukan argumen default untuk DAG
default_args = {
    "owner": "airflow",  # Pemilik DAG
    "depends_on_past": False,  
    "email_on_failure": False,
    "email_on_retry": False,  
    "retries": 1,  # Jumlah retry jika tugas gagal
}

# Mendefinisikan DAG dengan atribut yang sudah ditentukan
dag = DAG(
    "ingest_customer",                      # Id untuk DAG
    default_args=default_args,              # Menggunakan argumen default yang sudah ditentukan
    description="Customer Data Ingestion",  # Deskripsi dari DAG
    schedule_interval="@once",              # DAG hanya dijalankan sekali
    start_date=datetime(2023, 1, 1),        # Tanggal mulai operasi DAG
    catchup=False,                          # Tidak mengeksekusi yang terlewatkan
)

# Mendefinisikan tugas(task) untuk menjalankan fungsi customers_funnel
task_load_customer = PythonOperator(
     task_id="ingest_customer",             # Id untuk tugas
     python_callable=customers_funnel,      # Fungsi Python yang akan dieksekusi
     dag=dag,                               # Menghubungkan tugas dengan DAG yang sudah didefinisikan
)

# Menjalankan tugas (task) load customer
task_load_customer  
