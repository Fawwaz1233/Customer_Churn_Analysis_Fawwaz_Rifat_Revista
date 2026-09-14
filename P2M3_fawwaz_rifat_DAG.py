import datetime as dt
from datetime import timedelta
import pandas as pd
import psycopg2
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from elasticsearch import Elasticsearch

# Path file CSV
raw_path = "/opt/airflow/dags/P2M3_fawwaz_rifat_data_raw.csv"
clean_path = "/opt/airflow/dags/P2M3_fawwaz_rifat_data_clean.csv"
es_host = "host.docker.internal"

# Fungsi untuk mengambil data dari PostgreSQL
def get_data_from_postgresql():
    '''
    Fungsi ini ditujukan untuk mengambil/extract data dari 
    PostgreSQL sebelum dilakukan data cleaning
    
    Parameter:
    host : string - nama container postgresql
    database: string - nama database tempat data disimpan
    user : string - username postgresql
    password : string - password postgresql
    port = integer - port

    Return
    raw_path : list of str - daftar data yang ada di database
    '''
    # Membuat koneksi ke postgresql
    conn = psycopg2.connect(
        host="postgres", # Nama container di docker
        database="airflow", # Nama database di postgresql
        user="airflow", # Username server postgresql
        password="airflow", # Password server postgresql
        port=5432 # Port koneksi ke postgresql
    )
    
    query = "SELECT * FROM table_m3;" # Query untuk mengambil semua data dari table_m3
    df = pd.read_sql_query(query, conn) # Membuat dataframe dari data yang diambil
    conn.close() # Menutup koneksi postgresql
    
    df.to_csv(raw_path, index=False) # Mengubah format data menjadi CSV
    return raw_path  # Mengembalikan raw_path 

# Fungsi untuk cleaning data
def data_clean():
    '''
    Fungsi ini ditujukan untuk membersihkan data yang telah 
    diambil dari Postgresql 
    
    Step by step :
    1. Load data bersih (clean data)
    2. Lowerisasi dan mengganti spasi dengan underscore (_) 
       pada nama kolom
    3. Menyimpan data bersih dalam format CSV   

    Return
    clean_path : data yang telah dibersihkan
    '''
    df = pd.read_csv(raw_path) # Load data CSV
    df.columns = df.columns.str.lower().str.replace(" ", "_") # Lowerisasi nama kolom dan mengganti spasi dengan underscore (_)
    df.to_csv(clean_path, index=False) # Mengubah format data bersih dan menyimpannya kedalam clean_path
    return clean_path  # Mengembalikan clean path

# Fungsi untuk memuat data ke Elasticsearch
def data_load():
    '''
    Fungsi ini ditujukan untuk load data yang telah 
    dibersihkan ke elasticsearch dan dapat diakses
    oleh Kibana.  

    Return
    clean_path : data yang telah dibersihkan
    '''
    es = Elasticsearch("http://elasticsearch:9200") # Menyesuaaikan akses port dari program
    df = pd.read_csv(clean_path) # Membuat variabel dataframe

    for i, r in df.iterrows(): # Fungsi iterasi
        doc= r.to_json() # Mengubah ke JSON
        res=es.index(index="m3_clean_data3", body=doc) # Upload ke elasticsearch
        print(res) # Print


# Default arguments untuk DAG
default_args = {
    'owner': 'fawwaz_r', 
    'start_date': dt.datetime(2024, 11, 1) - timedelta(hours=7),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
}

# Definisi DAG
with DAG('m3_cleaned_data_af_3',
         default_args=default_args,
         schedule_interval='10-30/10 9 * * 6',
         catchup=False) as dag: # catchup False  = tidak mengejar ketertinggalan dari start_date

    # Task 1 : Menampilkan pesan di terminal
    print_starting = BashOperator(
        task_id='starting',
        bash_command='echo "I am reading the CSV now....."'
    )

    # Task 2 : Mengambil data dari PostgreSQL
    extract_task = PythonOperator(
        task_id='extract_from_postgresql',
        python_callable=get_data_from_postgresql
    )

    # Task 3 : Membersihkan data
    clean_task = PythonOperator(
        task_id='clean_data',
        python_callable=data_clean
    )

    # Task 4 : Load data ke Elasticsearch
    load_task = PythonOperator(
        task_id='load_to_elasticsearch',
        python_callable=data_load
    )

# Menentukan urutan eksekusi tugas
print_starting >> extract_task >> clean_task >> load_task