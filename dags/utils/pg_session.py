import os
import psycopg2
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path('/opt/airflow/.env'))

def get_postgres_conn():
    conn = psycopg2.connect(
        host='postgres',
        dbname='airflow',
        user='airflow',
        password='airflow',
        port=5432
    )
    return conn