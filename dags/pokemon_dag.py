from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from utils.pokemon_fetcher import fetch_pokemon
from scripts.stream_to_kafka import stream
from scripts.kafka_to_cassandra_db import kafka_consumer
from airflow.utils.timezone import make_aware
from pytz import timezone

ist_tz = timezone('Asia/Kolkata')

default_args = {
    'owner': 'airflow',
    'start_date': make_aware(datetime.now() + timedelta(minutes=30), ist_tz),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id='pokemon_dag',
    default_args=default_args,
    schedule_interval='*/10 * * * *',
    catchup=False,
    max_active_runs=1
) as dag:

    fetch_pokemon_task = PythonOperator(
        task_id='fetch_pokemon',
        python_callable=fetch_pokemon,
        dag=dag
    )

    stream_task = PythonOperator(
        task_id='stream',
        python_callable=stream,
        dag=dag
    )

    kafka_consumer_task = PythonOperator(
        task_id='kafka_consumer',
        python_callable=kafka_consumer,
        dag=dag
    )

    fetch_pokemon_task >> stream_task >> kafka_consumer_task