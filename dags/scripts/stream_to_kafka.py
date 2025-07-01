import os
import json
from kafka import KafkaProducer
from pathlib import Path
from dotenv import load_dotenv

from utils.pg_session import get_postgres_conn

load_dotenv(dotenv_path=Path('/opt/airflow/.env'))

KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')
KALFKA_BROKER = os.getenv('KAFKA_BROKER')

LAST_SEEN_FILE = os.getenv('LAST_SEEN_FILE')

COLUMNS = ['id', 'p_name', 'p_url']

def get_last_seen_id():
    try:
        with open(LAST_SEEN_FILE, 'r') as f:
            return int(f.read().strip())
    except FileNotFoundError:
        return 0

def save_last_seen_id(last_id):
    with open(LAST_SEEN_FILE, 'w') as f:
        f.write(str(last_id))

def fetch_new_rows(last_id):
    conn = get_postgres_conn()
    cursor = conn.cursor()
    cursor.execute(f"SELECT id, p_name, p_url FROM pokemon_character WHERE id > {last_id} ORDER BY id ASC")
    rows = cursor.fetchall()
    conn.close()
    return [dict(zip(COLUMNS, row)) for row in rows]


def send_to_kafka(rows):
    producer = KafkaProducer(
        bootstrap_servers=KALFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    for row in rows:
        producer.send(KAFKA_TOPIC, row)
    producer.flush()


def stream():
    last_seen_id = get_last_seen_id()
    new_rows = fetch_new_rows(last_seen_id)
    if new_rows:
        send_to_kafka(new_rows)
        save_last_seen_id(new_rows[-1]['id'])

