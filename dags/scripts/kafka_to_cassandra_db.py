import requests
import uuid
from kafka import KafkaConsumer
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.auth import PlainTextAuthProvider
from dotenv import load_dotenv
from pathlib import Path
import json
import os
import logging
import time

# Load environment variables
load_dotenv(dotenv_path=Path('/opt/airflow/.env'))

# Constants
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')
KAFKA_BOOTSTRAP_SERVER = os.getenv('KAFKA_BROKER')

CASSANDRA_KEYSPACE = os.getenv('CASSANDRA_KEYSPACE')
CASSANDRA_HOST = os.getenv('CASSANDRA_HOST')
CASSANDRA_PORT = int(os.getenv('CASSANDRA_PORT'))
CASSANDRA_USER = os.getenv('CASSANDRA_USER')
CASSANDRA_PASS = os.getenv('CASSANDRA_PASSWORD')

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def setup_cassandra_connection():
    """Establish Cassandra connection with retry logic"""
    auth_provider = None
    if CASSANDRA_USER and CASSANDRA_PASS:
        auth_provider = PlainTextAuthProvider(
            username=CASSANDRA_USER,
            password=CASSANDRA_PASS
        )
    logger.info("Attempting to connect to Cassandra with...")
    cluster = Cluster(
        [CASSANDRA_HOST],
        port=CASSANDRA_PORT,
        load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='datacenter1'),
        protocol_version=4,
        connect_timeout=30,
        auth_provider=auth_provider
    )

    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            session = cluster.connect()
            logger.info("Successfully connected to Cassandra")
            return cluster, session
        except Exception as e:
            retry_count += 1
            logger.warning(f"Connection attempt {retry_count} failed: {e}")
            if retry_count == max_retries:
                logger.error("Max retries reached for Cassandra connection")
                raise
            time.sleep(5)

def fetch_pokemon_details(url):
    """Fetch Pokémon details from PokeAPI"""
    try:
        logger.info(f"Fetching Pokémon details from {url}")
        response = requests.get(url, timeout=20)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed for {url}: {e}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON response from {url}: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error fetching {url}: {e}")
        return None

def validate_pokemon_data(json_data):
    """Validate required fields in Pokémon data"""
    required_fields = ['name', 'url', 'weight', 'height']
    for field in required_fields:
        if field not in json_data:
            logger.error(f"Missing required field: {field}")
            return False
        
    try:
        # Convert to int and validate
        weight = int(json_data.get('weight', 0))
        height = int(json_data.get('height', 0))
        
        if weight <= 0 or height <= 0:
            logger.error(f"Invalid weight/height values: weight={weight}, height={height}")
            return False
            
        return True
    except (ValueError, TypeError) as e:
        logger.error(f"Invalid numeric values in weight/height: {e}")
        return False

def save_pokemon_to_cassandra(session, json_data):
    """Save Pokémon data to Cassandra"""
    try:
        if not validate_pokemon_data(json_data):
            return False

        logger.info(f"Processing Pokémon data: {json_data['name']}")
        
        user_id = uuid.uuid4()
        weight = int(json_data.get('weight', 0))
        height = int(json_data.get('height', 0))
        
        logger.info(f"Converted values - weight: {weight}, height: {height}")

        insert_query = """
            INSERT INTO pokemon_detail (
                id, name, url, weight, height
            ) VALUES (%s, %s, %s, %s, %s)
        """
        values = (user_id, json_data['name'], json_data['url'], weight, height)
        
        session.execute(insert_query, values)
        logger.info(f"Successfully inserted Pokémon: {json_data['name']}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to save Pokémon to Cassandra: {e}", exc_info=True)
        return False

def kafka_consumer():
    """Consume messages from Kafka and process them"""
    consumer = None
    cluster, session = None, None
    
    try:
        # Initialize Cassandra connection
        cluster, session = setup_cassandra_connection()
        session.set_keyspace(CASSANDRA_KEYSPACE)

        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
            auto_offset_reset='earliest',
            group_id='pokemon_group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            consumer_timeout_ms=10000
        )

        for message in consumer:
            try:
                data = message.value
                logger.info(f"Consumed message: {data}")
                
                pokemon_url = data.get("p_url")
                if not pokemon_url:
                    logger.error("Missing Pokémon URL in message")
                    continue
                    
                pokemon_details = fetch_pokemon_details(pokemon_url)
                if not pokemon_details:
                    logger.error(f"Failed to fetch details for {data.get('p_name', 'unknown')}")
                    continue
                    
                # Add URL to details if not present
                pokemon_details['url'] = pokemon_url
                
                if not save_pokemon_to_cassandra(session, pokemon_details):
                    logger.error(f"Failed to save Pokémon: {pokemon_details.get('name', 'unknown')}")

            except Exception as e:
                logger.error(f"Error processing message: {e}", exc_info=True)
                continue

    except Exception as e:
        logger.error(f"Kafka consumer error: {e}", exc_info=True)
    finally:
        try:
            if consumer is not None:
                consumer.close()
        except Exception as e:
            logger.error(f"Error closing consumer: {e}")
        
        try:
            if cluster is not None:
                cluster.shutdown()
        except Exception as e:
            logger.error(f"Error shutting down Cassandra cluster: {e}")
        
        logger.info("Cleanup complete")
