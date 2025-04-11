"""
Script to initialize Cassandra keyspace and tables for the Messenger application.
"""
import os
import time
import logging
from cassandra.cluster import Cluster

# Logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Cassandra connection settings
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "messenger")

def wait_for_cassandra():
    logger.info("Waiting for Cassandra to be ready...")
    cluster = None
    for attempt in range(10):
        try:
            cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
            session = cluster.connect()
            logger.info("Cassandra is ready!")
            return cluster
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1}/10 - Cassandra not ready: {e}")
            time.sleep(5)
    raise Exception("Could not connect to Cassandra after multiple attempts.")

def create_keyspace(session):
    logger.info(f"Creating keyspace '{CASSANDRA_KEYSPACE}'...")
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE}
        WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': '1' }};
    """)
    logger.info(f"Keyspace '{CASSANDRA_KEYSPACE}' created.")

def create_tables(session):
    logger.info("Creating tables...")
    
    session.execute("DROP TABLE IF EXISTS conversations;")


    session.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id UUID PRIMARY KEY,
            username TEXT
        );
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS conversations (
            conversation_id UUID PRIMARY KEY,
            participant1 UUID,
            participant2 UUID
        );
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            conversation_id UUID,
            message_id TIMEUUID,
            sender_id UUID,
            message TEXT,
            timestamp TIMESTAMP,
            PRIMARY KEY ((conversation_id), timestamp, message_id)
        ) WITH CLUSTERING ORDER BY (timestamp ASC, message_id ASC);
    """)

    logger.info("Tables created successfully.")

def main():
    logger.info("Starting Cassandra initialization...")
    cluster = wait_for_cassandra()
    try:
        session = cluster.connect()
        create_keyspace(session)
        session.set_keyspace(CASSANDRA_KEYSPACE)
        create_tables(session)
        logger.info("Cassandra initialization completed.")
    except Exception as e:
        logger.error(f"Error during setup: {e}")
        raise
    finally:
        cluster.shutdown()

if __name__ == "__main__":
    main()
