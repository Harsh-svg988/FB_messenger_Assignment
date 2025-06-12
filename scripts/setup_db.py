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
    
    # Drop existing tables
    session.execute("DROP TABLE IF EXISTS messages_by_conversation;")
    session.execute("DROP TABLE IF EXISTS conversations_by_user;")
    session.execute("DROP TABLE IF EXISTS conversation_lookup;")

    # Create messages_by_conversation table
    # Partition key: conversation_id
    # Clustering columns: created_at DESC, message_id
    session.execute("""
        CREATE TABLE IF NOT EXISTS messages_by_conversation (
            conversation_id INT,
            message_id INT,
            sender_id INT,
            receiver_id INT,
            content TEXT,
            created_at TIMESTAMP,
            PRIMARY KEY ((conversation_id), created_at, message_id)
        ) WITH CLUSTERING ORDER BY (created_at DESC, message_id ASC);
    """)

    # Create conversations_by_user table
    # Partition key: user_id
    # Clustering columns: last_message_at DESC, conversation_id
    session.execute("""
        CREATE TABLE IF NOT EXISTS conversations_by_user (
            user_id INT,
            conversation_id INT,
            other_user_id INT,
            last_message_at TIMESTAMP,
            last_message_content TEXT,
            PRIMARY KEY ((user_id), last_message_at, conversation_id)
        ) WITH CLUSTERING ORDER BY (last_message_at DESC, conversation_id ASC);
    """)

    # Create conversation_lookup table
    # Partition key: user1_id, user2_id (ordered)
    # Clustering columns: none
    session.execute("""
        CREATE TABLE IF NOT EXISTS conversation_lookup (
            user1_id INT,
            user2_id INT,
            conversation_id INT,
            PRIMARY KEY ((user1_id, user2_id))
        );
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
