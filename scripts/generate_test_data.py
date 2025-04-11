"""
Script to generate test data for the Messenger application.
"""
import os
import uuid
import logging
import random
from datetime import datetime, timedelta
from cassandra.cluster import Cluster

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Cassandra connection settings
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "messenger")

# Test data configuration
NUM_USERS = 10
NUM_CONVERSATIONS = 15
MAX_MESSAGES_PER_CONVERSATION = 50

def connect_to_cassandra():
    logger.info("Connecting to Cassandra...")
    try:
        cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
        session = cluster.connect(CASSANDRA_KEYSPACE)
        logger.info("Connected to Cassandra.")
        return cluster, session
    except Exception as e:
        logger.error(f"Connection failed: {e}")
        raise

def generate_test_data(session):
    logger.info("Generating test data...")

    user_ids = [uuid.uuid4() for _ in range(NUM_USERS)]
    for user_id in user_ids:
        username = f"user_{str(user_id)[:8]}"
        session.execute(
            "INSERT INTO users (user_id, username) VALUES (%s, %s)",
            (user_id, username)
        )
    logger.info(f"Inserted {NUM_USERS} users.")

    conversations = set()
    while len(conversations) < NUM_CONVERSATIONS:
        u1, u2 = random.sample(user_ids, 2)
        if (u1, u2) not in conversations and (u2, u1) not in conversations:
            conversations.add((u1, u2))

    for p1, p2 in conversations:
        conversation_id = uuid.uuid4()

        session.execute(
            "INSERT INTO conversations (conversation_id, participant1, participant2) VALUES (%s, %s, %s)",
            (conversation_id, p1, p2)
        )

        num_messages = random.randint(10, MAX_MESSAGES_PER_CONVERSATION)
        base_time = datetime.utcnow()

        for i in range(num_messages):
            sender_id = random.choice([p1, p2])
            msg_time = base_time + timedelta(seconds=i * 10)
            message_id = uuid.uuid1()  # time-based UUID
            message = f"Message {i + 1} in conversation {str(conversation_id)[:8]}"

            session.execute(
                """
                INSERT INTO messages (conversation_id, message_id, sender_id, message, timestamp)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (conversation_id, message_id, sender_id, message, msg_time)
            )

    logger.info(f"Inserted {NUM_CONVERSATIONS} conversations with messages.")
    logger.info("Test data generation complete.")

def main():
    cluster = None
    try:
        cluster, session = connect_to_cassandra()
        generate_test_data(session)
    except Exception as e:
        logger.error(f"Error during test data generation: {e}")
    finally:
        if cluster:
            cluster.shutdown()
            logger.info("Cassandra connection closed.")

if __name__ == "__main__":
    main()
