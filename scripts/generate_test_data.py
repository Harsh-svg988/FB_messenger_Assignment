"""
Script to generate test data for the Messenger application.
"""

import os
import logging
import random
from datetime import datetime, timedelta
from cassandra.cluster import Cluster

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Cassandra connection settings
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "messenger")

# Test data configuration
NUM_USERS = 10  # Total number of users to simulate
NUM_CONVERSATIONS = 15  # Total conversations to generate
MAX_MESSAGES_PER_CONVERSATION = 50  # Maximum number of messages in a conversation

def connect_to_cassandra():
    """
    Connect to Cassandra cluster and return the session.
    """
    logger.info("Connecting to Cassandra cluster...")
    try:
        cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
        session = cluster.connect(CASSANDRA_KEYSPACE)
        logger.info("Successfully connected to Cassandra.")
        return cluster, session
    except Exception as e:
        logger.error(f"Failed to connect to Cassandra: {e}")
        raise

def generate_test_data(session):
    """
    Generate conversations and messages for testing.
    This simulates realistic messaging data to test APIs.
    """
    logger.info("Starting test data generation...")

    # Step 1: Create user IDs
    user_ids = list(range(1, NUM_USERS + 1))
    logger.info(f"Generated {NUM_USERS} user IDs.")

    # Step 2: Create unique user pairs for conversations
    conversations = set()
    while len(conversations) < NUM_CONVERSATIONS:
        u1, u2 = random.sample(user_ids, 2)
        if (u1, u2) not in conversations and (u2, u1) not in conversations:
            conversations.add((u1, u2))

    # Sample conversation topics
    topics = [
        "weekend plans", "project deadline", "lunch tomorrow", "movie recommendations",
        "vacation ideas", "new tech gadget", "birthday party", "concert tickets",
        "book club", "gym schedule"
    ]

    # Step 3: Insert conversations and generate messages
    for p1, p2 in conversations:
        # Ensure consistent ordering of user IDs
        if p1 > p2:
            p1, p2 = p2, p1

        # Create conversation in lookup table
        conversation_id = random.randint(1, 2**31 - 1)
        session.execute(
            "INSERT INTO conversation_lookup (user1_id, user2_id, conversation_id) VALUES (%s, %s, %s)",
            (p1, p2, conversation_id)
        )

        topic = random.choice(topics)
        num_messages = random.randint(10, MAX_MESSAGES_PER_CONVERSATION)
        base_time = datetime.utcnow() - timedelta(days=30)

        for i in range(num_messages):
            sender_id = random.choice([p1, p2])
            receiver_id = p2 if sender_id == p1 else p1
            message_id = random.randint(1, 2**31 - 1)
            timestamp = base_time + timedelta(seconds=i * 10)
            content = (
                f"Hey, do you have any thoughts about {topic}?"
                if i == 0 else
                random.choice([
                    f"I was thinking about {topic} yesterday.",
                    f"What do you think about the latest on {topic}?",
                    f"I'm really excited about {topic}!",
                    f"Can we discuss {topic} later?",
                    f"Let's plan something around {topic}.",
                    "Sure, sounds good!",
                    "I'll check and get back to you.",
                    "Thanks for the update!",
                    "Yes, that works.",
                    "No worries, let's reschedule.",
                ])
            )

            # Insert message
            session.execute(
                """
                INSERT INTO messages_by_conversation 
                (conversation_id, message_id, sender_id, receiver_id, content, created_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (conversation_id, message_id, sender_id, receiver_id, content, timestamp)
            )

            # Update conversations_by_user for both users
            session.execute(
                """
                INSERT INTO conversations_by_user
                (user_id, conversation_id, other_user_id, last_message_at, last_message_content)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (sender_id, conversation_id, receiver_id, timestamp, content)
            )
            session.execute(
                """
                INSERT INTO conversations_by_user
                (user_id, conversation_id, other_user_id, last_message_at, last_message_content)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (receiver_id, conversation_id, sender_id, timestamp, content)
            )

            # Print message details
            print(f"Message ID: {message_id}, Sender ID: {sender_id}, Receiver ID: {receiver_id}, Message: {content}")

    logger.info(f"{NUM_CONVERSATIONS} conversations generated with random messages.")
    logger.info("Test data generation completed successfully.")


def main():
    """
    Main execution flow for data generation script.
    """
    cluster = None
    try:
        cluster, session = connect_to_cassandra()
        generate_test_data(session)
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        if cluster:
            cluster.shutdown()
            logger.info("Cassandra connection closed.")

if __name__ == "__main__":
    main()
