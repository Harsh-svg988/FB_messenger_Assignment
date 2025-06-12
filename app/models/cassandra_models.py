# Updated MessageModel and ConversationModel
import uuid
from datetime import datetime
from typing import List, Dict, Any, Optional

from app.db.cassandra_client import cassandra_client


class MessageModel:
    @staticmethod
    async def create_message(
        sender_id: int,
        receiver_id: int,
        content: str,
        conversation_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        session = cassandra_client.get_session()

        if conversation_id is None:
            conversation_id = await ConversationModel.create_or_get_conversation(
                sender_id, receiver_id
            )

        message_id = int(uuid.uuid4().int % (2**31 - 1))
        timestamp = datetime.utcnow()

        # Insert message into messages_by_conversation
        query = """
            INSERT INTO messages_by_conversation 
            (conversation_id, message_id, sender_id, receiver_id, content, created_at) 
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        session.execute(query, (conversation_id, message_id, sender_id, receiver_id, content, timestamp))

        # Update conversations_by_user for both users
        convo_query = """
            INSERT INTO conversations_by_user 
            (user_id, conversation_id, other_user_id, last_message_at, last_message_content) 
            VALUES (%s, %s, %s, %s, %s)
        """
        session.execute(convo_query, (sender_id, conversation_id, receiver_id, timestamp, content))
        session.execute(convo_query, (receiver_id, conversation_id, sender_id, timestamp, content))

        return {
            "id": message_id,
            "conversation_id": conversation_id,
            "sender_id": sender_id,
            "receiver_id": receiver_id,
            "content": content,
            "created_at": timestamp,
        }

    @staticmethod
    async def get_conversation_messages(conversation_id: int, page: int = 1, limit: int = 20) -> Dict[str, Any]:
        query = """
            SELECT conversation_id, message_id, sender_id, receiver_id, content, created_at 
            FROM messages_by_conversation 
            WHERE conversation_id = %s 
            LIMIT %s
        """
        result = cassandra_client.execute(query, (conversation_id, limit))

        # Get total count
        count_query = """
            SELECT COUNT(*) as total
            FROM messages_by_conversation 
            WHERE conversation_id = %s
        """
        total_result = cassandra_client.execute(count_query, (conversation_id,))
        total = total_result[0]["total"] if total_result else 0

        return {
            "total": total,
            "page": page,
            "limit": limit,
            "data": result
        }

    @staticmethod
    async def get_messages_before_timestamp(conversation_id: int, before_timestamp: datetime, page: int = 1, limit: int = 20) -> Dict[str, Any]:
        query = """
            SELECT conversation_id, message_id, sender_id, receiver_id, content, created_at 
            FROM messages_by_conversation 
            WHERE conversation_id = %s AND created_at < %s 
            LIMIT %s
        """
        result = cassandra_client.execute(query, (conversation_id, before_timestamp, limit))

        # Get total count
        count_query = """
            SELECT COUNT(*) as total
            FROM messages_by_conversation 
            WHERE conversation_id = %s AND created_at < %s
        """
        total_result = cassandra_client.execute(count_query, (conversation_id, before_timestamp))
        total = total_result[0]["total"] if total_result else 0

        return {
            "total": total,
            "page": page,
            "limit": limit,
            "data": result
        }


class ConversationModel:
    @staticmethod
    async def get_user_conversations(user_id: int, page: int = 1, limit: int = 20) -> Dict[str, Any]:
        query = """
            SELECT user_id, conversation_id, other_user_id, last_message_at, last_message_content 
            FROM conversations_by_user 
            WHERE user_id = %s 
            LIMIT %s
        """
        result = cassandra_client.execute(query, (user_id, limit))

        # Get total count
        count_query = """
            SELECT COUNT(*) as total
            FROM conversations_by_user 
            WHERE user_id = %s
        """
        total_result = cassandra_client.execute(count_query, (user_id,))
        total = total_result[0]["total"] if total_result else 0

        return {
            "total": total,
            "page": page,
            "limit": limit,
            "data": result
        }

    @staticmethod
    async def get_conversation(conversation_id: int) -> Optional[Dict[str, Any]]:
        # Get the first message in the conversation to get user IDs
        query = """
            SELECT conversation_id, sender_id, receiver_id, content, created_at
            FROM messages_by_conversation 
            WHERE conversation_id = %s 
            LIMIT 1
        """
        result = cassandra_client.execute(query, (conversation_id,))

        if not result:
            return None

        msg = result[0]
        return {
            "id": conversation_id,
            "user1_id": msg["sender_id"],
            "user2_id": msg["receiver_id"],
            "last_message_at": msg["created_at"],
            "last_message_content": msg["content"],
        }

    @staticmethod
    async def create_or_get_conversation(user1_id: int, user2_id: int) -> int:
        # Ensure consistent ordering of user IDs
        if user1_id > user2_id:
            user1_id, user2_id = user2_id, user1_id

        # Check if conversation exists
        query = """
            SELECT conversation_id
            FROM conversation_lookup 
            WHERE user1_id = %s AND user2_id = %s
        """
        result = cassandra_client.execute(query, (user1_id, user2_id))

        if result:
            return result[0]["conversation_id"]

        # Create new conversation
        conversation_id = int(uuid.uuid4().int % (2**31 - 1))

        insert_query = """
            INSERT INTO conversation_lookup (user1_id, user2_id, conversation_id) 
            VALUES (%s, %s, %s)
        """
        cassandra_client.execute(insert_query, (user1_id, user2_id, conversation_id))

        return conversation_id