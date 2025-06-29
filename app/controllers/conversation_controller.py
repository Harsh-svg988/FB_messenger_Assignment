from fastapi import HTTPException, status

from app.schemas.conversation import ConversationResponse, PaginatedConversationResponse
from app.models.cassandra_models import ConversationModel


class ConversationController:
    """
    Controller for handling conversation operations
    """

    async def get_user_conversations(
        self, user_id: int, page: int = 1, limit: int = 20
    ) -> PaginatedConversationResponse:
        """
        Get all conversations for a user with pagination

        Args:
            user_id: ID of the user
            page: Page number
            limit: Number of conversations per page

        Returns:
            Paginated list of conversations

        Raises:
            HTTPException: If user not found or access denied
        """
        try:
            result = await ConversationModel.get_user_conversations(
                user_id=user_id, page=page, limit=limit
            )

            # Transform data to match the expected schema
            conversations = []
            for conv in result["data"]:
                conversations.append(
                    ConversationResponse(
                        id=conv["conversation_id"],
                        user1_id=user_id,
                        user2_id=conv["other_user_id"],
                        last_message_at=conv["last_message_at"],
                        last_message_content=conv["last_message_content"],
                    )
                )

            return PaginatedConversationResponse(
                total=result["total"],
                page=result["page"],
                limit=result["limit"],
                data=conversations,
            )
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to get user conversations: {str(e)}",
            )

    async def get_conversation(self, conversation_id: int) -> ConversationResponse:
        """
        Get a specific conversation by ID

        Args:
            conversation_id: ID of the conversation

        Returns:
            Conversation details

        Raises:
            HTTPException: If conversation not found or access denied
        """
        try:
            conversation = await ConversationModel.get_conversation(
                conversation_id=conversation_id
            )

            if not conversation:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Conversation with ID {conversation_id} not found",
                )

            return ConversationResponse(
                id=conversation["id"],
                user1_id=conversation["user1_id"],
                user2_id=conversation["user2_id"],
                last_message_at=conversation["last_message_at"],
                last_message_content=conversation["last_message_content"],
            )
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to get conversation: {str(e)}",
            )     
