"""
Unit tests for management-api endpoints and background processing.

Tests verify:
- Health check endpoint
- Purchase retrieval endpoint with pagination
- Lifespan management (MongoDB, Kafka consumer, DLQ producer)
- Background consumer message processing
- Retry logic with exponential backoff
- Dead Letter Queue (DLQ) handling
- MongoDB index creation
- Edge cases and error handling

Run tests with: pytest test_app.py -v
Coverage: pytest test_app.py --cov=app --cov-report=html
"""

import sys
import json
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch, call
from datetime import datetime

import pytest
from fastapi.testclient import TestClient

# Mock dependencies before importing app
sys.modules['aiokafka'] = MagicMock()
sys.modules['motor'] = MagicMock()
sys.modules['motor.motor_asyncio'] = MagicMock()

# Now import the app
from app import app, consume_messages, Purchase, PurchasesResponse  # noqa: E402


# Test client
client = TestClient(app)


# ============================================================================
# Health and Basic Endpoint Tests (Existing)
# ============================================================================

def test_health_endpoint():
    """Test health check endpoint returns 200 OK."""
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "healthy", "service": "management-api"}


@patch('app.mongo_client')
def test_get_purchases_empty(mock_mongo):
    """Test retrieving purchases for user with no purchases."""
    # Mock MongoDB collection - collection itself is sync, not async
    mock_collection = MagicMock()
    mock_cursor = MagicMock()  # Cursor methods are sync, only to_list is async
    mock_cursor.to_list = AsyncMock(return_value=[])

    mock_collection.find.return_value = mock_cursor
    mock_cursor.sort.return_value = mock_cursor
    mock_cursor.skip.return_value = mock_cursor
    mock_cursor.limit.return_value = mock_cursor

    mock_db = MagicMock()
    mock_db.purchases = mock_collection
    mock_mongo.__getitem__.return_value = mock_db

    response = client.get("/purchases/user123")

    assert response.status_code == 200
    data = response.json()
    assert data["user_id"] == "user123"
    assert data["count"] == 0
    assert data["purchases"] == []


@patch('app.mongo_client')
def test_get_purchases_with_data(mock_mongo):
    """Test retrieving purchases for user with purchases."""
    # Mock MongoDB collection - collection itself is sync, not async
    mock_purchases = [
        {
            "username": "alice",
            "user_id": "user123",
            "item_name": "Magic Potion",
            "price": 9.99,
            "timestamp": "2024-01-01T12:00:00"
        },
        {
            "username": "alice",
            "user_id": "user123",
            "item_name": "Health Potion",
            "price": 5.99,
            "timestamp": "2024-01-01T13:00:00"
        }
    ]

    mock_collection = MagicMock()
    mock_cursor = MagicMock()  # Cursor methods are sync, only to_list is async
    mock_cursor.to_list = AsyncMock(return_value=mock_purchases)

    mock_collection.find.return_value = mock_cursor
    mock_cursor.sort.return_value = mock_cursor
    mock_cursor.skip.return_value = mock_cursor
    mock_cursor.limit.return_value = mock_cursor

    mock_db = MagicMock()
    mock_db.purchases = mock_collection
    mock_mongo.__getitem__.return_value = mock_db

    response = client.get("/purchases/user123")

    assert response.status_code == 200
    data = response.json()
    assert data["user_id"] == "user123"
    assert data["count"] == 2
    assert len(data["purchases"]) == 2


@patch('app.mongo_client')
def test_get_purchases_with_pagination(mock_mongo):
    """Test purchases retrieval with pagination parameters."""
    mock_collection = MagicMock()  # Collection is sync, not async
    mock_cursor = MagicMock()  # Cursor methods are sync, only to_list is async
    mock_cursor.to_list = AsyncMock(return_value=[])

    mock_collection.find.return_value = mock_cursor
    mock_cursor.sort.return_value = mock_cursor
    mock_cursor.skip.return_value = mock_cursor
    mock_cursor.limit.return_value = mock_cursor

    mock_db = MagicMock()
    mock_db.purchases = mock_collection
    mock_mongo.__getitem__.return_value = mock_db

    response = client.get("/purchases/user123?limit=50&offset=10")

    assert response.status_code == 200
    # Verify pagination parameters were applied
    mock_cursor.skip.assert_called_with(10)
    mock_cursor.limit.assert_called_with(50)


@patch('app.mongo_client')
def test_get_purchases_limit_cap(mock_mongo):
    """Test that limit is capped at maximum value."""
    mock_collection = MagicMock()  # Collection is sync, not async
    mock_cursor = MagicMock()  # Cursor methods are sync, only to_list is async
    mock_cursor.to_list = AsyncMock(return_value=[])

    mock_collection.find.return_value = mock_cursor
    mock_cursor.sort.return_value = mock_cursor
    mock_cursor.skip.return_value = mock_cursor
    mock_cursor.limit.return_value = mock_cursor

    mock_db = MagicMock()
    mock_db.purchases = mock_collection
    mock_mongo.__getitem__.return_value = mock_db

    # Request more than max (1000)
    response = client.get("/purchases/user123?limit=5000")

    assert response.status_code == 200
    # Verify limit was capped at 1000
    mock_cursor.limit.assert_called_with(1000)


# ============================================================================
# Purchase Retrieval Extended Tests
# ============================================================================

@patch('app.mongo_client')
def test_get_purchases_negative_limit(mock_mongo):
    """Test that negative limit is converted to minimum value (1)."""
    mock_collection = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.to_list = AsyncMock(return_value=[])

    mock_collection.find.return_value = mock_cursor
    mock_cursor.sort.return_value = mock_cursor
    mock_cursor.skip.return_value = mock_cursor
    mock_cursor.limit.return_value = mock_cursor

    mock_db = MagicMock()
    mock_db.purchases = mock_collection
    mock_mongo.__getitem__.return_value = mock_db

    response = client.get("/purchases/user123?limit=-10")

    assert response.status_code == 200
    # Verify limit was set to minimum (1)
    mock_cursor.limit.assert_called_with(1)


@patch('app.mongo_client')
def test_get_purchases_zero_limit(mock_mongo):
    """Test that zero limit is converted to minimum value (1)."""
    mock_collection = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.to_list = AsyncMock(return_value=[])

    mock_collection.find.return_value = mock_cursor
    mock_cursor.sort.return_value = mock_cursor
    mock_cursor.skip.return_value = mock_cursor
    mock_cursor.limit.return_value = mock_cursor

    mock_db = MagicMock()
    mock_db.purchases = mock_collection
    mock_mongo.__getitem__.return_value = mock_db

    response = client.get("/purchases/user123?limit=0")

    assert response.status_code == 200
    # Verify limit was set to minimum (1)
    mock_cursor.limit.assert_called_with(1)


@patch('app.mongo_client')
def test_get_purchases_negative_offset(mock_mongo):
    """Test that negative offset is converted to zero."""
    mock_collection = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.to_list = AsyncMock(return_value=[])

    mock_collection.find.return_value = mock_cursor
    mock_cursor.sort.return_value = mock_cursor
    mock_cursor.skip.return_value = mock_cursor
    mock_cursor.limit.return_value = mock_cursor

    mock_db = MagicMock()
    mock_db.purchases = mock_collection
    mock_mongo.__getitem__.return_value = mock_db

    response = client.get("/purchases/user123?offset=-5")

    assert response.status_code == 200
    # Verify offset was set to 0
    mock_cursor.skip.assert_called_with(0)


@patch('app.mongo_client')
def test_get_purchases_query_structure(mock_mongo):
    """Test that MongoDB query is structured correctly."""
    mock_collection = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.to_list = AsyncMock(return_value=[])

    mock_collection.find.return_value = mock_cursor
    mock_cursor.sort.return_value = mock_cursor
    mock_cursor.skip.return_value = mock_cursor
    mock_cursor.limit.return_value = mock_cursor

    mock_db = MagicMock()
    mock_db.purchases = mock_collection
    mock_mongo.__getitem__.return_value = mock_db

    response = client.get("/purchases/user456")

    assert response.status_code == 200
    # Verify query filter and projection
    mock_collection.find.assert_called_once_with(
        {"user_id": "user456"},
        {"_id": 0}
    )
    # Verify sorting by timestamp descending
    mock_cursor.sort.assert_called_once_with("timestamp", -1)


@patch('app.mongo_client')
def test_get_purchases_mongodb_error(mock_mongo):
    """Test handling of MongoDB query failures."""
    mock_collection = MagicMock()
    mock_cursor = MagicMock()
    # Simulate MongoDB error
    mock_cursor.to_list = AsyncMock(side_effect=Exception("MongoDB connection error"))

    mock_collection.find.return_value = mock_cursor
    mock_cursor.sort.return_value = mock_cursor
    mock_cursor.skip.return_value = mock_cursor
    mock_cursor.limit.return_value = mock_cursor

    mock_db = MagicMock()
    mock_db.purchases = mock_collection
    mock_mongo.__getitem__.return_value = mock_db

    # Should raise an exception
    with pytest.raises(Exception) as exc_info:
        client.get("/purchases/user123")

    assert "MongoDB connection error" in str(exc_info.value)


@patch('app.mongo_client')
def test_get_purchases_boundary_pagination(mock_mongo):
    """Test pagination at boundary conditions."""
    mock_collection = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.to_list = AsyncMock(return_value=[])

    mock_collection.find.return_value = mock_cursor
    mock_cursor.sort.return_value = mock_cursor
    mock_cursor.skip.return_value = mock_cursor
    mock_cursor.limit.return_value = mock_cursor

    mock_db = MagicMock()
    mock_db.purchases = mock_collection
    mock_mongo.__getitem__.return_value = mock_db

    # Test with large offset
    response = client.get("/purchases/user123?limit=100&offset=999999")

    assert response.status_code == 200
    mock_cursor.skip.assert_called_with(999999)
    mock_cursor.limit.assert_called_with(100)


# ============================================================================
# Lifespan Management Tests
# ============================================================================

@pytest.mark.asyncio
@patch('app.consume_messages')
@patch('app.AIOKafkaConsumer')
@patch('app.AIOKafkaProducer')
@patch('app.AsyncIOMotorClient')
async def test_lifespan_successful_initialization(mock_motor, mock_producer_class, mock_consumer_class, mock_consume):
    """Test successful initialization of MongoDB, Kafka consumer, and DLQ producer."""
    # Mock MongoDB client
    mock_mongo_client = MagicMock()
    mock_mongo_client.admin.command = AsyncMock(return_value={"ok": 1})
    mock_mongo_client.close = MagicMock()
    mock_motor.return_value = mock_mongo_client

    # Mock Kafka consumer
    mock_consumer = MagicMock()
    mock_consumer.start = AsyncMock()
    mock_consumer.stop = AsyncMock()
    mock_consumer.commit = AsyncMock()
    mock_consumer_class.return_value = mock_consumer

    # Mock DLQ producer
    mock_producer = MagicMock()
    mock_producer.start = AsyncMock()
    mock_producer.stop = AsyncMock()
    mock_producer_class.return_value = mock_producer

    # Mock consume_messages as async coroutine
    async def mock_consume_coro():
        await asyncio.sleep(0.1)  # Simulate running
        raise asyncio.CancelledError()

    mock_consume.return_value = mock_consume_coro()

    # Import and use lifespan
    from app import lifespan

    async with lifespan(app) as _:
        # Verify MongoDB connection
        mock_motor.assert_called_once()
        mock_mongo_client.admin.command.assert_called_once_with('ping')

        # Verify DLQ producer started
        mock_producer.start.assert_called_once()

        # Verify Kafka consumer started
        mock_consumer.start.assert_called_once()

    # Verify shutdown sequence
    mock_consumer.commit.assert_called_once()
    mock_consumer.stop.assert_called_once()
    mock_producer.stop.assert_called_once()
    mock_mongo_client.close.assert_called_once()


@pytest.mark.asyncio
@patch('app.AIOKafkaConsumer')
@patch('app.AIOKafkaProducer')
@patch('app.AsyncIOMotorClient')
async def test_lifespan_mongodb_connection_failure(mock_motor, mock_producer_class, mock_consumer_class):
    """Test handling of MongoDB connection failure during startup."""
    # Mock MongoDB client with failed ping
    mock_mongo_client = MagicMock()
    mock_mongo_client.admin.command = AsyncMock(side_effect=Exception("Connection refused"))
    mock_motor.return_value = mock_mongo_client

    from app import lifespan

    # Should raise exception on MongoDB connection failure
    with pytest.raises(Exception) as exc_info:
        async with lifespan(app) as _:
            pass

    assert "Connection refused" in str(exc_info.value)
    mock_mongo_client.admin.command.assert_called_once_with('ping')


@pytest.mark.asyncio
@patch('app.consume_messages')
@patch('app.AIOKafkaConsumer')
@patch('app.AIOKafkaProducer')
@patch('app.AsyncIOMotorClient')
async def test_lifespan_consumer_task_cancellation(mock_motor, mock_producer_class, mock_consumer_class, mock_consume):
    """Test consumer task cancellation during shutdown."""
    # Mock MongoDB client
    mock_mongo_client = MagicMock()
    mock_mongo_client.admin.command = AsyncMock(return_value={"ok": 1})
    mock_mongo_client.close = MagicMock()
    mock_motor.return_value = mock_mongo_client

    # Mock Kafka consumer
    mock_consumer = MagicMock()
    mock_consumer.start = AsyncMock()
    mock_consumer.stop = AsyncMock()
    mock_consumer.commit = AsyncMock()
    mock_consumer_class.return_value = mock_consumer

    # Mock DLQ producer
    mock_producer = MagicMock()
    mock_producer.start = AsyncMock()
    mock_producer.stop = AsyncMock()
    mock_producer_class.return_value = mock_producer

    # Mock consume_messages to immediately raise CancelledError
    async def mock_consume_coro():
        raise asyncio.CancelledError()

    mock_consume.return_value = mock_consume_coro()

    from app import lifespan

    async with lifespan(app) as _:
        pass

    # Verify shutdown was graceful
    mock_consumer.stop.assert_called_once()
    mock_producer.stop.assert_called_once()


@pytest.mark.asyncio
@patch('app.consume_messages')
@patch('app.AIOKafkaConsumer')
@patch('app.AIOKafkaProducer')
@patch('app.AsyncIOMotorClient')
async def test_lifespan_commit_error_during_shutdown(mock_motor, mock_producer_class, mock_consumer_class, mock_consume):
    """Test handling of commit errors during graceful shutdown."""
    # Mock MongoDB client
    mock_mongo_client = MagicMock()
    mock_mongo_client.admin.command = AsyncMock(return_value={"ok": 1})
    mock_mongo_client.close = MagicMock()
    mock_motor.return_value = mock_mongo_client

    # Mock Kafka consumer with commit error
    mock_consumer = MagicMock()
    mock_consumer.start = AsyncMock()
    mock_consumer.stop = AsyncMock()
    mock_consumer.commit = AsyncMock(side_effect=Exception("Commit failed"))
    mock_consumer_class.return_value = mock_consumer

    # Mock DLQ producer
    mock_producer = MagicMock()
    mock_producer.start = AsyncMock()
    mock_producer.stop = AsyncMock()
    mock_producer_class.return_value = mock_producer

    # Mock consume_messages
    async def mock_consume_coro():
        raise asyncio.CancelledError()

    mock_consume.return_value = mock_consume_coro()

    from app import lifespan

    # Should not raise exception even if commit fails
    async with lifespan(app) as _:
        pass

    # Verify commit was attempted
    mock_consumer.commit.assert_called_once()
    # Verify stop was still called despite commit error
    mock_consumer.stop.assert_called_once()


# ============================================================================
# Consumer Message Processing Tests
# ============================================================================

@pytest.mark.asyncio
@patch('app.mongo_client')
@patch('app.consumer')
@patch('app.dlq_producer')
async def test_consume_messages_successful_processing(mock_dlq_producer, mock_consumer, mock_mongo_client):
    """Test successful message consumption and MongoDB insertion."""
    # Mock MongoDB collection
    mock_collection = MagicMock()
    mock_collection.create_index = AsyncMock()
    mock_collection.insert_one = AsyncMock()

    mock_db = MagicMock()
    mock_db.purchases = mock_collection
    mock_mongo_client.__getitem__.return_value = mock_db

    # Mock Kafka message
    purchase_data = {
        "username": "alice",
        "user_id": "user123",
        "item_name": "Sword",
        "price": 9.99,
        "timestamp": "2024-01-01T12:00:00"
    }

    mock_message = MagicMock()
    mock_message.value = purchase_data
    mock_message.partition = 0
    mock_message.offset = 42

    # Create async iterator that yields one message then stops
    async def mock_iter():
        yield mock_message
        raise asyncio.CancelledError()

    mock_consumer.__aiter__ = lambda self: mock_iter()

    # Run consume_messages
    await consume_messages()

    # Verify index creation
    mock_collection.create_index.assert_called_once_with("user_id")

    # Verify message was inserted
    mock_collection.insert_one.assert_called_once_with(purchase_data)


@pytest.mark.asyncio
@patch('app.mongo_client')
@patch('app.consumer')
@patch('app.dlq_producer')
@patch('app.asyncio.sleep', new_callable=AsyncMock)
async def test_consume_messages_retry_with_backoff(mock_sleep, mock_dlq_producer, mock_consumer, mock_mongo_client):
    """Test retry logic with exponential backoff for transient errors."""
    # Mock MongoDB collection
    mock_collection = MagicMock()
    mock_collection.create_index = AsyncMock()
    # Fail twice, succeed on third attempt
    mock_collection.insert_one = AsyncMock(side_effect=[
        Exception("Transient error 1"),
        Exception("Transient error 2"),
        None  # Success
    ])

    mock_db = MagicMock()
    mock_db.purchases = mock_collection
    mock_mongo_client.__getitem__.return_value = mock_db

    # Mock Kafka message
    purchase_data = {
        "username": "bob",
        "user_id": "user456",
        "item_name": "Shield",
        "price": 19.99,
        "timestamp": "2024-01-01T12:00:00"
    }

    mock_message = MagicMock()
    mock_message.value = purchase_data
    mock_message.partition = 1
    mock_message.offset = 100

    # Create async iterator
    async def mock_iter():
        yield mock_message
        raise asyncio.CancelledError()

    mock_consumer.__aiter__ = lambda self: mock_iter()

    # Run consume_messages
    await consume_messages()

    # Verify retries with exponential backoff
    assert mock_collection.insert_one.call_count == 3
    assert mock_sleep.call_count == 2
    # Check backoff times: 2^1 = 2, 2^2 = 4
    mock_sleep.assert_any_call(2)
    mock_sleep.assert_any_call(4)


@pytest.mark.asyncio
@patch('app.mongo_client')
@patch('app.consumer')
@patch('app.dlq_producer')
@patch('app.asyncio.sleep', new_callable=AsyncMock)
async def test_consume_messages_dlq_after_max_retries(mock_sleep, mock_dlq_producer, mock_consumer, mock_mongo_client):
    """Test message sent to DLQ after exceeding max retries."""
    # Mock MongoDB collection with persistent failure
    mock_collection = MagicMock()
    mock_collection.create_index = AsyncMock()
    mock_collection.insert_one = AsyncMock(side_effect=Exception("Persistent error"))

    mock_db = MagicMock()
    mock_db.purchases = mock_collection
    mock_mongo_client.__getitem__.return_value = mock_db

    # Mock DLQ producer
    mock_dlq_producer.send_and_wait = AsyncMock()

    # Mock Kafka message
    purchase_data = {
        "username": "charlie",
        "user_id": "user789",
        "item_name": "Potion",
        "price": 5.99,
        "timestamp": "2024-01-01T12:00:00"
    }

    mock_message = MagicMock()
    mock_message.value = purchase_data
    mock_message.partition = 2
    mock_message.offset = 200

    # Create async iterator
    async def mock_iter():
        yield mock_message
        raise asyncio.CancelledError()

    mock_consumer.__aiter__ = lambda self: mock_iter()

    # Run consume_messages
    with patch('app.KAFKA_DLQ_TOPIC', 'purchases-dlq'):
        await consume_messages()

    # Verify max retries (3 attempts)
    assert mock_collection.insert_one.call_count == 3

    # Verify DLQ message was sent
    mock_dlq_producer.send_and_wait.assert_called_once()
    call_args = mock_dlq_producer.send_and_wait.call_args

    # Verify DLQ topic
    assert call_args[0][0] == 'purchases-dlq'

    # Verify DLQ message format
    dlq_message = json.loads(call_args[1]['value'].decode('utf-8'))
    assert dlq_message['original_message'] == purchase_data
    assert 'Persistent error' in dlq_message['error']
    assert 'failed_at' in dlq_message
    assert dlq_message['partition'] == 2
    assert dlq_message['offset'] == 200


@pytest.mark.asyncio
@patch('app.mongo_client')
@patch('app.consumer')
@patch('app.dlq_producer')
@patch('app.asyncio.sleep', new_callable=AsyncMock)
async def test_consume_messages_dlq_failure(mock_sleep, mock_dlq_producer, mock_consumer, mock_mongo_client):
    """Test handling of DLQ producer failures."""
    # Mock MongoDB collection with persistent failure
    mock_collection = MagicMock()
    mock_collection.create_index = AsyncMock()
    mock_collection.insert_one = AsyncMock(side_effect=Exception("Persistent error"))

    mock_db = MagicMock()
    mock_db.purchases = mock_collection
    mock_mongo_client.__getitem__.return_value = mock_db

    # Mock DLQ producer with failure
    mock_dlq_producer.send_and_wait = AsyncMock(side_effect=Exception("DLQ send failed"))

    # Mock Kafka message
    purchase_data = {"username": "dave", "user_id": "user999", "item_name": "Armor", "price": 29.99, "timestamp": "2024-01-01T12:00:00"}

    mock_message = MagicMock()
    mock_message.value = purchase_data
    mock_message.partition = 0
    mock_message.offset = 300

    # Create async iterator
    async def mock_iter():
        yield mock_message
        raise asyncio.CancelledError()

    mock_consumer.__aiter__ = lambda self: mock_iter()

    # Run consume_messages - should not raise exception
    await consume_messages()

    # Verify DLQ send was attempted
    mock_dlq_producer.send_and_wait.assert_called_once()


@pytest.mark.asyncio
@patch('app.mongo_client')
@patch('app.consumer')
@patch('app.dlq_producer')
async def test_consume_messages_index_creation(mock_dlq_producer, mock_consumer, mock_mongo_client):
    """Test MongoDB index creation on user_id field."""
    # Mock MongoDB collection
    mock_collection = MagicMock()
    mock_collection.create_index = AsyncMock()

    mock_db = MagicMock()
    mock_db.purchases = mock_collection
    mock_mongo_client.__getitem__.return_value = mock_db

    # Create proper async iterator that stops immediately
    class AsyncIterator:
        def __aiter__(self):
            return self

        async def __anext__(self):
            raise asyncio.CancelledError()

    mock_consumer.__aiter__ = lambda self: AsyncIterator()

    # Run consume_messages
    await consume_messages()

    # Verify index creation on user_id
    mock_collection.create_index.assert_called_once_with("user_id")


@pytest.mark.asyncio
@patch('app.mongo_client')
@patch('app.consumer')
@patch('app.dlq_producer')
async def test_consume_messages_cancellation(mock_dlq_producer, mock_consumer, mock_mongo_client):
    """Test consumer task handles CancelledError gracefully."""
    # Mock MongoDB collection
    mock_collection = MagicMock()
    mock_collection.create_index = AsyncMock()

    mock_db = MagicMock()
    mock_db.purchases = mock_collection
    mock_mongo_client.__getitem__.return_value = mock_db

    # Create proper async iterator that raises CancelledError
    class AsyncIterator:
        def __aiter__(self):
            return self

        async def __anext__(self):
            raise asyncio.CancelledError()

    mock_consumer.__aiter__ = lambda self: AsyncIterator()

    # Should not raise exception
    await consume_messages()


@pytest.mark.asyncio
@patch('app.mongo_client')
@patch('app.consumer')
@patch('app.dlq_producer')
async def test_consume_messages_unexpected_exception(mock_dlq_producer, mock_consumer, mock_mongo_client):
    """Test consumer raises unexpected exceptions (not CancelledError)."""
    # Mock MongoDB collection
    mock_collection = MagicMock()
    mock_collection.create_index = AsyncMock()

    mock_db = MagicMock()
    mock_db.purchases = mock_collection
    mock_mongo_client.__getitem__.return_value = mock_db

    # Create proper async iterator that raises unexpected exception
    class AsyncIterator:
        def __aiter__(self):
            return self

        async def __anext__(self):
            raise RuntimeError("Unexpected consumer error")

    mock_consumer.__aiter__ = lambda self: AsyncIterator()

    # Should raise the unexpected exception
    with pytest.raises(RuntimeError) as exc_info:
        await consume_messages()

    assert "Unexpected consumer error" in str(exc_info.value)


@pytest.mark.asyncio
@patch('app.mongo_client')
@patch('app.consumer')
@patch('app.dlq_producer')
async def test_consume_messages_malformed_message(mock_dlq_producer, mock_consumer, mock_mongo_client):
    """Test handling of malformed Kafka messages."""
    # Mock MongoDB collection
    mock_collection = MagicMock()
    mock_collection.create_index = AsyncMock()
    mock_collection.insert_one = AsyncMock()

    mock_db = MagicMock()
    mock_db.purchases = mock_collection
    mock_mongo_client.__getitem__.return_value = mock_db

    # Mock DLQ producer
    mock_dlq_producer.send_and_wait = AsyncMock()

    # Mock malformed message (missing required fields)
    malformed_data = {"username": "eve"}  # Missing user_id, item_name, price, timestamp

    mock_message = MagicMock()
    mock_message.value = malformed_data
    mock_message.partition = 0
    mock_message.offset = 400

    # Create async iterator
    async def mock_iter():
        yield mock_message
        raise asyncio.CancelledError()

    mock_consumer.__aiter__ = lambda self: mock_iter()

    # Run consume_messages
    await consume_messages()

    # Verify message was inserted (app doesn't validate, MongoDB handles it)
    mock_collection.insert_one.assert_called_once_with(malformed_data)


@pytest.mark.asyncio
@patch('app.mongo_client')
@patch('app.consumer')
@patch('app.dlq_producer')
async def test_consume_messages_multiple_messages(mock_dlq_producer, mock_consumer, mock_mongo_client):
    """Test processing multiple messages in sequence."""
    # Mock MongoDB collection
    mock_collection = MagicMock()
    mock_collection.create_index = AsyncMock()
    mock_collection.insert_one = AsyncMock()

    mock_db = MagicMock()
    mock_db.purchases = mock_collection
    mock_mongo_client.__getitem__.return_value = mock_db

    # Mock multiple messages
    messages = [
        {"username": "user1", "user_id": "id1", "item_name": "Item1", "price": 10.0, "timestamp": "2024-01-01T12:00:00"},
        {"username": "user2", "user_id": "id2", "item_name": "Item2", "price": 20.0, "timestamp": "2024-01-01T13:00:00"},
        {"username": "user3", "user_id": "id3", "item_name": "Item3", "price": 30.0, "timestamp": "2024-01-01T14:00:00"}
    ]

    mock_messages = []
    for i, data in enumerate(messages):
        msg = MagicMock()
        msg.value = data
        msg.partition = i % 3
        msg.offset = i * 100
        mock_messages.append(msg)

    # Create async iterator
    async def mock_iter():
        for msg in mock_messages:
            yield msg
        raise asyncio.CancelledError()

    mock_consumer.__aiter__ = lambda self: mock_iter()

    # Run consume_messages
    await consume_messages()

    # Verify all messages were inserted
    assert mock_collection.insert_one.call_count == 3
    for msg_data in messages:
        mock_collection.insert_one.assert_any_call(msg_data)


# ============================================================================
# DLQ Message Format Tests
# ============================================================================

@pytest.mark.asyncio
@patch('app.mongo_client')
@patch('app.consumer')
@patch('app.dlq_producer')
@patch('app.asyncio.sleep', new_callable=AsyncMock)
@patch('app.datetime')
async def test_dlq_message_format(mock_datetime, mock_sleep, mock_dlq_producer, mock_consumer, mock_mongo_client):
    """Test DLQ message includes all required metadata."""
    # Mock datetime
    mock_now = datetime(2024, 1, 1, 15, 30, 45)
    mock_datetime.utcnow.return_value = mock_now

    # Mock MongoDB collection with persistent failure
    mock_collection = MagicMock()
    mock_collection.create_index = AsyncMock()
    mock_collection.insert_one = AsyncMock(side_effect=Exception("Database error"))

    mock_db = MagicMock()
    mock_db.purchases = mock_collection
    mock_mongo_client.__getitem__.return_value = mock_db

    # Mock DLQ producer
    mock_dlq_producer.send_and_wait = AsyncMock()

    # Mock Kafka message
    purchase_data = {"username": "frank", "user_id": "user111", "item_name": "Helm", "price": 15.99, "timestamp": "2024-01-01T12:00:00"}

    mock_message = MagicMock()
    mock_message.value = purchase_data
    mock_message.partition = 1
    mock_message.offset = 500

    # Create async iterator
    async def mock_iter():
        yield mock_message
        raise asyncio.CancelledError()

    mock_consumer.__aiter__ = lambda self: mock_iter()

    # Run consume_messages
    with patch('app.KAFKA_TOPIC', 'purchases'):
        with patch('app.KAFKA_DLQ_TOPIC', 'purchases-dlq'):
            await consume_messages()

    # Verify DLQ message structure
    call_args = mock_dlq_producer.send_and_wait.call_args
    dlq_message = json.loads(call_args[1]['value'].decode('utf-8'))

    assert dlq_message['original_message'] == purchase_data
    assert dlq_message['error'] == "Database error"
    assert dlq_message['failed_at'] == mock_now.isoformat()
    assert dlq_message['topic'] == 'purchases'
    assert dlq_message['partition'] == 1
    assert dlq_message['offset'] == 500


# ============================================================================
# Pydantic Model Tests
# ============================================================================

def test_purchase_model_valid():
    """Test Purchase model with valid data."""
    purchase = Purchase(
        username="alice",
        user_id="user123",
        item_name="Sword",
        price=9.99,
        timestamp="2024-01-01T12:00:00"
    )

    assert purchase.username == "alice"
    assert purchase.user_id == "user123"
    assert purchase.item_name == "Sword"
    assert purchase.price == 9.99
    assert purchase.timestamp == "2024-01-01T12:00:00"


def test_purchases_response_model():
    """Test PurchasesResponse model."""
    purchases = [
        Purchase(
            username="alice",
            user_id="user123",
            item_name="Sword",
            price=9.99,
            timestamp="2024-01-01T12:00:00"
        )
    ]

    response = PurchasesResponse(
        user_id="user123",
        purchases=purchases,
        count=1
    )

    assert response.user_id == "user123"
    assert len(response.purchases) == 1
    assert response.count == 1


# ============================================================================
# Edge Cases and Integration Tests
# ============================================================================

@patch('app.mongo_client')
def test_get_purchases_very_large_result_set(mock_mongo):
    """Test handling of very large result sets (at max limit)."""
    # Create 1000 mock purchases (max limit)
    mock_purchases = [
        {
            "username": f"user{i}",
            "user_id": "user_heavy",
            "item_name": f"Item{i}",
            "price": float(i),
            "timestamp": f"2024-01-01T{i % 24:02d}:00:00"
        }
        for i in range(1000)
    ]

    mock_collection = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.to_list = AsyncMock(return_value=mock_purchases)

    mock_collection.find.return_value = mock_cursor
    mock_cursor.sort.return_value = mock_cursor
    mock_cursor.skip.return_value = mock_cursor
    mock_cursor.limit.return_value = mock_cursor

    mock_db = MagicMock()
    mock_db.purchases = mock_collection
    mock_mongo.__getitem__.return_value = mock_db

    response = client.get("/purchases/user_heavy?limit=1000")

    assert response.status_code == 200
    data = response.json()
    assert data["count"] == 1000
    assert len(data["purchases"]) == 1000


@patch('app.mongo_client')
def test_get_purchases_special_characters_in_user_id(mock_mongo):
    """Test handling of special characters in user_id."""
    mock_collection = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.to_list = AsyncMock(return_value=[])

    mock_collection.find.return_value = mock_cursor
    mock_cursor.sort.return_value = mock_cursor
    mock_cursor.skip.return_value = mock_cursor
    mock_cursor.limit.return_value = mock_cursor

    mock_db = MagicMock()
    mock_db.purchases = mock_collection
    mock_mongo.__getitem__.return_value = mock_db

    # Test with special characters
    special_user_id = "user@example.com"
    response = client.get(f"/purchases/{special_user_id}")

    assert response.status_code == 200
    # Verify query used the special character user_id
    mock_collection.find.assert_called_once_with(
        {"user_id": special_user_id},
        {"_id": 0}
    )


@pytest.mark.asyncio
@patch('app.mongo_client')
@patch('app.consumer')
@patch('app.dlq_producer')
async def test_consume_messages_mongodb_duplicate_key_error(mock_dlq_producer, mock_consumer, mock_mongo_client):
    """Test handling of MongoDB duplicate key errors."""
    # Mock MongoDB collection with duplicate key error
    from pymongo.errors import DuplicateKeyError

    mock_collection = MagicMock()
    mock_collection.create_index = AsyncMock()
    mock_collection.insert_one = AsyncMock(side_effect=DuplicateKeyError("Duplicate key"))

    mock_db = MagicMock()
    mock_db.purchases = mock_collection
    mock_mongo_client.__getitem__.return_value = mock_db

    # Mock DLQ producer
    mock_dlq_producer.send_and_wait = AsyncMock()

    # Mock Kafka message
    purchase_data = {"username": "george", "user_id": "user222", "item_name": "Ring", "price": 99.99, "timestamp": "2024-01-01T12:00:00"}

    mock_message = MagicMock()
    mock_message.value = purchase_data
    mock_message.partition = 0
    mock_message.offset = 600

    # Create async iterator
    async def mock_iter():
        yield mock_message
        raise asyncio.CancelledError()

    mock_consumer.__aiter__ = lambda self: mock_iter()

    # Run consume_messages
    await consume_messages()

    # Verify retries were attempted
    assert mock_collection.insert_one.call_count == 3

    # Verify message was sent to DLQ after max retries
    mock_dlq_producer.send_and_wait.assert_called_once()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
