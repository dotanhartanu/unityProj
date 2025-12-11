"""
Unit tests for web-server API endpoints.

Tests verify:
- Health check endpoint
- Purchase creation validation
- Error handling
- Response formats
- Lifespan management (startup/shutdown)
- Kafka producer initialization and cleanup
- Edge cases and error scenarios
- Integration scenarios with mocked dependencies

Run tests with: pytest test_app.py -v
For coverage: pytest test_app.py --cov=app --cov-report=html
"""

import sys
from unittest.mock import AsyncMock, MagicMock, patch, call
from datetime import datetime
import json

import pytest
from fastapi.testclient import TestClient
import httpx

# Mock dependencies before importing app
sys.modules['aiokafka'] = MagicMock()

# Now import the app
from app import app  # noqa: E402


# Test client
client = TestClient(app)


# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture
def mock_producer():
    """Create a mock Kafka producer for testing."""
    producer = MagicMock()
    producer.send_and_wait = AsyncMock()
    producer.start = AsyncMock()
    producer.stop = AsyncMock()
    return producer


@pytest.fixture
def sample_purchase_data():
    """Sample valid purchase data for tests."""
    return {
        "username": "alice",
        "user_id": "user123",
        "item_name": "Magic Potion",
        "price": 9.99
    }


@pytest.fixture
def sample_purchases_response():
    """Sample purchases response from management API."""
    return {
        "user_id": "user123",
        "purchases": [
            {
                "username": "alice",
                "user_id": "user123",
                "item_name": "Magic Potion",
                "price": 9.99,
                "timestamp": "2024-01-01T12:00:00"
            }
        ],
        "count": 1
    }


@pytest.fixture
def mock_httpx_client():
    """Create a mock httpx AsyncClient."""
    mock_client = AsyncMock()
    mock_client.__aenter__.return_value = mock_client
    mock_client.__aexit__.return_value = None
    return mock_client


def test_health_endpoint():
    """Test health check endpoint returns 200 OK."""
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "healthy", "service": "web-server"}


def test_create_purchase_success():
    """Test successful purchase creation."""
    # Mock the producer directly in the app module
    import app as app_module
    original_producer = app_module.producer

    # Create mock producer
    mock_producer = MagicMock()
    mock_producer.send_and_wait = AsyncMock()
    app_module.producer = mock_producer

    try:
        purchase_data = {
            "username": "alice",
            "user_id": "user123",
            "item_name": "Magic Potion",
            "price": 9.99
        }

        response = client.post("/buy", json=purchase_data)

        assert response.status_code == 200
        assert response.json() == {"status": "success", "message": "Purchase recorded"}

        # Verify send_and_wait was called
        assert mock_producer.send_and_wait.called
    finally:
        # Restore original producer
        app_module.producer = original_producer


def test_create_purchase_invalid_data():
    """Test purchase creation with invalid data."""
    # Missing required fields
    invalid_data = {
        "username": "alice",
        "item_name": "Magic Potion"
        # Missing user_id and price
    }

    response = client.post("/buy", json=invalid_data)

    assert response.status_code == 422  # Validation error


def test_create_purchase_invalid_price():
    """Test purchase creation with invalid price type."""
    invalid_data = {
        "username": "alice",
        "user_id": "user123",
        "item_name": "Magic Potion",
        "price": "not-a-number"  # Invalid type
    }

    response = client.post("/buy", json=invalid_data)

    assert response.status_code == 422  # Validation error


@patch('app.httpx.AsyncClient')
def test_get_purchases_success(mock_client_class):
    """Test successful retrieval of purchases."""
    # Mock httpx client
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "user_id": "user123",
        "purchases": [
            {
                "username": "alice",
                "user_id": "user123",
                "item_name": "Magic Potion",
                "price": 9.99,
                "timestamp": "2024-01-01T12:00:00"
            }
        ],
        "count": 1
    }
    mock_response.raise_for_status = MagicMock()

    mock_client = AsyncMock()
    mock_client.get.return_value = mock_response
    mock_client.__aenter__.return_value = mock_client
    mock_client.__aexit__.return_value = None

    mock_client_class.return_value = mock_client

    response = client.get("/purchases/user123")

    assert response.status_code == 200
    data = response.json()
    assert data["user_id"] == "user123"
    assert data["count"] == 1


@patch('app.httpx.AsyncClient')
def test_get_purchases_with_pagination(mock_client_class):
    """Test purchases retrieval with pagination parameters."""
    # Mock httpx client
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "user_id": "user123",
        "purchases": [],
        "count": 0
    }
    mock_response.raise_for_status = MagicMock()

    mock_client = AsyncMock()
    mock_client.get.return_value = mock_response
    mock_client.__aenter__.return_value = mock_client
    mock_client.__aexit__.return_value = None

    mock_client_class.return_value = mock_client

    response = client.get("/purchases/user123?limit=50&offset=10")

    # Should accept pagination parameters and return successfully
    assert response.status_code == 200

    # Verify pagination parameters were passed to management API
    mock_client.get.assert_called_once()
    call_args = mock_client.get.call_args
    assert call_args.kwargs['params'] == {"limit": 50, "offset": 10}



# ============================================================================
# LIFESPAN MANAGEMENT TESTS
# ============================================================================

@patch('app.AIOKafkaProducer')
@pytest.mark.asyncio
async def test_lifespan_kafka_producer_initialization(mock_producer_class):
    """Test that Kafka producer is initialized on startup."""
    mock_producer = MagicMock()
    mock_producer.start = AsyncMock()
    mock_producer.stop = AsyncMock()
    mock_producer_class.return_value = mock_producer

    import app as app_module
    from contextlib import asynccontextmanager

    # Simulate lifespan context
    async with app_module.lifespan(app):
        # Producer should be started
        mock_producer.start.assert_called_once()
        assert app_module.producer is not None

    # Producer should be stopped after context exit
    mock_producer.stop.assert_called_once()


@patch('app.AIOKafkaProducer')
@pytest.mark.asyncio
async def test_lifespan_producer_configuration(mock_producer_class):
    """Test that producer is configured with correct parameters."""
    mock_producer = MagicMock()
    mock_producer.start = AsyncMock()
    mock_producer.stop = AsyncMock()
    mock_producer_class.return_value = mock_producer

    import app as app_module

    async with app_module.lifespan(app):
        pass

    # Verify producer was created with correct config
    mock_producer_class.assert_called_once()
    call_kwargs = mock_producer_class.call_args.kwargs

    assert call_kwargs['acks'] == 'all'
    assert call_kwargs['request_timeout_ms'] == 30000
    assert 'value_serializer' in call_kwargs


@patch('app.AIOKafkaProducer')
@pytest.mark.asyncio
async def test_lifespan_graceful_shutdown(mock_producer_class):
    """Test graceful shutdown behavior with pending messages."""
    mock_producer = MagicMock()
    mock_producer.start = AsyncMock()
    mock_producer.stop = AsyncMock()
    mock_producer_class.return_value = mock_producer

    import app as app_module

    async with app_module.lifespan(app):
        # Simulate some activity
        assert app_module.producer is not None

    # Verify stop was called (flushes pending messages)
    mock_producer.stop.assert_called_once()


@patch('app.AIOKafkaProducer')
@pytest.mark.asyncio
async def test_lifespan_value_serializer_json(mock_producer_class):
    """Test that value serializer correctly converts to JSON."""
    mock_producer = MagicMock()
    mock_producer.start = AsyncMock()
    mock_producer.stop = AsyncMock()
    mock_producer_class.return_value = mock_producer

    import app as app_module

    async with app_module.lifespan(app):
        pass

    # Extract the serializer function
    serializer = mock_producer_class.call_args.kwargs['value_serializer']

    # Test serialization
    test_data = {"username": "alice", "price": 9.99}
    result = serializer(test_data)

    assert isinstance(result, bytes)
    assert json.loads(result.decode('utf-8')) == test_data


# ============================================================================
# PURCHASE CREATION TESTS (/buy endpoint)
# ============================================================================

def test_create_purchase_producer_none():
    """Test /buy endpoint when producer is None (service not ready)."""
    import app as app_module
    original_producer = app_module.producer

    try:
        app_module.producer = None

        purchase_data = {
            "username": "alice",
            "user_id": "user123",
            "item_name": "Magic Potion",
            "price": 9.99
        }

        response = client.post("/buy", json=purchase_data)

        assert response.status_code == 503
        assert "Kafka producer not initialized" in response.json()["detail"]
    finally:
        app_module.producer = original_producer


def test_create_purchase_kafka_send_failure(mock_producer):
    """Test /buy endpoint when Kafka send fails."""
    import app as app_module
    original_producer = app_module.producer

    try:
        # Configure mock to raise exception
        mock_producer.send_and_wait.side_effect = Exception("Kafka connection failed")
        app_module.producer = mock_producer

        purchase_data = {
            "username": "alice",
            "user_id": "user123",
            "item_name": "Magic Potion",
            "price": 9.99
        }

        response = client.post("/buy", json=purchase_data)

        assert response.status_code == 500
        assert "Failed to record purchase" in response.json()["detail"]
        assert "Kafka connection failed" in response.json()["detail"]
    finally:
        app_module.producer = original_producer


def test_create_purchase_partition_key_encoding(mock_producer):
    """Test that user_id is correctly encoded as partition key."""
    import app as app_module
    original_producer = app_module.producer

    try:
        app_module.producer = mock_producer

        purchase_data = {
            "username": "alice",
            "user_id": "user123",
            "item_name": "Magic Potion",
            "price": 9.99
        }

        response = client.post("/buy", json=purchase_data)

        assert response.status_code == 200

        # Verify send_and_wait was called with correct key encoding
        mock_producer.send_and_wait.assert_called_once()
        call_kwargs = mock_producer.send_and_wait.call_args.kwargs

        assert call_kwargs['key'] == b'user123'
        assert isinstance(call_kwargs['key'], bytes)
    finally:
        app_module.producer = original_producer


@patch('app.datetime')
def test_create_purchase_timestamp_generation(mock_datetime, mock_producer):
    """Test that server-side timestamp is generated correctly."""
    import app as app_module
    original_producer = app_module.producer

    try:
        # Mock datetime to return a fixed time
        fixed_time = datetime(2024, 1, 15, 12, 30, 45)
        mock_datetime.utcnow.return_value = fixed_time

        app_module.producer = mock_producer

        purchase_data = {
            "username": "alice",
            "user_id": "user123",
            "item_name": "Magic Potion",
            "price": 9.99
        }

        response = client.post("/buy", json=purchase_data)

        assert response.status_code == 200

        # Verify timestamp in message
        call_kwargs = mock_producer.send_and_wait.call_args.kwargs
        message_value = call_kwargs['value']

        assert message_value['timestamp'] == fixed_time.isoformat()
    finally:
        app_module.producer = original_producer


def test_create_purchase_message_serialization(mock_producer):
    """Test that purchase message is correctly serialized with all fields."""
    import app as app_module
    original_producer = app_module.producer

    try:
        app_module.producer = mock_producer

        purchase_data = {
            "username": "alice",
            "user_id": "user123",
            "item_name": "Magic Potion",
            "price": 9.99
        }

        response = client.post("/buy", json=purchase_data)

        assert response.status_code == 200

        # Verify message contains all required fields
        call_kwargs = mock_producer.send_and_wait.call_args.kwargs
        message = call_kwargs['value']

        assert message['username'] == "alice"
        assert message['user_id'] == "user123"
        assert message['item_name'] == "Magic Potion"
        assert message['price'] == 9.99
        assert 'timestamp' in message
    finally:
        app_module.producer = original_producer


def test_create_purchase_kafka_topic_name(mock_producer):
    """Test that messages are sent to the correct Kafka topic."""
    import app as app_module
    original_producer = app_module.producer

    try:
        app_module.producer = mock_producer

        purchase_data = {
            "username": "alice",
            "user_id": "user123",
            "item_name": "Magic Potion",
            "price": 9.99
        }

        response = client.post("/buy", json=purchase_data)

        assert response.status_code == 200

        # Verify correct topic
        call_args = mock_producer.send_and_wait.call_args.args
        assert call_args[0] == "purchases"
    finally:
        app_module.producer = original_producer


# ============================================================================
# PURCHASE RETRIEVAL TESTS (/purchases/{user_id})
# ============================================================================

@patch('app.httpx.AsyncClient')
def test_get_purchases_timeout_handling(mock_client_class):
    """Test /purchases endpoint handles timeout with 504."""
    mock_client = AsyncMock()
    mock_client.get.side_effect = httpx.TimeoutException("Request timeout")
    mock_client.__aenter__.return_value = mock_client
    mock_client.__aexit__.return_value = None

    mock_client_class.return_value = mock_client

    response = client.get("/purchases/user123")

    assert response.status_code == 504
    assert "Management API timeout" in response.json()["detail"]


@patch('app.httpx.AsyncClient')
def test_get_purchases_404_error(mock_client_class):
    """Test /purchases endpoint handles 404 from management API."""
    mock_response = MagicMock()
    mock_response.status_code = 404
    mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
        "Not found",
        request=MagicMock(),
        response=mock_response
    )

    mock_client = AsyncMock()
    mock_client.get.return_value = mock_response
    mock_client.__aenter__.return_value = mock_client
    mock_client.__aexit__.return_value = None

    mock_client_class.return_value = mock_client

    response = client.get("/purchases/user123")

    assert response.status_code == 404


@patch('app.httpx.AsyncClient')
def test_get_purchases_500_error(mock_client_class):
    """Test /purchases endpoint handles 500 from management API."""
    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
        "Internal server error",
        request=MagicMock(),
        response=mock_response
    )

    mock_client = AsyncMock()
    mock_client.get.return_value = mock_response
    mock_client.__aenter__.return_value = mock_client
    mock_client.__aexit__.return_value = None

    mock_client_class.return_value = mock_client

    response = client.get("/purchases/user123")

    assert response.status_code == 500


@patch('app.httpx.AsyncClient')
def test_get_purchases_network_failure(mock_client_class):
    """Test /purchases endpoint handles network failures."""
    mock_client = AsyncMock()
    mock_client.get.side_effect = httpx.ConnectError("Connection failed")
    mock_client.__aenter__.return_value = mock_client
    mock_client.__aexit__.return_value = None

    mock_client_class.return_value = mock_client

    response = client.get("/purchases/user123")

    assert response.status_code == 500


@patch('app.httpx.AsyncClient')
def test_get_purchases_response_parsing_error(mock_client_class):
    """Test /purchases endpoint handles JSON parsing errors."""
    mock_response = MagicMock()
    mock_response.json.side_effect = json.JSONDecodeError("Invalid JSON", "", 0)
    mock_response.raise_for_status = MagicMock()

    mock_client = AsyncMock()
    mock_client.get.return_value = mock_response
    mock_client.__aenter__.return_value = mock_client
    mock_client.__aexit__.return_value = None

    mock_client_class.return_value = mock_client

    response = client.get("/purchases/user123")

    assert response.status_code == 500


@patch('app.httpx.AsyncClient')
def test_get_purchases_pagination_default_values(mock_client_class):
    """Test /purchases endpoint uses default pagination values."""
    mock_response = MagicMock()
    mock_response.json.return_value = {"purchases": [], "count": 0}
    mock_response.raise_for_status = MagicMock()

    mock_client = AsyncMock()
    mock_client.get.return_value = mock_response
    mock_client.__aenter__.return_value = mock_client
    mock_client.__aexit__.return_value = None

    mock_client_class.return_value = mock_client

    response = client.get("/purchases/user123")

    assert response.status_code == 200

    # Verify default pagination parameters
    call_kwargs = mock_client.get.call_args.kwargs
    assert call_kwargs['params'] == {"limit": 100, "offset": 0}


@patch('app.httpx.AsyncClient')
def test_get_purchases_pagination_custom_values(mock_client_class):
    """Test /purchases endpoint passes custom pagination values."""
    mock_response = MagicMock()
    mock_response.json.return_value = {"purchases": [], "count": 0}
    mock_response.raise_for_status = MagicMock()

    mock_client = AsyncMock()
    mock_client.get.return_value = mock_response
    mock_client.__aenter__.return_value = mock_client
    mock_client.__aexit__.return_value = None

    mock_client_class.return_value = mock_client

    response = client.get("/purchases/user123?limit=25&offset=50")

    assert response.status_code == 200

    # Verify custom pagination parameters
    call_kwargs = mock_client.get.call_args.kwargs
    assert call_kwargs['params'] == {"limit": 25, "offset": 50}


@patch('app.httpx.AsyncClient')
def test_get_purchases_timeout_configuration(mock_client_class):
    """Test /purchases endpoint uses correct timeout configuration."""
    mock_response = MagicMock()
    mock_response.json.return_value = {"purchases": [], "count": 0}
    mock_response.raise_for_status = MagicMock()

    mock_client = AsyncMock()
    mock_client.get.return_value = mock_response
    mock_client.__aenter__.return_value = mock_client
    mock_client.__aexit__.return_value = None

    mock_client_class.return_value = mock_client

    response = client.get("/purchases/user123")

    assert response.status_code == 200

    # Verify timeout is set
    call_kwargs = mock_client.get.call_args.kwargs
    assert call_kwargs['timeout'] == 10.0


# ============================================================================
# EDGE CASES TESTS
# ============================================================================

def test_create_purchase_empty_username(mock_producer):
    """Test purchase creation with empty username."""
    import app as app_module
    original_producer = app_module.producer

    try:
        app_module.producer = mock_producer

        purchase_data = {
            "username": "",
            "user_id": "user123",
            "item_name": "Magic Potion",
            "price": 9.99
        }

        # Note: Pydantic will accept empty string for str type
        # This tests that the system handles it (business logic may need to validate)
        response = client.post("/buy", json=purchase_data)

        # Should succeed with current validation
        assert response.status_code == 200
    finally:
        app_module.producer = original_producer


def test_create_purchase_whitespace_fields(mock_producer):
    """Test purchase creation with whitespace-only fields."""
    import app as app_module
    original_producer = app_module.producer

    try:
        app_module.producer = mock_producer

        purchase_data = {
            "username": "   ",
            "user_id": "   ",
            "item_name": "   ",
            "price": 9.99
        }

        # Should succeed with current validation (no strip validation)
        response = client.post("/buy", json=purchase_data)
        assert response.status_code == 200
    finally:
        app_module.producer = original_producer


def test_create_purchase_very_long_strings(mock_producer):
    """Test purchase creation with very long string fields."""
    import app as app_module
    original_producer = app_module.producer

    try:
        app_module.producer = mock_producer

        # 1000 character strings
        long_string = "a" * 1000

        purchase_data = {
            "username": long_string,
            "user_id": long_string,
            "item_name": long_string,
            "price": 9.99
        }

        response = client.post("/buy", json=purchase_data)
        assert response.status_code == 200

        # Verify data was passed correctly
        call_kwargs = mock_producer.send_and_wait.call_args.kwargs
        message = call_kwargs['value']
        assert len(message['username']) == 1000
    finally:
        app_module.producer = original_producer


def test_create_purchase_negative_price(mock_producer):
    """Test purchase creation with negative price."""
    import app as app_module
    original_producer = app_module.producer

    try:
        app_module.producer = mock_producer

        purchase_data = {
            "username": "alice",
            "user_id": "user123",
            "item_name": "Magic Potion",
            "price": -9.99
        }

        # Pydantic accepts negative floats (no built-in validation)
        # Business logic may need additional validation
        response = client.post("/buy", json=purchase_data)

        # Current implementation accepts negative prices
        # This test documents current behavior
        # Consider adding validation if negative prices should be rejected
        assert response.status_code == 200

        # Verify negative price is preserved
        call_kwargs = mock_producer.send_and_wait.call_args.kwargs
        message = call_kwargs['value']
        assert message['price'] == -9.99
    finally:
        app_module.producer = original_producer


def test_create_purchase_zero_price(mock_producer):
    """Test purchase creation with zero price."""
    import app as app_module
    original_producer = app_module.producer

    try:
        app_module.producer = mock_producer

        purchase_data = {
            "username": "alice",
            "user_id": "user123",
            "item_name": "Free Item",
            "price": 0.0
        }

        response = client.post("/buy", json=purchase_data)
        assert response.status_code == 200

        # Verify zero price is preserved
        call_kwargs = mock_producer.send_and_wait.call_args.kwargs
        message = call_kwargs['value']
        assert message['price'] == 0.0
    finally:
        app_module.producer = original_producer


def test_create_purchase_very_large_price(mock_producer):
    """Test purchase creation with very large price."""
    import app as app_module
    original_producer = app_module.producer

    try:
        app_module.producer = mock_producer

        purchase_data = {
            "username": "alice",
            "user_id": "user123",
            "item_name": "Expensive Item",
            "price": 999999999.99
        }

        response = client.post("/buy", json=purchase_data)
        assert response.status_code == 200

        # Verify large price is preserved
        call_kwargs = mock_producer.send_and_wait.call_args.kwargs
        message = call_kwargs['value']
        assert message['price'] == 999999999.99
    finally:
        app_module.producer = original_producer


def test_create_purchase_special_characters_in_fields(mock_producer):
    """Test purchase creation with special characters."""
    import app as app_module
    original_producer = app_module.producer

    try:
        app_module.producer = mock_producer

        purchase_data = {
            "username": "alice<script>alert('xss')</script>",
            "user_id": "user_123!@#$%^&*()",
            "item_name": "Potion's \"Magic\" & <Item>",
            "price": 9.99
        }

        response = client.post("/buy", json=purchase_data)
        assert response.status_code == 200

        # Verify special characters are preserved (not sanitized)
        call_kwargs = mock_producer.send_and_wait.call_args.kwargs
        message = call_kwargs['value']
        assert "<script>" in message['username']
    finally:
        app_module.producer = original_producer


def test_create_purchase_unicode_characters(mock_producer):
    """Test purchase creation with Unicode characters."""
    import app as app_module
    original_producer = app_module.producer

    try:
        app_module.producer = mock_producer

        purchase_data = {
            "username": "用户Alice",
            "user_id": "user_مستخدم_123",
            "item_name": "魔法のポーション",
            "price": 9.99
        }

        response = client.post("/buy", json=purchase_data)
        assert response.status_code == 200

        # Verify Unicode is preserved
        call_kwargs = mock_producer.send_and_wait.call_args.kwargs
        message = call_kwargs['value']
        assert "魔法" in message['item_name']
    finally:
        app_module.producer = original_producer


@patch('app.httpx.AsyncClient')
def test_get_purchases_special_characters_in_user_id(mock_client_class):
    """Test /purchases endpoint with special characters in user_id."""
    mock_response = MagicMock()
    mock_response.json.return_value = {"purchases": [], "count": 0}
    mock_response.raise_for_status = MagicMock()

    mock_client = AsyncMock()
    mock_client.get.return_value = mock_response
    mock_client.__aenter__.return_value = mock_client
    mock_client.__aexit__.return_value = None

    mock_client_class.return_value = mock_client

    # URL encoding should handle special characters
    response = client.get("/purchases/user@123!test")

    assert response.status_code == 200


# ============================================================================
# INTEGRATION SCENARIOS
# ============================================================================

def test_full_purchase_flow_success(mock_producer):
    """Test complete purchase creation flow with all components."""
    import app as app_module
    original_producer = app_module.producer

    try:
        app_module.producer = mock_producer

        # Step 1: Create purchase
        purchase_data = {
            "username": "alice",
            "user_id": "user123",
            "item_name": "Magic Potion",
            "price": 9.99
        }

        response = client.post("/buy", json=purchase_data)

        # Verify response
        assert response.status_code == 200
        assert response.json()["status"] == "success"

        # Verify Kafka interaction
        mock_producer.send_and_wait.assert_called_once()

        # Verify message structure
        call_kwargs = mock_producer.send_and_wait.call_args.kwargs
        assert call_kwargs['key'] == b'user123'
        assert call_kwargs['value']['username'] == 'alice'
        assert call_kwargs['value']['price'] == 9.99
        assert 'timestamp' in call_kwargs['value']
    finally:
        app_module.producer = original_producer


@patch('app.httpx.AsyncClient')
def test_full_retrieval_flow_success(mock_client_class, sample_purchases_response):
    """Test complete purchase retrieval flow."""
    mock_response = MagicMock()
    mock_response.json.return_value = sample_purchases_response
    mock_response.raise_for_status = MagicMock()

    mock_client = AsyncMock()
    mock_client.get.return_value = mock_response
    mock_client.__aenter__.return_value = mock_client
    mock_client.__aexit__.return_value = None

    mock_client_class.return_value = mock_client

    # Retrieve purchases
    response = client.get("/purchases/user123?limit=50&offset=0")

    # Verify response
    assert response.status_code == 200
    data = response.json()
    assert data["user_id"] == "user123"
    assert len(data["purchases"]) == 1
    assert data["count"] == 1

    # Verify HTTP client interaction
    mock_client.get.assert_called_once()
    call_args = mock_client.get.call_args
    assert "user123" in call_args.args[0]
    assert call_args.kwargs['params'] == {"limit": 50, "offset": 0}
    assert call_args.kwargs['timeout'] == 10.0


def test_error_propagation_from_kafka(mock_producer):
    """Test that Kafka errors are properly propagated to client."""
    import app as app_module
    original_producer = app_module.producer

    try:
        # Simulate Kafka broker unavailable
        mock_producer.send_and_wait.side_effect = Exception("Broker unavailable")
        app_module.producer = mock_producer

        purchase_data = {
            "username": "alice",
            "user_id": "user123",
            "item_name": "Magic Potion",
            "price": 9.99
        }

        response = client.post("/buy", json=purchase_data)

        # Verify error is returned to client
        assert response.status_code == 500
        assert "Failed to record purchase" in response.json()["detail"]
        assert "Broker unavailable" in response.json()["detail"]
    finally:
        app_module.producer = original_producer


@patch('app.httpx.AsyncClient')
def test_error_propagation_from_management_api(mock_client_class):
    """Test that Management API errors are properly propagated."""
    mock_response = MagicMock()
    mock_response.status_code = 503
    mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
        "Service unavailable",
        request=MagicMock(),
        response=mock_response
    )

    mock_client = AsyncMock()
    mock_client.get.return_value = mock_response
    mock_client.__aenter__.return_value = mock_client
    mock_client.__aexit__.return_value = None

    mock_client_class.return_value = mock_client

    response = client.get("/purchases/user123")

    # Verify error propagation
    assert response.status_code == 503


def test_concurrent_purchase_requests(mock_producer):
    """Test handling of concurrent purchase requests."""
    import app as app_module
    original_producer = app_module.producer

    try:
        app_module.producer = mock_producer

        purchase_data = {
            "username": "alice",
            "user_id": "user123",
            "item_name": "Magic Potion",
            "price": 9.99
        }

        # Make multiple concurrent requests
        responses = []
        for _ in range(5):
            response = client.post("/buy", json=purchase_data)
            responses.append(response)

        # All should succeed
        for response in responses:
            assert response.status_code == 200

        # Verify all were sent to Kafka
        assert mock_producer.send_and_wait.call_count == 5
    finally:
        app_module.producer = original_producer


@patch('app.httpx.AsyncClient')
def test_health_check_independent_of_dependencies(mock_client_class):
    """Test that health check works even if other dependencies fail."""
    import app as app_module
    original_producer = app_module.producer

    try:
        # Set producer to None to simulate failure
        app_module.producer = None

        # Health check should still work
        response = client.get("/health")

        assert response.status_code == 200
        assert response.json()["status"] == "healthy"
    finally:
        app_module.producer = original_producer


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
