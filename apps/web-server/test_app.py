"""
Unit tests for web-server API endpoints.

Tests verify:
- Health check endpoint
- Purchase creation validation
- Error handling
- Response formats

Run tests with: pytest test_app.py
"""

import sys
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

# Mock dependencies before importing app
sys.modules['aiokafka'] = MagicMock()

# Now import the app
from app import app  # noqa: E402


# Test client
client = TestClient(app)


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


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
