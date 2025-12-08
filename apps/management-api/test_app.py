"""
Unit tests for management-api endpoints.

Tests verify:
- Health check endpoint
- Purchase retrieval endpoint
- Pagination support
- Response format validation

Run tests with: pytest test_app.py
"""

import sys
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

# Mock dependencies before importing app
sys.modules['aiokafka'] = MagicMock()
sys.modules['motor'] = MagicMock()
sys.modules['motor.motor_asyncio'] = MagicMock()

# Now import the app
from app import app  # noqa: E402


# Test client
client = TestClient(app)


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


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
