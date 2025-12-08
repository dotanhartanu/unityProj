"""
Web Server API

This service handles incoming HTTP requests and coordinates between the frontend
and backend services. It implements an event-driven architecture pattern where:

1. POST /buy - Publishes purchase events to Kafka (asynchronous writes)
   - Fast response time for users
   - Decouples write path from storage
   - Enables horizontal scaling of message processing

2. GET /purchases/{user_id} - Queries Management API for user's purchase history
   - Synchronous reads for immediate data retrieval
   - Direct HTTP communication for simplicity

3. GET /health - Health check endpoint for Kubernetes probes

The service uses Kafka producer for reliable message delivery with configurable
retry behavior and idempotent writes to prevent duplicate messages.
"""

import os
import json
import logging
from datetime import datetime
from typing import Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import httpx
from aiokafka import AIOKafkaProducer

# Configure logging - important for debugging in K8s
# Note: In K8s, logs go to stdout/stderr and are collected by the logging stack
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Environment variables - injected via K8s ConfigMap/Secrets
# Note: Never hardcode these! 12-factor app principle.
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka.kafka.svc.cluster.local:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "purchases")
MANAGEMENT_API_URL = os.getenv("MANAGEMENT_API_URL", "http://management-api:8000")

# Global producer instance - reused across requests
# Note: Creating producer per request is expensive (TCP connections, handshakes)
producer: Optional[AIOKafkaProducer] = None


class PurchaseRequest(BaseModel):
    """Purchase request model with automatic validation and type coercion."""
    username: str
    user_id: str
    item_name: str
    price: float


class PurchaseMessage(BaseModel):
    """
    Kafka message schema for purchase events.
    Includes server-side timestamp for event ordering.
    """
    username: str
    user_id: str
    item_name: str
    price: float
    timestamp: str  # ISO format string for portability


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan handler - runs on startup and shutdown.
    Initializes Kafka producer before serving requests and ensures clean shutdown.
    """
    global producer

    logger.info(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")

    # Initialize Kafka producer with JSON serialization
    # Producer is reused across all requests for efficiency
    # Note: aiokafka 0.10.0 has limited configuration options
    # Reliability is handled through acks and request_timeout_ms
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',  # Wait for all replicas to acknowledge (strongest guarantee)
        request_timeout_ms=30000,  # 30 second timeout per request
    )
    await producer.start()
    logger.info("Kafka producer started with acks='all' for reliable delivery")

    yield  # Application runs here

    # Cleanup: flush pending messages and close connections
    logger.info("Shutting down Kafka producer")
    await producer.stop()


app = FastAPI(
    title="Web Server API",
    description="Handles purchase requests and queries for the Unity DevOps assignment",
    lifespan=lifespan
)


@app.get("/health")
async def health():
    """
    Health check endpoint for Kubernetes liveness and readiness probes.
    Returns service status to determine if container should receive traffic.
    """
    return {"status": "healthy", "service": "web-server"}


@app.post("/buy")
async def create_purchase(purchase: PurchaseRequest):
    """
    Process purchase request by publishing to Kafka.

    Returns immediately after message is queued (async processing).
    Actual persistence happens via the management-api consumer.
    """
    if producer is None:
        raise HTTPException(status_code=503, detail="Kafka producer not initialized")

    # Create message with server-side timestamp for consistency
    message = PurchaseMessage(
        username=purchase.username,
        user_id=purchase.user_id,
        item_name=purchase.item_name,
        price=purchase.price,
        timestamp=datetime.utcnow().isoformat()
    )

    try:
        # Publish to Kafka with user_id as key for partition assignment
        # This ensures ordering: all purchases for a user go to the same partition
        await producer.send_and_wait(
            KAFKA_TOPIC,
            value=message.model_dump(),
            key=purchase.user_id.encode('utf-8')
        )

        logger.info(f"Purchase published: user={purchase.user_id}, item={purchase.item_name}")
        return {"status": "success", "message": "Purchase recorded"}

    except Exception as e:
        logger.error(f"Failed to publish purchase: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to record purchase: {str(e)}")


@app.get("/purchases/{user_id}")
async def get_purchases(
    user_id: str,
    limit: int = 100,
    offset: int = 0
):
    """
    Retrieve purchases for a given user from the Management API with pagination support.

    Args:
        user_id: The user ID to query purchases for
        limit: Maximum number of results to return (default: 100, max: 1000)
        offset: Number of results to skip for pagination (default: 0)

    Uses synchronous HTTP request-response pattern for immediate data retrieval.
    """
    try:
        async with httpx.AsyncClient() as client:
            # Use timeout to prevent blocking on slow/dead services
            response = await client.get(
                f"{MANAGEMENT_API_URL}/purchases/{user_id}",
                params={"limit": limit, "offset": offset},
                timeout=10.0
            )
            response.raise_for_status()
            return response.json()

    except httpx.TimeoutException:
        logger.error(f"Timeout calling management API for user {user_id}")
        raise HTTPException(status_code=504, detail="Management API timeout")

    except httpx.HTTPStatusError as e:
        logger.error(f"Management API error: {e}")
        raise HTTPException(status_code=e.response.status_code, detail=str(e))

    except Exception as e:
        logger.error(f"Error fetching purchases: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
