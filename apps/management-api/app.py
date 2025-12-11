"""
Management API

This service implements the event processing layer of the purchase system:

1. Kafka Consumer - Consumes purchase events from the 'purchases' topic
2. Data Persistence - Stores processed events in MongoDB
3. Query API - Provides GET /purchases/{user_id} endpoint for data retrieval

The service runs two concurrent tasks:
- Background consumer processing Kafka messages
- FastAPI server handling HTTP queries

KEDA autoscaling monitors Kafka consumer lag to scale replicas dynamically.
"""

import os
import json
import asyncio
import logging
from datetime import datetime
from typing import List, Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI
from pydantic import BaseModel
from motor.motor_asyncio import AsyncIOMotorClient
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from prometheus_fastapi_instrumentator import Instrumentator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Environment variables
# Note: These come from K8s ConfigMap
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka.kafka.svc.cluster.local:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "purchases")
KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "management-api-group")
KAFKA_DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "purchases-dlq")  # Dead Letter Queue topic
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://mongodb.mongodb.svc.cluster.local:27017")
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE", "purchases_db")

# Global instances
mongo_client: Optional[AsyncIOMotorClient] = None
consumer: Optional[AIOKafkaConsumer] = None
dlq_producer: Optional[AIOKafkaProducer] = None  # Producer for dead letter queue
consumer_task: Optional[asyncio.Task] = None


class Purchase(BaseModel):
    """Purchase record stored in MongoDB."""
    username: str
    user_id: str
    item_name: str
    price: float
    timestamp: str


class PurchasesResponse(BaseModel):
    """Response for GET /purchases/{user_id}."""
    user_id: str
    purchases: List[Purchase]
    count: int


async def consume_messages():
    """
    Background task that continuously consumes Kafka messages and persists them to MongoDB.

    Runs concurrently with the FastAPI server to handle both message processing
    and HTTP requests. Multiple service replicas join the same consumer group,
    with Kafka distributing partitions across replicas for horizontal scaling.

    Consumer starts from 'earliest' offset to ensure no messages are lost on restart.

    Error Handling: Failed messages are sent to a Dead Letter Queue (DLQ) for later
    inspection and manual reprocessing. This prevents a single bad message from
    blocking the entire consumer.
    """
    db = mongo_client[MONGODB_DATABASE]
    collection = db.purchases

    # Create index on user_id for efficient query performance
    await collection.create_index("user_id")

    logger.info(f"Starting Kafka consumer for topic: {KAFKA_TOPIC}")

    MAX_RETRIES = 3  # Retry each message up to 3 times before DLQ

    try:
        async for message in consumer:
            retry_count = 0
            success = False

            while retry_count < MAX_RETRIES and not success:
                try:
                    # Process message and store in MongoDB
                    purchase_data = message.value
                    logger.info(f"Received message: {purchase_data} (attempt {retry_count + 1})")

                    await collection.insert_one(purchase_data)
                    logger.info(f"Stored purchase for user: {purchase_data.get('user_id')}")

                    success = True
                    # Offset commit happens automatically (enable_auto_commit=True)

                except Exception as e:
                    retry_count += 1
                    logger.error(f"Error processing message (attempt {retry_count}/{MAX_RETRIES}): {e}")

                    if retry_count < MAX_RETRIES:
                        # Wait before retrying (exponential backoff)
                        await asyncio.sleep(2 ** retry_count)
                    else:
                        # Max retries exceeded - send to Dead Letter Queue
                        try:
                            dlq_message = {
                                "original_message": purchase_data,
                                "error": str(e),
                                "failed_at": datetime.utcnow().isoformat(),
                                "topic": KAFKA_TOPIC,
                                "partition": message.partition,
                                "offset": message.offset
                            }
                            await dlq_producer.send_and_wait(
                                KAFKA_DLQ_TOPIC,
                                value=json.dumps(dlq_message).encode('utf-8')
                            )
                            logger.warning(f"Message sent to DLQ: {dlq_message}")
                        except Exception as dlq_error:
                            logger.error(f"Failed to send message to DLQ: {dlq_error}")

    except asyncio.CancelledError:
        logger.info("Consumer task cancelled")
    except Exception as e:
        logger.error(f"Consumer error: {e}")
        raise


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan handler for setup and teardown.
    Initializes MongoDB connection, Kafka consumer, DLQ producer, and background processing task.
    """
    global mongo_client, consumer, dlq_producer, consumer_task

    # Connect to MongoDB
    logger.info(f"Connecting to MongoDB at {MONGODB_URI}")
    mongo_client = AsyncIOMotorClient(MONGODB_URI)

    # Verify MongoDB connection
    try:
        await mongo_client.admin.command('ping')
        logger.info("MongoDB connection successful")
    except Exception as e:
        logger.error(f"MongoDB connection failed: {e}")
        raise

    # Initialize Dead Letter Queue producer
    # Used to send failed messages for later inspection/reprocessing
    dlq_producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS
    )
    await dlq_producer.start()
    logger.info(f"DLQ producer started (topic: {KAFKA_DLQ_TOPIC})")

    # Initialize Kafka consumer with JSON deserialization
    # Starts from earliest offset to prevent message loss on restart
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_CONSUMER_GROUP,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    await consumer.start()
    logger.info("Kafka consumer started")

    # Start background consumer task
    consumer_task = asyncio.create_task(consume_messages())

    yield  # Application runs here

    # Graceful shutdown
    logger.info("Shutting down gracefully...")

    if consumer_task:
        # Cancel the background consumer task
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            logger.info("Consumer task cancelled")

    if consumer:
        # Commit any pending offsets before shutdown
        # This ensures we don't lose track of what we've processed
        try:
            await consumer.commit()
            logger.info("Final offset commit successful")
        except Exception as e:
            logger.warning(f"Error during final commit: {e}")

        # Stop the consumer gracefully
        await consumer.stop()
        logger.info("Kafka consumer stopped")

    if dlq_producer:
        # Stop DLQ producer
        await dlq_producer.stop()
        logger.info("DLQ producer stopped")

    if mongo_client:
        mongo_client.close()
        logger.info("MongoDB client closed")


app = FastAPI(
    title="Customer Management API",
    description="Consumes purchases from Kafka and provides query API",
    lifespan=lifespan
)

# Initialize Prometheus instrumentation
# Exposes /metrics endpoint for Prometheus scraping
Instrumentator().instrument(app).expose(app)


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "healthy", "service": "management-api"}


@app.get("/purchases/{user_id}", response_model=PurchasesResponse)
async def get_purchases(
    user_id: str,
    limit: int = 100,
    offset: int = 0
):
    """
    Retrieve purchases for a specific user from MongoDB with pagination support.

    Args:
        user_id: The user ID to query purchases for
        limit: Maximum number of results to return (default: 100, max: 1000)
        offset: Number of results to skip for pagination (default: 0)

    Returns:
        PurchasesResponse with purchases sorted by timestamp (most recent first)

    Note:
        For production use, consider implementing cursor-based pagination for better
        performance with large datasets. Current offset-based pagination can be slow
        for large offsets as MongoDB must scan all skipped documents.
    """
    # Validate and cap limit to prevent excessive memory usage
    limit = min(max(1, limit), 1000)
    offset = max(0, offset)

    db = mongo_client[MONGODB_DATABASE]
    collection = db.purchases

    # Query user's purchases with indexed user_id field
    cursor = collection.find(
        {"user_id": user_id},
        {"_id": 0}  # Exclude MongoDB's internal _id field
    ).sort("timestamp", -1).skip(offset).limit(limit)  # Most recent first

    purchases = await cursor.to_list(length=limit)

    return PurchasesResponse(
        user_id=user_id,
        purchases=purchases,
        count=len(purchases)
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
