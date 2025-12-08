#!/usr/bin/env python3
"""
Load test script for KEDA autoscaling
Sends many purchase requests to create Kafka consumer lag
"""
import asyncio
import httpx
import random
from datetime import datetime

# Configuration
WEB_SERVER_URL = "http://purchase.localhost:8080/api/buy"
NUM_REQUESTS = 500  # Send 200 purchases
CONCURRENT_REQUESTS = 50  # Send 20 at a time

PRODUCTS = [
    "Laptop Pro 15", "Wireless Mouse", "Mechanical Keyboard",
    "USB-C Cable", "External SSD 1TB", "Monitor 27 inch",
    "Webcam HD", "Headphones", "Desk Lamp", "Phone Charger"
]

USERS = [f"user{i}@test.com" for i in range(1, 21)]

async def send_purchase(client, request_num):
    """Send a single purchase request"""
    user_email = random.choice(USERS)
    purchase = {
        "username": user_email.split('@')[0],  # user1, user2, etc.
        "user_id": user_email,
        "item_name": random.choice(PRODUCTS),
        "price": round(random.uniform(10.99, 999.99), 2)
    }

    try:
        response = await client.post(WEB_SERVER_URL, json=purchase, timeout=10.0)
        if response.status_code == 200:
            print(f"✓ Request {request_num}: {purchase['item_name']}")
        else:
            print(f"✗ Request {request_num}: Status {response.status_code}")
    except Exception as e:
        print(f"✗ Request {request_num}: {e}")

async def load_test():
    """Run load test"""
    print(f"Starting load test: {NUM_REQUESTS} purchases")
    print(f"Target: {WEB_SERVER_URL}")
    print(f"Time: {datetime.now()}")
    print("-" * 60)

    async with httpx.AsyncClient() as client:
        # Send requests in batches
        for batch_start in range(0, NUM_REQUESTS, CONCURRENT_REQUESTS):
            batch_end = min(batch_start + CONCURRENT_REQUESTS, NUM_REQUESTS)
            tasks = [
                send_purchase(client, i)
                for i in range(batch_start, batch_end)
            ]
            await asyncio.gather(*tasks)

            # Small delay between batches
            await asyncio.sleep(0.5)

    print("-" * 60)
    print(f"Load test complete! Sent {NUM_REQUESTS} requests")
    print("\nNow check KEDA scaling:")
    print("  kubectl get deployment management-api -n purchase-system -w")
    print("  kubectl get pods -n purchase-system -w")

if __name__ == "__main__":
    asyncio.run(load_test())
