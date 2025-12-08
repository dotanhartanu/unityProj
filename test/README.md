# Testing Guide

This directory contains all testing resources and documentation for the Purchase Tracking System.

## 📁 Test Files

- `load-test.py` - Python script for load testing and KEDA autoscaling validation

---

## 🧪 Manual Testing

### Testing via UI

1. Open http://purchase.localhost:8080
2. Fill in purchase form:
   - Username: `john`
   - User ID: `user123`
   - Item Name: `Golden Sword`
   - Price: `29.99`
3. Click "Process Purchase"
4. Enter `user123` in search box
5. Click "Search" to view purchases

### Testing via API (curl)

**Make a purchase:**
```bash
curl -X POST http://purchase.localhost:8080/api/buy \
  -H "Content-Type: application/json" \
  -d '{
    "username": "alice",
    "user_id": "user456",
    "item_name": "Magic Potion",
    "price": 9.99
  }'
```

**Get purchases for user:**
```bash
curl http://purchase.localhost:8080/api/purchases/user456
```

---

## 🚀 Load Testing

### Using the Python Load Test Script

The `load-test.py` script generates realistic load and validates KEDA autoscaling.

**Install dependencies:**
```bash
pip install requests
```

**Run load test:**
```bash
# Generate 100 purchase requests
python test/load-test.py

# Watch Management API scale up
kubectl get pods -n purchase-system -w
```

**What it does:**
- Sends 100 concurrent POST requests to `/api/buy`
- Generates unique users (load1, load2, ..., load100)
- Validates response status codes
- Reports success/failure count

### Using curl for Quick Load

```bash
# Generate 100 requests with background jobs
for i in {1..100}; do
  curl -X POST http://purchase.localhost:8080/api/buy \
    -H "Content-Type: application/json" \
    -d "{\"username\":\"load$i\",\"user_id\":\"load$i\",\"item_name\":\"Test\",\"price\":1.00}" &
done

# Wait for all requests to complete
wait
```

---

## 📊 Validating KEDA Autoscaling

### Expected Behavior

When load is applied, KEDA should:
1. Detect consumer lag in Kafka
2. Scale Management API from 1 → 2 → 3 replicas
3. Scale back down to 1 after cooldown period (60s)

### Monitor Scaling

**Watch pods scale:**
```bash
kubectl get pods -n purchase-system -w
```

Expected output:
```
NAME                              READY   STATUS    RESTARTS   AGE
management-api-xxx-yyy            1/1     Running   0          5m
management-api-xxx-zzz            0/1     Pending   0          0s   <- New pod
management-api-xxx-zzz            1/1     Running   0          5s   <- Ready
```

**Check KEDA ScaledObject:**
```bash
kubectl get scaledobject -n purchase-system
kubectl describe scaledobject management-api-scaledobject -n purchase-system
```

**Check HPA (created by KEDA):**
```bash
kubectl get hpa -n purchase-system
```

Expected output:
```
NAME             REFERENCE                   TARGETS     MINPODS   MAXPODS   REPLICAS
management-api   Deployment/management-api   5/10        1         3         2
```

### Check Kafka Consumer Lag

```bash
# Check consumer group lag
kubectl exec -n kafka kafka-broker-0 -- \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group management-api-group
```

Expected output:
```
GROUP                TOPIC      PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
management-api-group purchases  0          50              100             50
management-api-group purchases  1          45              95              50
management-api-group purchases  2          48              98              50
```

**Understanding the output:**
- `CURRENT-OFFSET`: Last consumed message offset
- `LOG-END-OFFSET`: Latest message offset in partition
- `LAG`: Messages waiting to be consumed (LOG-END-OFFSET - CURRENT-OFFSET)
- High lag triggers KEDA to scale up

---

## 🐛 Troubleshooting Tests

### No Response from API

**Check if pods are running:**
```bash
kubectl get pods -n purchase-system
```

**Check ingress:**
```bash
kubectl get ingress -n purchase-system
```

**Check /etc/hosts:**
```bash
cat /etc/hosts | grep purchase.localhost
```

### Autoscaling Not Working

**Check KEDA is installed:**
```bash
kubectl get pods -n keda
```

**Check ScaledObject:**
```bash
kubectl describe scaledobject management-api-scaledobject -n purchase-system
```

Look for errors in events section.

**Check KEDA operator logs:**
```bash
kubectl logs -n keda -l app.kubernetes.io/name=keda-operator -f
```

### Messages Not Being Consumed

**Check Management API logs:**
```bash
kubectl logs -n purchase-system -l app=management-api -f
```

**Check Kafka connectivity:**
```bash
# Check if Kafka service is accessible
kubectl get svc -n kafka kafka-kafka-bootstrap

# Check management-api logs for Kafka connection
kubectl logs -n purchase-system -l app=management-api --tail=50 | grep -i kafka
```

**Verify topic exists:**
```bash
kubectl exec -n kafka kafka-broker-0 -- /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

---

## 📈 Performance Benchmarks

### Expected Throughput

With default resource limits:
- **Web Server**: ~100 requests/second per replica
- **Management API**: ~50 messages/second per replica (limited by MongoDB writes)
- **MongoDB**: ~200 writes/second (single instance)

### Scaling Thresholds

- **lagThreshold**: 10 messages per partition
- **minReplicaCount**: 1 (always one consumer running)
- **maxReplicaCount**: 3 (matches Kafka partition count)

**Scaling formula:**
```
desiredReplicas = totalLag / (partitions × lagThreshold)
```

**Example:**
- 60 messages lag across 3 partitions
- `60 / (3 × 10) = 2 replicas`

---

## 🔍 Monitoring During Tests

### Real-time Pod Monitoring

```bash
# Watch all pods in purchase-system namespace
watch -n 1 'kubectl get pods -n purchase-system'

# Watch deployment scaling
watch -n 1 'kubectl get deployment -n purchase-system'
```

### View Live Logs

```bash
# Frontend logs
kubectl logs -n purchase-system -l app=frontend -f

# Web Server logs
kubectl logs -n purchase-system -l app=web-server -f

# Management API logs (consumer + API)
kubectl logs -n purchase-system -l app=management-api -f
```

### Check Resource Usage

```bash
# CPU and memory usage
kubectl top pods -n purchase-system

# Node resource usage
kubectl top nodes
```

### Query MongoDB Directly

**Access MongoDB shell:**
```bash
kubectl exec -it -n mongodb mongodb-0 -c mongod -- \
  mongosh -u admin -p password --authenticationDatabase admin purchases_db
```

**Common queries inside mongosh:**
```javascript
// Count total purchases
db.purchases.countDocuments()

// View recent purchases
db.purchases.find().sort({timestamp: -1}).limit(10)

// Find purchases for specific user
db.purchases.find({user_id: "user123"})

// Check indexes
db.purchases.getIndexes()

// Clear all data (caution!)
db.purchases.deleteMany({})
```

**Quick one-liner (without entering shell):**
```bash
kubectl exec -n mongodb mongodb-0 -c mongod -- \
  mongosh -u admin -p password --authenticationDatabase admin purchases_db \
  --quiet --eval "db.purchases.countDocuments()"
```

---

## 📝 Test Checklist

Before marking testing as complete, verify:

- [ ] UI loads successfully at http://purchase.localhost:8080
- [ ] Can submit purchase via UI form
- [ ] Can query purchases via UI search
- [ ] POST /api/buy returns 200 OK
- [ ] GET /api/purchases/{user_id} returns correct data
- [ ] Load test triggers KEDA autoscaling (1 → 2+ replicas)
- [ ] Management API scales back down after cooldown (60s)
- [ ] Messages are consumed from Kafka (lag decreases)
- [ ] Purchases are stored in MongoDB
- [ ] Consumer group lag is visible in Kafka

---

## 🎯 Test Scenarios

### Scenario 1: Happy Path
1. Submit purchase via API
2. Verify 200 OK response
3. Query purchases - should return data
4. Check MongoDB - record exists
5. **Expected**: Full success

### Scenario 2: High Load
1. Run load test (100+ requests)
2. Monitor pod count
3. Check consumer lag
4. Wait for processing
5. **Expected**: Scales to 2-3 replicas, lag resolves

### Scenario 3: Consumer Failure
1. Delete Management API pod
2. Submit purchases
3. Wait for pod restart
4. Check if messages processed
5. **Expected**: Kafka retains messages, processed after restart

---

## 📚 Additional Testing Resources

- [Kafka Consumer Groups](https://kafka.apache.org/documentation/#consumergroups)
- [KEDA Scalers](https://keda.sh/docs/scalers/)
- [Kubernetes Load Testing](https://kubernetes.io/docs/tasks/debug-application-cluster/resource-usage-monitoring/)

---

**Happy Testing! 🧪**
