# Infrastructure Configuration

This directory contains Kubernetes manifest files for infrastructure components that are installed separately from the application.

## Files

### `kafka.yaml` - Strimzi Kafka Operator

**What it provides:**
- Apache Kafka cluster using Strimzi Operator
- KRaft mode (Kafka Raft) - no ZooKeeper required
- Production-ready operator from Red Hat/IBM

**Configuration:**
- **Kafka Version**: 4.0.0 (required by Strimzi 0.46+)
- **Mode**: KRaft (replaces ZooKeeper)
- **Brokers**: 1 node (combined broker + controller roles)
- **Partitions**: 3 (for topic `purchases`)
- **Protocol**: PLAINTEXT (no authentication - dev only)
- **Storage**: 10Gi persistent volume per broker
- **Namespace**: `kafka`

**Services Created:**
- `kafka-kafka-bootstrap.kafka.svc.cluster.local:9092` - Client connections

**Resources:**
- KafkaNodePool: Defines broker nodes with combined broker+controller roles
- Kafka: Main cluster configuration
- KafkaTopic: Creates `purchases` topic with 3 partitions

---

### `mongodb.yaml` - MongoDB Community Operator

**What it provides:**
- MongoDB database using MongoDB Community Operator
- Official operator from MongoDB Inc.
- Production-ready deployment pattern

**Configuration:**
- **MongoDB Version**: 6.0.5
- **Deployment**: Single-member ReplicaSet (scalable to 3+ later)
- **Storage**: 10Gi persistent volume
- **Authentication**: Enabled (username: admin, password via Secret)
- **Namespace**: `mongodb`

**Services Created:**
- `mongodb-0.mongodb-svc.mongodb.svc.cluster.local:27017` - Database connection

**Resources:**
- Secret: `mongodb-admin-password` (credentials)
- MongoDBCommunity: Database cluster definition

**Why ReplicaSet with 1 member?**
- MongoDB Community Operator requires ReplicaSet type
- Provides consistent API for scaling up later
- Supports transaction features even with single member

---

### `prometheus-values.yaml` - Prometheus Monitoring Stack

**What it provides:**
- Prometheus Operator and Prometheus server
- node-exporter for node-level metrics
- kube-state-metrics for Kubernetes object metrics
- Alertmanager for alert management

**Configuration:**
- **Scrape Interval**: 30 seconds
- **Retention Period**: 7 days
- **Storage**: 10Gi persistent volume
- **Namespace**: `monitoring`
- **Resource Limits**: 1 CPU, 4Gi memory (Prometheus); 200m CPU, 256Mi memory (Alertmanager)

**Services Created:**
- `prometheus-kube-prometheus-prometheus.monitoring.svc.cluster.local:9090` - Prometheus API

**Key Features:**
- Automatic service discovery via ServiceMonitors
- Monitors application, Kafka, and MongoDB metrics
- Pre-configured node and cluster metrics collection

---

### `grafana-values.yaml` - Grafana Visualization

**What it provides:**
- Grafana dashboard and visualization platform
- Pre-configured Prometheus datasource
- Ingress-enabled for external access

**Configuration:**
- **Admin Credentials**: admin/admin ⚠️ **CHANGE FOR PRODUCTION**
- **Storage**: 5Gi persistent volume
- **Ingress Path**: `/grafana`
- **Namespace**: `monitoring`
- **Resource Limits**: 500m CPU, 1Gi memory

**Access:**
- URL: `http://purchase.localhost:8080/grafana`
- Exposed via Traefik ingress controller
- Configured for subpath hosting

**Key Features:**
- Auto-loading dashboards via sidecar
- Pre-configured Kubernetes and Kafka dashboards
- Path-based routing with ingress

---

### `servicemonitors.yaml` - Prometheus Service Discovery

**What it provides:**
- ServiceMonitor CRDs for automatic metrics collection
- Configures Prometheus scrape targets

**Monitors:**
- **web-server-monitor**: Web server FastAPI metrics (`/metrics`)
- **management-api-monitor**: Management API FastAPI metrics (`/metrics`)
- **kafka-monitor**: Kafka broker metrics via JMX exporter
- **mongodb-exporter-monitor**: MongoDB metrics via Percona exporter

---

### `grafana-dashboards.yaml` - Pre-configured Dashboards

**What it provides:**
- ConfigMaps with dashboard definitions
- Auto-loaded into Grafana via sidecar

**Dashboards:**
- Dashboard configuration removed - use community dashboards instead
- **Recommended imports:**
  - Kubernetes Cluster Monitoring (ID: 315)
  - Kubernetes Namespace Pods (ID: 6417)
  - Kafka Overview (ID: 7589)
  - MongoDB Dashboard (ID: 2583)
  - FastAPI Observability (ID: 16110)

---

## Installation

These resources are automatically installed by the setup script:

```bash
./scripts/02_install-dependencies.sh
```

The script:
1. Installs Strimzi Operator via Helm
2. Applies `kafka.yaml` to create Kafka cluster
3. Installs MongoDB Community Operator via Helm
4. Applies `mongodb.yaml` to create MongoDB database
5. Installs KEDA operator for autoscaling
6. Installs Prometheus stack with ServiceMonitors
7. Installs Grafana with ingress at `/grafana`

**Manual installation:**

```bash
# Install Strimzi Operator
helm repo add strimzi https://strimzi.io/charts/
helm install strimzi-kafka-operator strimzi/strimzi-kafka-operator \
  --namespace kafka --create-namespace

# Apply Kafka configuration
kubectl apply -f infrastructure/kafka.yaml

# Install MongoDB Community Operator
helm repo add mongodb https://mongodb.github.io/helm-charts
helm install mongodb-operator mongodb/community-operator \
  --namespace mongodb --create-namespace

# Apply MongoDB configuration
kubectl apply -f infrastructure/mongodb.yaml

# Deploy MongoDB exporter
kubectl apply -f infrastructure/mongodb-exporter.yaml

# Install Prometheus stack
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm install prometheus prometheus-community/kube-prometheus-stack \
  --namespace monitoring --create-namespace \
  --values infrastructure/prometheus-values.yaml

# Create ServiceMonitors
kubectl apply -f infrastructure/servicemonitors.yaml

# Install Grafana
helm repo add grafana https://grafana.github.io/helm-charts
helm install grafana grafana/grafana \
  --namespace monitoring \
  --values infrastructure/grafana-values.yaml

# Create Grafana dashboards
kubectl apply -f infrastructure/grafana-dashboards.yaml
```

---

## Verification

**Check Kafka:**
```bash
# Check pods
kubectl get pods -n kafka

# Check Kafka cluster status
kubectl get kafka -n kafka

# List topics
kubectl exec -n kafka kafka-broker-0 -- /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
```

**Check MongoDB:**
```bash
# Check pods
kubectl get pods -n mongodb

# Check MongoDB status
kubectl get mongodbcommunity -n mongodb

# Connect to MongoDB
kubectl exec -it -n mongodb mongodb-0 -c mongod -- \
  mongosh -u admin -p password --authenticationDatabase admin
```

**Check Monitoring:**
```bash
# Check monitoring pods
kubectl get pods -n monitoring

# Check Prometheus
kubectl get prometheus -n monitoring

# Check ServiceMonitors
kubectl get servicemonitors -n monitoring

# Access Grafana (via ingress)
# Open: http://purchase.localhost:8080/grafana
# Username: admin
# Password: admin

# Access Prometheus (via port-forward)
kubectl port-forward -n monitoring svc/prometheus-kube-prometheus-prometheus 9090:9090
# Open: http://localhost:9090
```

---

## Architecture Decisions

### Why Strimzi instead of Bitnami Kafka?

**Strimzi Advantages:**
- Official CNCF project (Cloud Native Computing Foundation)
- Production-ready with enterprise support
- KRaft mode support (ZooKeeper-free)
- Better Kubernetes integration (native CRDs)
- Active development and updates

### Why MongoDB Community Operator instead of Bitnami MongoDB?

**MongoDB Community Operator Advantages:**
- Official operator from MongoDB Inc.
- Better production support and documentation
- Native MongoDB features (replica sets, sharding)
- More flexible configuration
- Regular updates aligned with MongoDB releases

### Why kube-prometheus-stack instead of standalone Prometheus?

**kube-prometheus-stack Advantages:**
- Includes Prometheus Operator for CRD-based configuration
- Bundles node-exporter and kube-state-metrics
- Pre-configured for Kubernetes monitoring
- ServiceMonitor CRDs for automatic service discovery
- Production-ready with best practices

### Why Grafana with Ingress instead of Port-Forward?

**Ingress Advantages:**
- Persistent access without manual port-forwarding
- Path-based routing (`/grafana`) allows multiple services on same host
- Production-ready pattern for external access
- Traefik integration included with k3d
- Easier to share dashboards with team members

### Why Separate Namespaces?

**Benefits:**
- **Isolation**: Infrastructure failures don't affect application
- **RBAC**: Different access controls for infra vs app teams
- **Lifecycle**: Upgrade infra independently from application
- **Resource Quotas**: Separate resource limits per namespace
- **Monitoring**: Easier to track resource usage per layer

### Why Prometheus + Grafana Stack?

**Prometheus Advantages:**
- Industry-standard for Kubernetes monitoring
- Pull-based metrics collection (ServiceMonitors)
- PromQL for powerful queries and alerting
- Native Kubernetes integration via Operator
- Includes kube-state-metrics and node-exporter for comprehensive cluster monitoring

**Grafana Advantages:**
- Rich visualization capabilities
- Dashboard as code (ConfigMaps)
- Multiple datasource support
- Community dashboards available
- Alert visualization and management

**kube-prometheus-stack Benefits:**
- Bundles Prometheus Operator, Prometheus, node-exporter, kube-state-metrics
- Production-ready configuration out-of-the-box
- CRDs for ServiceMonitors and PrometheusRules
- Regular updates and active community

---

## Security Notes

⚠️ **Development/Demo Only**

Current setup prioritizes simplicity over security:
- **Kafka**: PLAINTEXT (no auth/TLS)
- **MongoDB**: Basic auth with simple password in Secret
- **Grafana**: Default admin/admin credentials ⚠️
- **MongoDB Exporter**: Password in deployment manifest
- No network policies or pod security standards

### Production Security Checklist

Before deploying to production:

1. **Grafana Credentials**
   ```bash
   # Create secure password in Secret
   kubectl create secret generic grafana-admin \
     --from-literal=admin-password=$(openssl rand -base64 32) \
     -n monitoring

   # Update grafana-values.yaml to reference secret
   # admin:
   #   existingSecret: grafana-admin
   #   passwordKey: admin-password
   ```

2. **MongoDB Credentials**
   - Use external secret management (Vault, AWS Secrets Manager)
   - Rotate credentials regularly
   - Use separate credentials for exporter

3. **Kafka Security**
   - Enable TLS encryption
   - Configure SASL authentication
   - Implement ACLs for topic access

4. **Network Policies**
   - Restrict pod-to-pod communication
   - Limit ingress to monitoring namespace

5. **Ingress Security**
   - Enable TLS/HTTPS
   - Add authentication (OAuth, LDAP)
   - Use cert-manager for certificate management

See main [README.md](../README.md#-security-note) for production security checklist.

---

## Troubleshooting

**Kafka pod stuck in Pending:**
```bash
kubectl describe pod -n kafka kafka-broker-0
# Check: Insufficient resources, PVC binding issues
```

**MongoDB pod CrashLoopBackOff:**
```bash
kubectl logs -n mongodb mongodb-0 -c mongod
# Common: Secret not found, insufficient storage
```

**Topic not created:**
```bash
kubectl get kafkatopic -n kafka
kubectl describe kafkatopic purchases -n kafka
```

---

## Cleanup

To remove all infrastructure:

```bash
# Delete custom resources first (important!)
kubectl delete kafka kafka -n kafka
kubectl delete mongodbcommunity mongodb -n mongodb

# Uninstall operators and monitoring
helm uninstall strimzi-kafka-operator -n kafka
helm uninstall mongodb-operator -n mongodb
helm uninstall keda -n keda
helm uninstall prometheus -n monitoring
helm uninstall grafana -n monitoring

# Delete PVCs
kubectl delete pvc -n kafka --all
kubectl delete pvc -n mongodb --all
kubectl delete pvc -n monitoring --all

# Delete namespaces
kubectl delete namespace kafka mongodb keda monitoring
```

---

## Additional Resources

- [Strimzi Documentation](https://strimzi.io/docs/operators/latest/overview)
- [MongoDB Community Operator](https://github.com/mongodb/mongodb-kubernetes-operator)
- [KEDA Documentation](https://keda.sh/docs/)
- [Kafka KRaft Mode](https://kafka.apache.org/documentation/#kraft)
- [kube-prometheus-stack](https://github.com/prometheus-community/helm-charts/tree/main/charts/kube-prometheus-stack)
- [Grafana Documentation](https://grafana.com/docs/grafana/latest/)
- [Prometheus Operator](https://prometheus-operator.dev/)
