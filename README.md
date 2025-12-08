# Purchase Tracking System - Unity DevOps Assignment

A scalable, event-driven purchase tracking system demonstrating modern DevOps practices with Kubernetes, Kafka, and FastAPI.

## 🏗️ Architecture

```
┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐
│ Browser  │────▶│ Frontend │────▶│   Web    │────▶│  Kafka   │────▶│   Mgmt   │────▶│ MongoDB  │
│  (User)  │     │  Nginx   │     │  Server  │     │ 3 Part.  │     │   API    │     │ Database │
└──────────┘     └──────────┘     └──────────┘     └──────────┘     └──────────┘     └──────────┘
                       │                 │                                 │
                       │                 └─────────GET /purchases/────────┘
                       │
                       └──────Serves static HTML + Proxies /api/* requests

Flow:
1. POST /buy    → Web Server → Kafka (async write)
2. GET /purchases → Web Server → Management API → MongoDB (sync read)
3. Background: Management API consumes Kafka → MongoDB (async persistence)
```

### Components

| Component | Technology | Purpose | Replicas |
|-----------|-----------|---------|----------|
| **Frontend** | Nginx | Serves static HTML/CSS/JS, proxies API calls | 1 |
| **Web Server** | FastAPI | Handles `/buy` (produces to Kafka), `/purchases` (queries Mgmt API) | 2 |
| **Management API** | FastAPI + Kafka Consumer | Consumes messages, stores in MongoDB, serves queries | 1-3 (KEDA) |
| **Kafka** | Strimzi Operator | Message broker (3 partitions), production-ready operator | 1 |
| **MongoDB** | MongoDB Community Operator | Purchase data storage, official MongoDB operator | 1 |
| **KEDA** | KEDA Operator | Autoscales Management API based on Kafka lag | - |

### Key Design Decisions

**1. Separate Frontend from Backend**
- Independent scaling (nginx needs fewer resources)
- Independent deployment (update HTML without redeploying APIs)
- Production best practice

**2. Kafka for Writes, HTTP for Reads**
- **Writes**: Async via Kafka (fast user response, reliable delivery, scalable)
- **Reads**: Sync via HTTP (user expects immediate data)
- Different patterns for different requirements

**3. KEDA Autoscaling**
- Scales Management API based on Kafka consumer lag
- Ensures message processing keeps up with production rate
- Max 3 replicas (matches 3 Kafka partitions for optimal distribution)

**4. Separate Namespaces**
- Infrastructure (kafka, mongodb, keda) in separate namespaces
- Application (purchase-system) in own namespace
- Mirrors production separation of concerns

---

## 🚀 Quick Start

### Prerequisites

**Docker:**
- macOS: [Docker Desktop](https://www.docker.com/products/docker-desktop)
- Ubuntu/Linux: [Install Docker Engine](https://docs.docker.com/engine/install/ubuntu/)

**k3d:**
- macOS: `brew install k3d`
- Ubuntu/Linux: `wget -q -O - https://raw.githubusercontent.com/k3d-io/k3d/main/install.sh | bash`
- Or see [k3d installation](https://k3d.io/#installation)

**kubectl:**
- macOS: `brew install kubectl`
- Ubuntu/Linux: `sudo snap install kubectl --classic` or `sudo apt-get install kubectl -y`

**Helm 3:**
- macOS: `brew install helm`
- Ubuntu/Linux: `curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash`

### Complete Setup (3 commands)

```bash
# 1. Create k3d cluster
./scripts/01_setup-k3d.sh

# 2. Install infrastructure (Kafka, MongoDB, KEDA)
./scripts/02_install-dependencies.sh

# 3. Build and deploy application
./scripts/03_build-and-deploy-local.sh
```

### Access the Application

```bash
# Add to /etc/hosts
echo "127.0.0.1 purchase.localhost" | sudo tee -a /etc/hosts

# Open browser
# macOS:
open http://purchase.localhost:8080

# Ubuntu/Linux:
xdg-open http://purchase.localhost:8080
# Or just open in browser manually: http://purchase.localhost:8080
```

---

## 📋 Detailed Setup

### Step 1: Create k3d Cluster

```bash
./scripts/01_setup-k3d.sh
```

**What this does:**
- Creates k3d cluster named "unity-assignment"
- 1 server node (control plane)
- 2 agent nodes (workers)
- Port 8080 → Traefik ingress HTTP
- Port 8443 → Traefik ingress HTTPS

**Verify:**
```bash
kubectl cluster-info
kubectl get nodes
```

### Step 2: Install Infrastructure

```bash
./scripts/02_install-dependencies.sh
```

**What this installs:**
- **Kafka** (namespace: kafka) - 3 partitions, PLAINTEXT protocol
- **MongoDB** (namespace: mongodb) - Standalone, no auth
- **KEDA** (namespace: keda) - Autoscaling operator

**Verify:**
```bash
kubectl get pods -n kafka
kubectl get pods -n mongodb
kubectl get pods -n keda
```

### Step 3: Build and Deploy Application

```bash
./scripts/03_build-and-deploy-local.sh
```

**What this does:**
1. Builds 3 Docker images (frontend, web-server, management-api)
2. Imports images to k3d cluster
3. Deploys via Helm to `purchase-system` namespace
4. Waits for pods to be ready

**Verify:**
```bash
kubectl get pods -n purchase-system
kubectl get svc -n purchase-system
kubectl get ingress -n purchase-system
```

---

## 🧪 Testing

For comprehensive testing documentation including:
- Manual UI and API testing
- Load testing scripts and tools
- KEDA autoscaling validation
- Kafka consumer lag monitoring
- Performance benchmarks
- Troubleshooting guide

**See the [test/README.md](test/README.md) for complete testing guide.**

Quick test:
```bash
# Test via UI
open http://purchase.localhost:8080

# Test via API
curl -X POST http://purchase.localhost:8080/api/buy \
  -H "Content-Type: application/json" \
  -d '{"username":"alice","user_id":"user456","item_name":"Magic Potion","price":9.99}'

# Run load test
python test/load-test.py
```

---

## 🔍 Monitoring & Troubleshooting

### View Logs

```bash
# Frontend logs
kubectl logs -n purchase-system -l app=frontend -f

# Web Server logs
kubectl logs -n purchase-system -l app=web-server -f

# Management API logs (consumer + API)
kubectl logs -n purchase-system -l app=management-api -f
```

### Check Pod Status

```bash
# Get all pods with status
kubectl get pods -n purchase-system -o wide

# Describe specific pod
kubectl describe pod -n purchase-system <pod-name>

# Get pod events
kubectl get events -n purchase-system --sort-by='.lastTimestamp'
```

### Verify Services and Endpoints

```bash
# List services
kubectl get svc -n purchase-system

# Check service endpoints (actual pod IPs)
kubectl get endpoints -n purchase-system

# Test internal service connectivity
kubectl run -it --rm debug --image=busybox --restart=Never -- sh
# Inside pod:
wget -O- http://web-server.purchase-system.svc.cluster.local:8000/health
```

### Debug Kafka

```bash
# List topics (Strimzi Kafka)
kubectl exec -n kafka kafka-broker-0 -- /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list

# Describe purchases topic
kubectl exec -n kafka kafka-broker-0 -- /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --topic purchases \
  --describe

# Consume messages manually (see what's in topic)
kubectl exec -n kafka kafka-broker-0 -- /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic purchases \
  --from-beginning \
  --max-messages 10
```

### Debug MongoDB

```bash
# Access MongoDB shell
kubectl exec -it -n mongodb mongodb-0 -c mongod -- \
  mongosh -u admin -p password --authenticationDatabase admin

# Inside mongosh:
use purchases_db
db.purchases.find().limit(10)
db.purchases.countDocuments()
db.purchases.find({user_id: "user123"})

# Quick one-liner to count documents
kubectl exec -n mongodb mongodb-0 -c mongod -- \
  mongosh -u admin -p password --authenticationDatabase admin \
  purchases_db --quiet --eval "db.purchases.countDocuments()"
```

### Common Issues

**Pods stuck in Pending:**
```bash
kubectl describe pod -n purchase-system <pod-name>
# Look for: Insufficient CPU/memory, image pull errors
```

**Pods CrashLoopBackOff:**
```bash
kubectl logs -n purchase-system <pod-name> --previous
# Check previous container logs for startup errors
```

**Can't access via purchase.localhost:**
```bash
# Verify /etc/hosts
cat /etc/hosts | grep purchase.localhost

# Verify ingress
kubectl get ingress -n purchase-system

# Check Traefik logs
kubectl logs -n kube-system -l app.kubernetes.io/name=traefik -f
```

---

## 🔐 Security Note

**⚠️ Development/Demo Only - NOT Production Ready**

For simplicity, this setup uses:
- Kafka: PLAINTEXT (no auth/encryption)
- MongoDB: Basic auth (simple password)
- No TLS/HTTPS
- No network policies or RBAC

**Production checklist:**
- [ ] Kafka SASL/SCRAM + TLS
- [ ] MongoDB strong auth + TLS
- [ ] External secret management (Vault/AWS Secrets Manager)
- [ ] Network policies (restrict pod-to-pod)
- [ ] RBAC with service accounts
- [ ] Image vulnerability scanning
- [ ] Pod security standards

---

## 🔄 CI/CD Pipeline

### GitHub Actions Workflow

Located in `.github/workflows/ci-cd.yaml`

**Triggers:**
- Push to `main` branch (automatic)
- Pull requests (automatic)
- Manual trigger via workflow_dispatch

**Jobs:**
1. **Lint** - Python code quality (flake8) and Helm chart validation
2. **Test** - Run pytest unit tests for web-server and management-api
3. **Build & Push** - Builds and pushes Docker images to Docker Hub
4. **Security Scan** - Scans container images for vulnerabilities (Trivy)

### Setup GitHub Actions

**One-time configuration:**

1. Go to GitHub repository → **Settings** → **Secrets and variables** → **Actions**

2. Add repository secrets:
   ```
   DOCKERHUB_USERNAME: dotanhartanu
   DOCKERHUB_TOKEN: <your-docker-hub-access-token>
   ```

3. Get Docker Hub token:
   - Go to https://hub.docker.com/settings/security
   - Click "New Access Token"
   - Name: `github-actions`
   - Copy token and add to GitHub secrets

### Automatic Deployment

```bash
# Every push to main automatically:
git add .
git commit -m "Update application"
git push origin main

# GitHub Actions will:
# 1. Lint code (Python + Helm)
# 2. Run unit tests
# 3. Build 3 Docker images
# 4. Scan for vulnerabilities
# 5. Push to Docker Hub with tags:
#    - latest
#    - <short-commit-sha>
```

### Manual Deployment Trigger

1. Go to GitHub repository → **Actions** tab
2. Select **CI/CD Pipeline** workflow
3. Click **Run workflow** dropdown
4. Select branch and optionally:
   - Environment (dev/staging/prod)
   - Custom image tag
5. Click **Run workflow**

### Deploy from CI/CD Images

```bash
# Deploy using images built by GitHub Actions
helm upgrade --install purchase-system ./helm/purchase-system \
  --namespace purchase-system \
  --create-namespace \
  --set frontend.image.tag=<commit-sha> \
  --set webServer.image.tag=<commit-sha> \
  --set managementApi.image.tag=<commit-sha>
```

---

## 📦 Project Structure

```
.
├── README.md                          # This file
├── .github/
│   └── workflows/
│       └── ci-cd.yaml                 # GitHub Actions pipeline
├── apps/
│   ├── frontend/                      # Nginx frontend
│   │   ├── Dockerfile
│   │   ├── nginx.conf
│   │   └── static/
│   │       └── index.html
│   ├── web-server/                    # FastAPI web server
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── app.py
│   └── management-api/                # FastAPI + Kafka consumer
│       ├── Dockerfile
│       ├── requirements.txt
│       └── app.py
├── helm/
│   └── purchase-system/               # Helm chart
│       ├── Chart.yaml
│       ├── values.yaml
│       └── templates/
│           ├── _helpers.tpl
│           ├── configmap.yaml
│           ├── frontend-deployment.yaml
│           ├── frontend-service.yaml
│           ├── web-server-deployment.yaml
│           ├── web-server-service.yaml
│           ├── management-api-deployment.yaml
│           ├── management-api-service.yaml
│           ├── keda-scaledobject.yaml
│           └── ingress.yaml
├── infrastructure/                    # Infrastructure manifests
│   ├── README.md                      # Infrastructure docs
│   ├── kafka.yaml                     # Strimzi Kafka config (KRaft mode)
│   └── mongodb.yaml                   # MongoDB Community Operator config
├── test/                              # Testing resources
│   ├── README.md                      # Comprehensive testing guide
│   └── load-test.py                   # Load testing script
└── scripts/
    ├── 01_setup-k3d.sh                # Create k3d cluster
    ├── 02_install-dependencies.sh     # Install Kafka, MongoDB, KEDA
    └── 03_build-and-deploy-local.sh   # Local build & deploy
```

---

## 🧹 Cleanup

### Full Reset (Recommended)

For a clean slate, delete the entire k3d cluster:

```bash
# Delete k3d cluster (removes everything)
k3d cluster delete unity-assignment

# Remove from /etc/hosts
sudo sed -i '' '/purchase.localhost/d' /etc/hosts  # macOS
sudo sed -i '/purchase.localhost/d' /etc/hosts     # Linux
```

Then restart from scratch:
```bash
./scripts/01_setup-k3d.sh
./scripts/02_install-dependencies.sh
./scripts/03_build-and-deploy-local.sh
```

### Partial Cleanup (Application Only)

To keep infrastructure but remove the application:

```bash
# Delete application
helm uninstall purchase-system -n purchase-system
kubectl delete namespace purchase-system

# Infrastructure (Kafka, MongoDB, KEDA) remains intact
# Redeploy with:
./scripts/03_build-and-deploy-local.sh
```

---

## 📚 Additional Resources

- [k3d Documentation](https://k3d.io/)
- [Helm Documentation](https://helm.sh/docs/)
- [KEDA Documentation](https://keda.sh/)
- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [FastAPI Documentation](https://fastapi.tiangolo.com/)

---

## 📝 License

This project is for demonstration and interview purposes.

---

**Built with ❤️ for Unity DevOps Assignment**
