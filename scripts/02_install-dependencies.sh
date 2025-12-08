#!/bin/bash
# install-dependencies.sh
# Installs infrastructure components via Helm
#
# USAGE: Run from project root directory
#   ./scripts/02_install-dependencies.sh
#
# Note: We use Helm for infrastructure because:
# 1. Reproducible deployments
# 2. Easy upgrades with helm upgrade
# 3. Values files for environment-specific config
# 4. Dependency management between charts
#
# Configuration files are in infrastructure/ directory:
# - infrastructure/kafka.yaml (Strimzi Kafka with KRaft)
# - infrastructure/mongodb.yaml (MongoDB Community Operator)

set -e

# Get script directory and change to project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_ROOT"

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Spinner function to show progress
spinner() {
    local pid=$1
    local delay=0.1
    local spinstr='⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏'
    while ps -p $pid > /dev/null 2>&1; do
        local temp=${spinstr#?}
        printf " [%c]  " "$spinstr"
        local spinstr=$temp${spinstr%"$temp"}
        sleep $delay
        printf "\b\b\b\b\b\b"
    done
    printf "    \b\b\b\b"
}

# Function to run command with spinner
run_with_spinner() {
    local message=$1
    shift
    echo -n "$message"

    # Run command in background
    "$@" > /tmp/helm-install.log 2>&1 &
    local pid=$!

    # Show spinner while command runs
    spinner $pid

    # Wait for command to complete and get exit status
    wait $pid
    local status=$?

    if [ $status -eq 0 ]; then
        echo -e " ${GREEN}✓${NC}"
    else
        echo -e " ${YELLOW}⚠${NC}"
        echo "Check /tmp/helm-install.log for details"
    fi

    return $status
}

echo "=========================================="
echo -e "${BLUE}Installing Infrastructure Dependencies${NC}"
echo "=========================================="
echo ""

echo -e "${BLUE}Step 1/4: Adding Helm repositories${NC}"
echo ""

# Strimzi - Kafka operator (production-ready, open-source)
helm repo add strimzi https://strimzi.io/charts/ 2>/dev/null || echo "  Strimzi repo already exists"

# MongoDB Community Operator - official MongoDB operator
helm repo add mongodb https://mongodb.github.io/helm-charts 2>/dev/null || echo "  MongoDB repo already exists"

# KEDA - Kubernetes Event-Driven Autoscaling
helm repo add kedacore https://kedacore.github.io/charts 2>/dev/null || echo "  KEDA repo already exists"

echo -n "  Updating Helm repositories..."
helm repo update > /dev/null 2>&1 && echo -e " ${GREEN}✓${NC}"

echo ""
echo -e "${BLUE}Step 2/4: Installing Kafka with Strimzi (this may take 3-5 minutes)${NC}"
echo "  - Using Strimzi Kafka Operator (production-ready)"
echo "  - 1 broker, 3 partitions"
echo "  - PLAINTEXT protocol (no auth)"
echo ""

run_with_spinner "  Installing Strimzi Operator..." \
    helm upgrade --install strimzi-kafka-operator strimzi/strimzi-kafka-operator \
        --namespace kafka \
        --create-namespace \
        --set watchNamespaces='{kafka}' \
        --wait \
        --timeout 10m

echo -n "  Waiting for Strimzi Operator to be ready..."
kubectl wait --for=condition=ready pod -l name=strimzi-cluster-operator -n kafka --timeout=300s > /dev/null 2>&1 && echo -e " ${GREEN}✓${NC}" || echo -e " ${YELLOW}⚠${NC}"

echo -n "  Creating Kafka cluster (KRaft mode)..."
kubectl apply -f infrastructure/kafka.yaml > /dev/null 2>&1 && echo -e " ${GREEN}✓${NC}" || echo -e " ${YELLOW}⚠${NC}"

echo -n "  Waiting for Kafka cluster to be ready (may take 2-3 minutes)..."
kubectl wait kafka/kafka --for=condition=Ready --timeout=600s -n kafka > /dev/null 2>&1 && echo -e " ${GREEN}✓${NC}" || echo -e " ${YELLOW}⚠${NC}"

echo ""
echo -e "${BLUE}Step 3/4: Installing MongoDB with Community Operator (this may take 2-3 minutes)${NC}"
echo "  - Using MongoDB Community Operator (official)"
echo "  - Standalone replica set (1 member)"
echo "  - No authentication (dev mode)"
echo ""

run_with_spinner "  Installing MongoDB Community Operator..." \
    helm upgrade --install mongodb-operator mongodb/community-operator \
        --namespace mongodb \
        --create-namespace \
        --wait \
        --timeout 10m

echo -n "  Waiting for MongoDB Operator to be ready..."
kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=mongodb-kubernetes-operator -n mongodb --timeout=300s > /dev/null 2>&1 && echo -e " ${GREEN}✓${NC}" || echo -e " ${YELLOW}⚠${NC}"

echo -n "  Creating MongoDB cluster..."
kubectl apply -f infrastructure/mongodb.yaml > /dev/null 2>&1 && echo -e " ${GREEN}✓${NC}" || echo -e " ${YELLOW}⚠${NC}"

echo -n "  Waiting for MongoDB cluster to be ready (may take 2-3 minutes)..."
kubectl wait mongodbcommunity/mongodb --for=jsonpath='{.status.phase}'=Running --timeout=600s -n mongodb > /dev/null 2>&1 && echo -e " ${GREEN}✓${NC}" || echo -e " ${YELLOW}⚠${NC}"

echo ""
echo -e "${BLUE}Step 4/4: Installing KEDA (this may take 1-2 minutes)${NC}"
echo "  - Event-driven autoscaling operator"
echo ""

run_with_spinner "  Installing KEDA chart..." \
    helm upgrade --install keda kedacore/keda \
        --namespace keda \
        --create-namespace \
        --wait \
        --timeout 10m

echo -n "  Waiting for KEDA pods to be ready..."
kubectl wait --for=condition=ready pod -l app.kubernetes.io/part-of=keda-operator -n keda --timeout=300s > /dev/null 2>&1 && echo -e " ${GREEN}✓${NC}" || echo -e " ${YELLOW}⚠${NC}"

echo ""
echo "=========================================="
echo -e "${BLUE}Verifying Installations${NC}"
echo "=========================================="
echo ""

echo -e "${BLUE}Kafka pods:${NC}"
kubectl get pods -n kafka
echo ""

echo -e "${BLUE}MongoDB pods:${NC}"
kubectl get pods -n mongodb
echo ""

echo -e "${BLUE}KEDA pods:${NC}"
kubectl get pods -n keda

echo ""
echo "=========================================="
echo -e "${BLUE}Summary${NC}"
echo "=========================================="

KAFKA_STATUS=$(kubectl get pod kafka-kafka-0 -n kafka -o jsonpath='{.status.phase}' 2>/dev/null || echo "Not Found")
MONGODB_STATUS=$(kubectl get pod mongodb-0 -n mongodb -o jsonpath='{.status.phase}' 2>/dev/null || echo "Not Found")
KEDA_STATUS=$(kubectl get pods -n keda -l app.kubernetes.io/name=keda-operator -o jsonpath='{.items[0].status.phase}' 2>/dev/null || echo "Not Found")

if [ "$KAFKA_STATUS" = "Running" ]; then
    echo -e "${GREEN}✓${NC} Kafka:   $KAFKA_STATUS"
else
    echo -e "${YELLOW}⚠${NC} Kafka:   $KAFKA_STATUS"
fi

if [ "$MONGODB_STATUS" = "Running" ]; then
    echo -e "${GREEN}✓${NC} MongoDB: $MONGODB_STATUS"
else
    echo -e "${YELLOW}⚠${NC} MongoDB: $MONGODB_STATUS"
fi

if [ "$KEDA_STATUS" = "Running" ]; then
    echo -e "${GREEN}✓${NC} KEDA:    $KEDA_STATUS"
else
    echo -e "${YELLOW}⚠${NC} KEDA:    $KEDA_STATUS"
fi

echo ""
if [ "$KAFKA_STATUS" = "Running" ] && [ "$MONGODB_STATUS" = "Running" ] && [ "$KEDA_STATUS" = "Running" ]; then
    echo "=========================================="
    echo -e "${GREEN}✅ All dependencies installed successfully!${NC}"
    echo "=========================================="
    echo ""
    echo "Connection details:"
    echo "  Kafka:   kafka-kafka-bootstrap.kafka.svc.cluster.local:9092"
    echo "  MongoDB: mongodb-0.mongodb-svc.mongodb.svc.cluster.local:27017"
    echo ""
    echo "Next step: Build and deploy the application"
    echo "  ./scripts/03_build-and-deploy-local.sh"
else
    echo "=========================================="
    echo -e "${YELLOW}⚠️  Some dependencies may not be ready yet${NC}"
    echo "=========================================="
    echo ""
    echo "Try running:"
    echo "  kubectl get pods -A"
    echo ""
    echo "Wait a minute and check again, or review logs:"
    echo "  kubectl logs -n kafka kafka-kafka-0"
    echo "  kubectl logs -n mongodb mongodb-0"
fi

echo ""
