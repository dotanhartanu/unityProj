#!/bin/bash
# build-and-deploy-local.sh
# Local build and deployment script - simulates CI/CD pipeline locally
#
# Note: This script does what GitHub Actions does, but locally:
# 1. Build Docker images
# 2. Import to k3d cluster
# 3. Deploy with Helm
#
# Use this for local development/testing without pushing to Docker Hub

set -e  # Exit on error

CLUSTER_NAME="unity-assignment"
IMAGE_TAG="${IMAGE_TAG:-local}"  # Use 'local' tag by default, or override with env var

echo "=========================================="
echo "Local Build & Deploy Script"
echo "=========================================="
echo "Cluster: $CLUSTER_NAME"
echo "Image Tag: $IMAGE_TAG"
echo ""

# Color output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if cluster exists
if ! k3d cluster list | grep -q "$CLUSTER_NAME"; then
    echo -e "${YELLOW}Cluster $CLUSTER_NAME not found!${NC}"
    echo "Run ./scripts/setup-k3d.sh first"
    exit 1
fi

# Check if infrastructure is installed
if ! kubectl get namespace kafka &>/dev/null; then
    echo -e "${YELLOW}Infrastructure not found!${NC}"
    echo "Run ./scripts/install-dependencies.sh first"
    exit 1
fi

echo -e "${BLUE}Step 1: Building Docker images${NC}"
echo "=========================================="

# Build frontend
echo "Building frontend..."
docker build -t purchase-frontend:${IMAGE_TAG} apps/frontend

# Build web-server
echo "Building web-server..."
docker build -t purchase-web-server:${IMAGE_TAG} apps/web-server

# Build management-api
echo "Building management-api..."
docker build -t purchase-management-api:${IMAGE_TAG} apps/management-api

echo -e "${GREEN}✓ All images built successfully${NC}"
echo ""

echo -e "${BLUE}Step 2: Importing images to k3d cluster${NC}"
echo "=========================================="

k3d image import \
    purchase-frontend:${IMAGE_TAG} \
    purchase-web-server:${IMAGE_TAG} \
    purchase-management-api:${IMAGE_TAG} \
    -c ${CLUSTER_NAME}

echo -e "${GREEN}✓ Images imported to cluster${NC}"
echo ""

echo -e "${BLUE}Step 3: Deploying with Helm${NC}"
echo "=========================================="

helm upgrade --install purchase-system ./helm/purchase-system \
    --namespace purchase-system \
    --create-namespace \
    --set frontend.image.repository=purchase-frontend \
    --set frontend.image.tag=${IMAGE_TAG} \
    --set webServer.image.repository=purchase-web-server \
    --set webServer.image.tag=${IMAGE_TAG} \
    --set managementApi.image.repository=purchase-management-api \
    --set managementApi.image.tag=${IMAGE_TAG} \
    --set global.imagePullPolicy=Never \
    --wait

echo -e "${GREEN}✓ Application deployed successfully${NC}"
echo ""

echo -e "${BLUE}Step 4: Checking deployment status${NC}"
echo "=========================================="

echo "Pods:"
kubectl get pods -n purchase-system

echo ""
echo "Services:"
kubectl get services -n purchase-system

echo ""
echo "Ingress:"
kubectl get ingress -n purchase-system

echo ""
echo "=========================================="
echo -e "${GREEN}Deployment Complete!${NC}"
echo "=========================================="
echo ""
echo "Access the application at:"
echo "  http://purchase.localhost:8080"
echo ""
echo "Make sure /etc/hosts has:"
echo "  127.0.0.1 purchase.localhost"
echo ""
echo "Useful commands:"
echo "  kubectl get pods -n purchase-system -w   # Watch pods"
echo "  kubectl logs -n purchase-system -l app=web-server -f  # View logs"
echo "  helm list -n purchase-system              # List releases"
echo "  helm uninstall purchase-system -n purchase-system  # Uninstall"
echo ""
