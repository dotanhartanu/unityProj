#!/bin/bash
# setup-k3d.sh
# Creates a k3d cluster with ingress support
#
# Note: k3d runs k3s (lightweight K8s) inside Docker containers.
# k3s includes Traefik ingress controller by default.

set -e  # Exit on error

CLUSTER_NAME="unity-assignment"

# Check if cluster exists
if k3d cluster list | grep -q "$CLUSTER_NAME"; then
    echo "Cluster $CLUSTER_NAME already exists. Deleting..."
    k3d cluster delete "$CLUSTER_NAME"
fi

# Create cluster with:
# - 1 server (control plane) node
# - 2 agent (worker) nodes - good for testing pod distribution
# - Port 8080 mapped to Traefik ingress HTTP (80)
# - Port 8443 mapped to Traefik ingress HTTPS (443)
#
# Note: Why these port mappings?
# - @loadbalancer targets the built-in load balancer service
# - Maps host ports (8080/8443) to container ports (80/443)
# - Traefik ingress controller listens on these ports inside the cluster

k3d cluster create "$CLUSTER_NAME" \
    --servers 1 \
    --agents 2 \
    --port "8080:80@loadbalancer" \
    --port "8443:443@loadbalancer" \
    --wait

echo "Cluster created successfully!"
echo ""
echo "Traefik ingress controller is available at:"
echo "  - HTTP:  http://localhost:8080"
echo "  - HTTPS: https://localhost:8443"
echo ""
echo "Add your ingress hostname to /etc/hosts:"
echo "  127.0.0.1 purchase.localhost"
echo ""
kubectl cluster-info
