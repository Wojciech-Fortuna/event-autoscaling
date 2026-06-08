#!/usr/bin/env bash
set -e

echo "============================================================"
echo "STARTING DEMO"
echo "============================================================"

echo
echo "1. Enabling KEDA ScaledObject"
kubectl apply -f k8s/keda/consumer-scaledobject.yaml

echo
echo "2. Starting producer"
kubectl scale deployment producer -n event-autoscaling --replicas=1

echo
echo "3. Restarting producer to trigger burst"
kubectl rollout restart deployment/producer -n event-autoscaling

echo
echo "4. Waiting for producer rollout"
kubectl rollout status deployment/producer -n event-autoscaling --timeout=120s

echo
echo "5. Current state"
kubectl get hpa -n event-autoscaling || true
echo
kubectl get pods -n event-autoscaling
echo
kubectl get nodes

echo
echo "DEMO STARTED"
echo "Now watch ./demo/02-watch-demo.sh"