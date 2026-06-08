#!/usr/bin/env bash
set -e

echo "1. Disable KEDA"
kubectl delete scaledobject consumer-scaler -n event-autoscaling --ignore-not-found

echo "2. Stop producer"
kubectl scale deployment producer -n event-autoscaling --replicas=0

echo "3. Reset consumer resources"
kubectl set resources deployment/consumer \
  -n event-autoscaling \
  --containers=consumer \
  --requests=cpu=250m,memory=256Mi \
  --limits=cpu=500m,memory=512Mi

echo "4. Scale consumer to 1"
kubectl scale deployment consumer -n event-autoscaling --replicas=1

echo "5. Wait for consumer rollout"
kubectl rollout status deployment/consumer -n event-autoscaling --timeout=180s

echo "6. Scale nodegroup to 2"
eksctl scale nodegroup \
  --cluster kafka-keda-eks \
  --region us-east-1 \
  --name apps-ng \
  --nodes 2 \
  --nodes-min 2 \
  --nodes-max 6

echo "7. Waiting for terminating consumer pods to disappear"
while true; do
  COUNT=$(kubectl get pods -n event-autoscaling --no-headers | grep -E '^consumer-' | wc -l)
  if [ "$COUNT" -eq 1 ]; then
    break
  fi
  kubectl get pods -n event-autoscaling
  sleep 5
done

echo "8. Waiting for ASG desired capacity = 2"
while true; do
  DESIRED=$(aws autoscaling describe-auto-scaling-groups \
    --region us-east-1 \
    --query 'AutoScalingGroups[0].DesiredCapacity' \
    --output text)

  if [ "$DESIRED" = "2" ]; then
    break
  fi

  echo "Current desired capacity: $DESIRED"
  sleep 10
done

echo "RESET DONE"
echo
kubectl get pods -n event-autoscaling
echo
kubectl get nodes
echo
aws autoscaling describe-auto-scaling-groups \
  --region us-east-1 \
  --query 'AutoScalingGroups[*].[MinSize,MaxSize,DesiredCapacity]' \
  --output table