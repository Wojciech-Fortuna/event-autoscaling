#!/usr/bin/env bash

while true; do
  clear

  echo "============================================================"
  echo " EVENT AUTOSCALING DEMO - KAFKA / KEDA / EKS"
  echo " $(date)"
  echo "============================================================"
  echo

  echo "1) HPA / KEDA"
  echo "------------------------------------------------------------"
  kubectl get hpa -n event-autoscaling 2>/dev/null || echo "No HPA"
  echo

  echo "2) ScaledObject"
  echo "------------------------------------------------------------"
  kubectl get scaledobject -n event-autoscaling 2>/dev/null || echo "No ScaledObject"
  echo

  echo "3) Deployments"
  echo "------------------------------------------------------------"
  kubectl get deploy -n event-autoscaling
  echo

  echo "4) Pods"
  echo "------------------------------------------------------------"
  kubectl get pods -n event-autoscaling -o wide
  echo

  echo "5) Pending Pods"
  echo "------------------------------------------------------------"
  kubectl get pods -A --field-selector=status.phase=Pending
  echo

  echo "6) Nodes"
  echo "------------------------------------------------------------"
  kubectl get nodes
  echo

  echo "7) AWS Auto Scaling Group"
  echo "------------------------------------------------------------"
  aws autoscaling describe-auto-scaling-groups \
    --region us-east-1 \
    --query 'AutoScalingGroups[*].[AutoScalingGroupName,MinSize,MaxSize,DesiredCapacity]' \
    --output table
  echo

  echo "8) Kafka Consumer Lag"
  echo "------------------------------------------------------------"
  kubectl exec -n kafka kafka-0 -- \
    /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 \
    --describe \
    --group event-processors-autoscaled-test-2 2>/dev/null \
    | awk '
      NR == 1 { print $0; next }
      $2 == "events-test-2" {
        total += $6
        count += 1
      }
      END {
        if (count > 0) {
          print "TOTAL_LAG:", total
          print "PARTITIONS:", count
        } else {
          print "No lag data yet"
        }
      }'
  echo

  echo "Refresh: 5s | Stop: Ctrl+C"
  sleep 5
done