# KEDA consumer scaling

`consumer-scaledobject.yaml` scales the `consumer` deployment in namespace `app` based on total Kafka consumer lag for group `event-processors-k8s` on topic `events-v2`.

- **lagThreshold** — default `500`; desired replicas ≈ `totalLag / lagThreshold` (see [KEDA Kafka scaler](https://keda.sh/docs/latest/scalers/apache-kafka/)).
- **scaleDown** — HPA `stabilizationWindowSeconds: 0` so scale-down is not delayed by Kubernetes’ default 5-minute window.
- **maxReplicaCount** — capped at 6 to match the topic partition count.

Tune `lagThreshold`, `pollingInterval`, and `cooldownPeriod` if scaling is too slow or too aggressive. Values must stay aligned with [`k8s/configmap.yaml`](../configmap.yaml).
