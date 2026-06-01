# Event Autoscaling with Kafka and Kubernetes

A simple event processing system used to demonstrate autoscaling of Kafka consumers in Kubernetes.

## Architecture

```text
Producer
    ↓
Kafka
    ↓
Consumer Group
```

### Components

- **Producer** generates events at a configurable rate.
- **Kafka** stores events and distributes them across partitions.
- **Consumer** processes events with configurable processing delay.
- **Consumer Group** enables parallel processing across Kafka partitions.

---

## Prerequisites

- Docker
- Kubernetes (tested with k3d)
- kubectl

---

## Build Application Image

Build the application image:

```bash
docker build -t event-app:latest .
```

For k3d environments:

```bash
k3d image import event-app:latest -c beyla-lab
```

---

## Deploy Kafka

### Create Namespace

```bash
kubectl create namespace kafka
```

### Deploy Kafka

```bash
kubectl apply -f k8s/kafka.yaml
```

### Wait Until Kafka Is Running

```bash
kubectl get pods -n kafka -w
```

### Create Topic

```bash
kubectl exec -n kafka kafka-0 -- \
/opt/kafka/bin/kafka-topics.sh \
  --create \
  --if-not-exists \
  --topic events-v2 \
  --partitions 6 \
  --replication-factor 1 \
  --bootstrap-server localhost:9092
```

### Verify Topic

```bash
kubectl exec -n kafka kafka-0 -- \
/opt/kafka/bin/kafka-topics.sh \
  --describe \
  --topic events-v2 \
  --bootstrap-server localhost:9092
```

---

## Deploy Application

### Create Namespace

```bash
kubectl create namespace app
```

### Apply Configuration

```bash
kubectl apply -f k8s/configmap.yaml
```

### Deploy Producer

```bash
kubectl apply -f k8s/producer-deployment.yaml
```

### Deploy Consumer

```bash
kubectl apply -f k8s/consumer-deployment.yaml
```

### Verify Pods

```bash
kubectl get pods -n app
```

---

## Monitoring

### Producer Logs

```bash
kubectl logs -n app -l app=producer -f
```

### Consumer Logs

```bash
kubectl logs -n app -l app=consumer -f
```

---

## Manual Consumer Scaling

### Scale Consumers

```bash
kubectl scale deployment consumer \
  -n app \
  --replicas=3
```

### Verify Scaling

```bash
kubectl get pods -n app -l app=consumer
```

---

## Configuration

Application configuration is stored in:

```text
k8s/configmap.yaml
```

### Available Parameters

| Variable | Description |
|-----------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka bootstrap server |
| `TOPIC_NAME` | Kafka topic |
| `CONSUMER_GROUP` | Consumer group name |
| `MESSAGES_PER_SECOND` | Normal producer throughput |
| `BURST_MESSAGES_PER_SECOND` | Burst throughput |
| `BURST_DURATION_SECONDS` | Burst duration |
| `MESSAGE_SIZE_BYTES` | Event payload size |
| `PROCESSING_DELAY_MS` | Consumer processing delay |

---

## Current Status

- Kafka deployed in Kubernetes
- Producer deployed in Kubernetes
- Consumer deployed in Kubernetes
- Kafka topic partitioned for parallel processing
- Consumer group processing events in parallel
