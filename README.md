# Event Autoscaling with Kafka and Kubernetes

A simple event processing system used to demonstrate autoscaling of Kafka consumers in Kubernetes with [KEDA](https://keda.sh/) (consumer lag) and basic monitoring (Prometheus + Grafana).

## Architecture

```text
Producer → Kafka → Consumer group (KEDA scales consumers on lag)
                      ↓
              kafka-exporter → Prometheus → Grafana
```

### Components

- **Producer** — configurable normal load, then burst, then normal again.
- **Kafka** — event buffer (6 partitions on topic `events-v2`).
- **Consumer** — processes events with configurable delay; scaled by KEDA `ScaledObject`.
- **KEDA** — scales the `consumer` deployment from Kafka consumer lag.
- **kafka-exporter** — exposes consumer lag metrics to Prometheus.

---

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/)
- [kubectl](https://kubernetes.io/docs/tasks/tools/)
- [Minikube](https://minikube.sigs.k8s.io/docs/start/)
- [Helm 3](https://helm.sh/docs/intro/install/)

Recommended Minikube resources: **4 CPUs, 8 GB RAM** (Prometheus stack is heavy on smaller profiles).

---

## 1. Start Minikube

```bash
minikube start --cpus=4 --memory=8192
minikube addons enable metrics-server
```

Verify:

```bash
kubectl get nodes
```

---

## 2. Build the application image inside Minikube

The deployments use `event-app:latest` with `imagePullPolicy: IfNotPresent`. Build the image in Minikube’s Docker daemon so nodes can run it without a registry.

**Linux / macOS (bash):**

```bash
eval $(minikube docker-env)
docker build -t event-app:latest .
```

**Windows (PowerShell):**

```powershell
& minikube -p minikube docker-env --shell powershell | Invoke-Expression
docker build -t event-app:latest .
```

To use your host Docker again later:

```bash
eval $(minikube docker-env -u)
```

---

## 3. Deploy Kafka

### Create namespace

```bash
kubectl create namespace kafka
```

### Apply manifests

```bash
kubectl apply -f k8s/kafka.yaml
```

### Wait until Kafka is running

```bash
kubectl get pods -n kafka -w
```

### Create topic (6 partitions)

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

### Verify topic

```bash
kubectl exec -n kafka kafka-0 -- \
  /opt/kafka/bin/kafka-topics.sh \
  --describe \
  --topic events-v2 \
  --bootstrap-server localhost:9092
```

---

## 4. Deploy producer and consumer

### Create namespace

```bash
kubectl create namespace app
```

### Apply configuration and workloads

```bash
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/producer-deployment.yaml
kubectl apply -f k8s/consumer-deployment.yaml
```

### Verify pods

```bash
kubectl get pods -n app
```

### Application logs

```bash
kubectl logs -n app -l app=producer -f
```

```bash
kubectl logs -n app -l app=consumer -f
```

The producer runs ~30s at **20 msg/s**, then a **30s burst at 500 msg/s**, then returns to 20 msg/s (see `k8s/configmap.yaml`). These defaults are chosen so lag can drain after a burst and replicas can scale back down.

---

## 5. Install KEDA and enable autoscaling

### Add Helm repo and install KEDA

```bash
helm repo add kedacore https://kedacore.github.io/charts
helm repo update
helm install keda kedacore/keda --namespace keda --create-namespace
```

Wait until KEDA is ready:

```bash
kubectl get pods -n keda
```

### Apply ScaledObject

```bash
kubectl apply -f k8s/keda/consumer-scaledobject.yaml
```

Check status:

```bash
kubectl get scaledobject,hpa -n app
kubectl describe scaledobject consumer-scaler -n app
```

KEDA creates an HPA that drives the `consumer` deployment. Do not `kubectl scale` the consumer while the ScaledObject is active.

Threshold tuning: see [`k8s/keda/README.md`](k8s/keda/README.md).

---

## 6. Verify autoscaling (without Grafana)

In one terminal, watch replicas and HPA:

```bash
kubectl get deploy consumer -n app -w
```

In another, watch ScaledObject / HPA:

```bash
kubectl get scaledobject,hpa -n app -w
```

During the producer **burst** (after ~30s from pod start), consumer replicas should increase (up to **6**, matching partition count). After load drops, wait **1–3 minutes** for the backlog to drain; replicas then decrease (cooldown **30s** in the ScaledObject).


### Inspect consumer group lag

```bash
kubectl exec -n kafka kafka-0 -- \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group event-processors-k8s
```

### Restart producer to repeat the burst

```bash
kubectl rollout restart deployment/producer -n app
```

---

## 7. Monitoring (Prometheus + Grafana)

### Create monitoring namespace

```bash
kubectl apply -f k8s/monitoring/namespace.yaml
```

### Install kube-prometheus-stack

```bash
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update
helm install kube-prometheus prometheus-community/kube-prometheus-stack \
  --namespace monitoring \
  -f k8s/monitoring/kube-prometheus-stack-values.yaml
```

Wait for pods:

```bash
kubectl get pods -n monitoring
```

### Deploy kafka-exporter

```bash
kubectl apply -f k8s/monitoring/kafka-exporter.yaml
```

Confirm the ServiceMonitor exists and targets the exporter:

```bash
kubectl get servicemonitor -n monitoring
kubectl get pods -n monitoring -l app=kafka-exporter
```

## 8. Grafana and example queries

### Port-forward Grafana

```bash
kubectl port-forward -n monitoring svc/kube-prometheus-grafana 3000:80
```

Open [http://localhost:3000](http://localhost:3000). Default login (from values file): **admin** / **admin**.

### Explore → Prometheus — example PromQL

**Total consumer lag** (kafka-exporter):

```promql
sum(kafka_consumergroup_lag{consumergroup="event-processors-k8s", topic="events-v2"})
```

**Consumer deployment replicas:**

```promql
kube_deployment_status_replicas{namespace="app", deployment="consumer"}
```

Trigger another burst and confirm lag spikes align with replica increases.

### Optional: port-forward Prometheus

```bash
kubectl get svc -n monitoring
kubectl port-forward -n monitoring svc/kube-prometheus-kube-prom-prometheus 9090:9090
```

If the service name differs, use the Prometheus service listed by `kubectl get svc -n monitoring` (suffix `-prometheus`).

Open [http://localhost:9090](http://localhost:9090) and run the same queries under **Graph**.

---

## Configuration

Application settings: [`k8s/configmap.yaml`](k8s/configmap.yaml).

| Variable | Description |
|----------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka bootstrap server |
| `TOPIC_NAME` | Kafka topic |
| `CONSUMER_GROUP` | Consumer group (must match KEDA ScaledObject) |
| `MESSAGES_PER_SECOND` | Normal producer throughput |
| `BURST_MESSAGES_PER_SECOND` | Burst throughput |
| `BURST_DURATION_SECONDS` | Burst duration |
| `MESSAGE_SIZE_BYTES` | Event payload size |
| `PROCESSING_DELAY_MS` | Consumer processing delay |

After changing the ConfigMap:

```bash
kubectl apply -f k8s/configmap.yaml
kubectl rollout restart deployment/producer deployment/consumer -n app
```

If you change `CONSUMER_GROUP` or `TOPIC_NAME`, update [`k8s/keda/consumer-scaledobject.yaml`](k8s/keda/consumer-scaledobject.yaml) and kafka-exporter args accordingly.
