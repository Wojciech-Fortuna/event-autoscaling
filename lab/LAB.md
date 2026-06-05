# Lab: Event-Driven Autoscaling on Kubernetes

**Course topic:** Kubernetes autoscaling with KEDA and Kafka consumer lag  
**Estimated time:** 2–3 hours  
**Environment:** Local Minikube cluster

---

## Introduction

Modern applications often process events asynchronously: orders, sensor readings, log lines, or user actions arrive in bursts rather than at a steady rate. A message broker such as **Apache Kafka** buffers those events so producers and consumers can work at different speeds.

When incoming traffic spikes, a fixed number of consumer pods may not keep up. Messages accumulate in the topic — this backlog is called **consumer lag**. If lag grows too large or stays high for too long, users experience delayed processing, stale data, or missed deadlines.

**Horizontal Pod Autoscaler (HPA)** in Kubernetes adjusts the number of pod replicas based on metrics. CPU and memory are common signals, but for event-driven workloads **lag is often a better signal**: it reflects queue pressure *before* pods become CPU-saturated.

**KEDA** (Kubernetes Event-Driven Autoscaling) extends HPA with **external metrics** from event sources. A KEDA `ScaledObject` watches Kafka lag and creates or updates an HPA that scales your consumer deployment automatically.

### What you will build

```text
Producer → Kafka (events-v2) → Consumer group
                                    ↓
                          KEDA ScaledObject → HPA → consumer pods
```

In this lab you will:

1. Deploy Kafka and sample producer/consumer applications on Minikube.
2. Run a **static** consumer deployment (fixed replicas) and measure lag under burst load.
3. Implement **KEDA autoscaling** by writing a `ScaledObject` manifest.
4. Tune scaling parameters and compare two autoscaling runs.
5. Write a report comparing **static pods vs autoscaling**.

### Provided vs your work

| Provided for you | You create |
|------------------|------------|
| Python producer and consumer (`lab/apps/`) | `k8s/keda/consumer-scaledobject.yaml` |
| Kafka, ConfigMap, producer/consumer Deployments (`lab/starter/k8s/`) | Answers and comparison tables in this document |
| ScaledObject template (`lab/templates/`) | Optional bonus: Grafana screenshot |

### Prerequisites

Install before starting:

- [Docker](https://docs.docker.com/get-docker/)
- [kubectl](https://kubernetes.io/docs/tasks/tools/)
- [Minikube](https://minikube.sigs.k8s.io/docs/start/) — recommended: **4 CPUs, 8 GB RAM**
- [Helm 3](https://helm.sh/docs/intro/install/)

All commands assume your working directory is the `lab/` folder unless noted otherwise.

### Workload profile

The producer application (provided) follows this pattern in an infinite loop:

| Phase | Duration | Rate |
|-------|----------|------|
| Normal load | 30 seconds | 20 messages/s |
| Burst | 30 seconds | 500 messages/s |
| Normal load | until restarted | 20 messages/s |

The consumer sleeps **50 ms** per message (`PROCESSING_DELAY_MS` in the ConfigMap). With 2 pods, sustained throughput is roughly **40 msg/s** — far below the burst rate — so lag will grow during the burst unless you scale out.

### Success criteria

By the end of the lab you should have:

- A running Minikube cluster with Kafka and both applications.
- Measured lag under static (fixed) consumer replicas.
- A working KEDA `ScaledObject` that scales consumers during bursts.
- A tuned configuration with documented before/after metrics.
- A written comparison of static vs autoscaling behavior.

---

## Task 1 — Cluster and platform setup

### Learning objectives

- Start a local Kubernetes cluster and enable basic addons.
- Deploy Kafka and create a partitioned topic.
- Build a container image inside Minikube and run producer/consumer workloads.

### Theory

**Minikube** runs a single-node Kubernetes cluster on your machine. Application images must be available to that node — the simplest approach for local labs is to build directly in Minikube’s Docker daemon.

**Namespaces** isolate resources. This lab uses:

- `kafka` — message broker
- `app` — producer and consumer

Kafka topics are split into **partitions**. Each partition can be consumed by at most one consumer in a group at a time. Therefore, the maximum useful replica count for a consumer group equals the **partition count** of the topic.

### Steps

#### 1.1 Start Minikube

```bash
minikube start --cpus=4 --memory=8192
minikube addons enable metrics-server
kubectl get nodes
```

#### 1.2 Create namespaces

```bash
kubectl create namespace kafka
kubectl create namespace app
```

#### 1.3 Deploy Kafka

From the `lab/` directory:

```bash
kubectl apply -f starter/k8s/kafka.yaml
kubectl get pods -n kafka -w
```

Wait until `kafka-0` is `Running`, then create the topic with **6 partitions**:

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

Verify:

```bash
kubectl exec -n kafka kafka-0 -- \
  /opt/kafka/bin/kafka-topics.sh \
  --describe \
  --topic events-v2 \
  --bootstrap-server localhost:9092
```

#### 1.4 Build the application image

The deployments expect `event-app:latest`. Build it inside Minikube’s Docker environment.

**Linux / macOS (bash):**

```bash
cd apps
eval $(minikube docker-env)
docker build -t event-app:latest .
cd ..
```

**Windows (PowerShell):**

```powershell
cd apps
& minikube -p minikube docker-env --shell powershell | Invoke-Expression
docker build -t event-app:latest .
cd ..
```

#### 1.5 Deploy producer and consumer

```bash
kubectl apply -f starter/k8s/configmap.yaml
kubectl apply -f starter/k8s/producer-deployment.yaml
kubectl apply -f starter/k8s/consumer-deployment.yaml
kubectl get pods -n app
```

Optional — follow logs:

```bash
kubectl logs -n app -l app=producer -f
kubectl logs -n app -l app=consumer -f
```

### Deliverables

1. Paste the output of:

```bash
kubectl get pods -n kafka
kubectl get pods -n app
```

**Your output:**

```text
(paste here)
```

2. Answer: **Why must the topic partition count align with the maximum number of consumer replicas?**

**Your answer:**

```text
(write here)
```

### Hints

<details>
<summary>Hint 1</summary>

Kafka assigns each partition to at most one consumer in the same group. Extra replicas beyond the partition count will sit idle.
</details>

<details>
<summary>Hint 2</summary>

If you create the topic with only 1 partition, KEDA cannot scale the consumer group beyond 1 active consumer, regardless of `maxReplicaCount`.
</details>

<details>
<summary>Hint 3</summary>

See the [KEDA Kafka scaler documentation](https://keda.sh/docs/latest/scalers/apache-kafka/) for how lag is aggregated across partitions.
</details>

---

## Task 2 — Static consumer baseline (under-provisioned)

### Learning objectives

- Understand fixed replica count as static capacity planning.
- Measure consumer lag using Kafka tooling.
- Observe backlog growth when capacity is insufficient.

### Theory

A Kubernetes **Deployment** declares a desired number of **replicas**. Without autoscaling, that number stays fixed until someone changes it manually (`kubectl scale` or editing the manifest).

Static provisioning is simple and predictable, but you must choose capacity in advance:

- **Too few replicas** → lag grows during bursts; recovery takes a long time.
- **Too many replicas** → low lag, but wasted CPU/memory during quiet periods.

In this task you intentionally under-provision the consumer to establish a baseline for later comparison.

### Steps

#### 2.1 Fix consumer replicas at 2

Ensure KEDA is **not** installed yet (or no ScaledObject is applied). Scale the consumer:

```bash
kubectl scale deployment consumer -n app --replicas=2
kubectl get deploy consumer -n app
```

#### 2.2 Trigger a fresh burst

Restart the producer so the 30-second warmup and burst cycle starts again:

```bash
kubectl rollout restart deployment/producer -n app
```

#### 2.3 Watch lag during and after the burst

Run this command every 10–15 seconds during the burst and for several minutes after:

```bash
kubectl exec -n kafka kafka-0 -- \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group event-processors-k8s
```

The **LAG** column (sum across partitions) is your backlog. Record:

- Peak total lag during the burst.
- Approximate time until total lag drops below ~500 after the burst ends.
- Whether lag returned to near zero before the producer’s next burst cycle.

Optional — watch replica count (should stay at 2):

```bash
kubectl get deploy consumer -n app -w
```

### Deliverables

Fill in the table from your observations:

| Metric | Your measurement |
|--------|------------------|
| Fixed replica count | 2 |
| Peak total lag during burst | |
| Time until lag returns near 0 after burst | |
| Did lag fully drain before the next burst cycle? | yes / no |

Brief observation (2–3 sentences): What happened to lag during the 500 msg/s burst, and why?

**Your observation:**

```text
(write here)
```

### Hints

<details>
<summary>Hint 1</summary>

With 50 ms processing delay, one consumer handles about 20 msg/s. Two consumers handle about 40 msg/s — much less than the 500 msg/s burst.
</details>

<details>
<summary>Hint 2</summary>

Sum the LAG column from all partitions in the `--describe` output, or note the largest single-partition lag if partitions are balanced unevenly.
</details>

<details>
<summary>Hint 3</summary>

Peak lag is often in the **thousands** for this scenario. Recovery after the burst may take **several minutes** with only 2 replicas.
</details>

---

## Task 3 — Event-driven autoscaling with KEDA

### Learning objectives

- Install KEDA on a Kubernetes cluster.
- Connect a `ScaledObject` to Kafka consumer lag.
- Observe KEDA-created HPA scaling the consumer deployment.

### Theory

**KEDA** runs an operator that reads external signals (here: Kafka lag) and drives the Kubernetes **Horizontal Pod Autoscaler**. You define a `ScaledObject` that specifies:

- **scaleTargetRef** — which Deployment to scale
- **minReplicaCount / maxReplicaCount** — scaling bounds
- **triggers** — event source configuration (Kafka bootstrap servers, consumer group, topic, lag threshold)

KEDA creates an HPA resource automatically. Verify with:

```bash
kubectl get scaledobject,hpa -n app
```

**Important:** Do not use `kubectl scale` on the consumer while a ScaledObject is active — KEDA owns replica count.

Scaling formula (simplified):

```text
desiredReplicas ≈ ceil(totalConsumerLag / lagThreshold)
```

Capped between `minReplicaCount` and `maxReplicaCount`.

### Steps

#### 3.1 Install KEDA

```bash
helm repo add kedacore https://kedacore.github.io/charts
helm repo update
helm install keda kedacore/keda --namespace keda --create-namespace
kubectl get pods -n keda
```

Wait until KEDA operator pods are `Running`.

#### 3.2 Complete the ScaledObject manifest

1. Copy the template:

```bash
mkdir -p k8s/keda
cp templates/consumer-scaledobject.yaml k8s/keda/consumer-scaledobject.yaml
```

2. Replace every `__TODO__` placeholder. Cross-check values against `starter/k8s/configmap.yaml`.

3. Apply your manifest:

```bash
kubectl apply -f k8s/keda/consumer-scaledobject.yaml
kubectl get scaledobject,hpa -n app
kubectl describe scaledobject consumer-scaler -n app
```

#### 3.3 Verify autoscaling during a burst

In one terminal, watch replicas:

```bash
kubectl get deploy consumer -n app -w
```

In another, restart the producer and inspect lag:

```bash
kubectl rollout restart deployment/producer -n app
```

During the burst (~30–60 seconds after producer restart), replicas should increase above 1 (up to 6).

Capture output during the burst:

```bash
kubectl get scaledobject,hpa,deploy -n app
```

### Deliverables

1. Submit your completed `k8s/keda/consumer-scaledobject.yaml`.

2. Paste output showing scaling in action (replicas > 1 during burst):

```text
(paste here)
```

3. Explain in your own words: **What does `desiredReplicas ≈ totalLag / lagThreshold` mean, and what happens when you lower `lagThreshold`?**

**Your answer:**

```text
(write here)
```

### Hints

<details>
<summary>Hint 1</summary>

The bootstrap server is a cluster-internal DNS name for the Kafka service in the `kafka` namespace. Inspect `starter/k8s/configmap.yaml` for the exact value used by the apps.
</details>

<details>
<summary>Hint 2</summary>

`consumerGroup` must exactly match `CONSUMER_GROUP` in the ConfigMap (`event-processors-k8s`).
</details>

<details>
<summary>Hint 3</summary>

Set `maxReplicaCount` to **6** to match the topic you created in Task 1. A lower `lagThreshold` triggers scale-up sooner (more replicas for the same lag).
</details>

---

## Task 4 — Tune autoscaling behavior

### Learning objectives

- Understand `lagThreshold`, `pollingInterval`, and `cooldownPeriod`.
- Run controlled experiments and compare scaling profiles.
- Meet a simple backlog SLA through configuration.

### Theory

Key tuning parameters in your ScaledObject:

| Parameter | Effect |
|-----------|--------|
| `lagThreshold` | Lower → scale up sooner (more replicas for same lag) |
| `pollingInterval` | How often KEDA checks Kafka (seconds) |
| `cooldownPeriod` | Minimum wait after scaling before scaling down again |
| HPA `behavior` | Controls scale-up/down speed and stabilization windows |

**Run A (baseline):** use defaults — `lagThreshold: "500"`, `pollingInterval: 15`, `cooldownPeriod: 30`.

**Run B (tuned):** change **at least two** parameters. Target for Run B:

- Peak lag **below 3000** during the burst.
- Scale-down to near-minimum replicas within **3 minutes** after the burst ends.

Document every value you change.

Example directions (choose your own values):

- Lower `lagThreshold` to scale up faster.
- Reduce `pollingInterval` for quicker detection.
- Adjust `cooldownPeriod` or HPA scale-down policies for faster scale-in.

### Steps

For each run:

1. Update `k8s/keda/consumer-scaledobject.yaml` and apply:

```bash
kubectl apply -f k8s/keda/consumer-scaledobject.yaml
```

2. Restart the producer:

```bash
kubectl rollout restart deployment/producer -n app
```

3. Record peak lag, max replicas observed, approximate scale-up delay (seconds from burst start to first replica increase), and scale-down time (burst end to replicas near minimum).

Useful commands:

```bash
kubectl get deploy consumer -n app -w
kubectl exec -n kafka kafka-0 -- \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group event-processors-k8s
```

### Deliverables

#### Run A configuration (defaults)

```yaml
(paste relevant spec fields: lagThreshold, pollingInterval, cooldownPeriod, min/max replicas)
```

#### Run B configuration (your tuning)

```yaml
(paste your tuned values and briefly note what you changed)
```

#### Comparison table

| Metric | Run A | Run B |
|--------|-------|-------|
| lagThreshold | | |
| pollingInterval | | |
| cooldownPeriod | | |
| Peak total lag | | |
| Max replicas observed | | |
| Approx. scale-up delay | | |
| Approx. scale-down time | | |

Did Run B meet the target (peak lag < 3000, scale-down within 3 min)? **yes / no**

If not, what would you try next?

**Your answer:**

```text
(write here)
```

### Hints

<details>
<summary>Hint 1</summary>

Halving `lagThreshold` (e.g. 500 → 250) roughly doubles the desired replica count for the same lag.
</details>

<details>
<summary>Hint 2</summary>

The template sets `stabilizationWindowSeconds: 0` on scale-down so Kubernetes does not wait the default 5 minutes before reducing replicas.
</details>

<details>
<summary>Hint 3</summary>

Very aggressive tuning may cause **flapping** (rapid scale up/down). If that happens, slightly increase `cooldownPeriod` or lagThreshold.
</details>

---

## Task 5 — Final comparison: static vs autoscaling

### Learning objectives

- Synthesize results from static and autoscaling experiments.
- Articulate trade-offs between fixed capacity and dynamic scaling.
- Identify scenarios where static provisioning is preferable.

### Theory

**Static provisioning** offers simplicity and stable resource usage. Capacity is chosen upfront; during low traffic you may pay for idle pods, and during spikes you may fail to meet latency/backlog SLAs.

**Event-driven autoscaling** reacts to backlog (lag), adding capacity when needed and releasing it when the queue drains. The cost is operational complexity, cold-start delay for new pods, and tuning effort.

A complete comparison considers:

- **Peak lag** — worst backlog during burst.
- **Recovery time** — how long until lag is acceptable after load drops.
- **Average replicas** — resource usage over a observation window (~10 minutes).
- **Operational complexity** — what you had to configure and monitor.

### Steps

#### 5.1 Summarize static run (Task 2)

Use your Task 2 measurements for **2 fixed replicas**.

#### 5.2 Summarize autoscaling run (Task 4)

Use your best autoscaling run (Run A or Run B — state which).

#### 5.3 Optional — over-provisioned static experiment

Scale consumer to 6 replicas (matching partition count) **without** ScaledObject (delete ScaledObject first if needed):

```bash
kubectl delete -f k8s/keda/consumer-scaledobject.yaml
kubectl scale deployment consumer -n app --replicas=6
kubectl rollout restart deployment/producer -n app
```

Observe lag and resource usage. Write 2–3 sentences on waste vs performance.

**Optional observation:**

```text
(write here, or skip)
```

Re-apply your ScaledObject after this experiment if you continue to Task 6.

### Deliverables

#### Comparison table

| Aspect | Static (2 replicas) | Autoscaling (Run ___) |
|--------|---------------------|------------------------|
| Peak lag during burst | | |
| Recovery time after burst | | |
| Avg replicas over ~10 min | 2 | |
| Handles burst without manual intervention? | | |
| Resource efficiency during low load | | |

#### Three conclusions

Write **three bullet points** about trade-offs you observed:

1.
2.
3.

#### When would you choose static pods?

Describe **one realistic scenario** where fixed replicas are preferable to KEDA autoscaling, and justify your answer.

**Your answer:**

```text
(write here)
```

---

## Task 6 — Bonus: Monitoring with Prometheus and Grafana

> **Optional.** Not required to pass the lab. Requires additional cluster resources (~8 GB RAM recommended).

### Learning objectives

- Expose Kafka consumer lag as Prometheus metrics.
- Correlate lag and replica count in Grafana.

### Theory

**kafka-exporter** polls Kafka and exposes metrics such as `kafka_consumergroup_lag`. **Prometheus** scrapes those metrics; **Grafana** visualizes them. This stack helps validate scaling behavior with graphs instead of only CLI snapshots.

### Steps

#### 6.1 Deploy monitoring namespace and kafka-exporter

```bash
kubectl apply -f starter/k8s/monitoring/namespace.yaml
kubectl apply -f starter/k8s/monitoring/kafka-exporter.yaml
```

#### 6.2 Install kube-prometheus-stack

From the `lab/` directory:

```bash
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update
helm install kube-prometheus prometheus-community/kube-prometheus-stack \
  --namespace monitoring \
  -f starter/k8s/monitoring/kube-prometheus-stack-values.yaml
kubectl get pods -n monitoring
```

#### 6.3 Explore in Grafana

Port-forward:

```bash
kubectl port-forward -n monitoring svc/kube-prometheus-grafana 3000:80
```

Open [http://localhost:3000](http://localhost:3000) — login **admin** / **admin**.

In **Explore → Prometheus**, try:

**Total consumer lag:**

```promql
sum(kafka_consumergroup_lag{consumergroup="event-processors-k8s", topic="events-v2"})
```

**Consumer replicas:**

```promql
kube_deployment_status_replicas{namespace="app", deployment="consumer"}
```

Restart the producer, trigger a burst, and confirm lag spikes align with replica increases.

### Deliverables

Attach one screenshot or paste query output showing lag and replica count changing together.

**Bonus evidence:**

```text
(paste or describe screenshot filename)
```

---

## Submission checklist

Submit the following:

- [ ] This file (`LAB.md`) with all answer sections completed
- [ ] `k8s/keda/consumer-scaledobject.yaml` (your completed manifest)
- [ ] Task 5 comparison table and three conclusions
- [ ] (Optional) Task 6 Grafana screenshot or query output

**Student name:** ____________________  
**Date:** ____________________

---

## Reference — useful commands

```bash
# Pod status
kubectl get pods -n kafka
kubectl get pods -n app

# Consumer lag
kubectl exec -n kafka kafka-0 -- \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group event-processors-k8s

# Scaling status
kubectl get scaledobject,hpa,deploy -n app

# Restart producer burst cycle
kubectl rollout restart deployment/producer -n app

# Application logs
kubectl logs -n app -l app=producer --tail=50
kubectl logs -n app -l app=consumer --tail=50
```

## Reference — configuration values

From `starter/k8s/configmap.yaml`:

| Variable | Value |
|----------|-------|
| `TOPIC_NAME` | `events-v2` |
| `CONSUMER_GROUP` | `event-processors-k8s` |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka.kafka.svc.cluster.local:9092` |
| `MESSAGES_PER_SECOND` | 20 |
| `BURST_MESSAGES_PER_SECOND` | 500 |
| `BURST_DURATION_SECONDS` | 30 |
| `PROCESSING_DELAY_MS` | 50 |
