# Event Autoscaling with Kafka, KEDA and AWS EKS

A cloud-native event processing system used to demonstrate autoscaling of Kafka consumers in Kubernetes with KEDA based on Kafka consumer lag, and infrastructure autoscaling on AWS EKS with Cluster Autoscaler.

The project supports both:

1. Local Kubernetes / Minikube experiments
2. Cloud experiments on AWS EKS with ECR and Cluster Autoscaler

---

## Architecture

```text
Producer
   ↓
Kafka topic
   ↓
Consumer group
   ↓
KEDA ScaledObject
   ↓
Horizontal Pod Autoscaler
   ↓
Consumer replicas
   ↓
Cluster Autoscaler
   ↓
AWS EKS worker nodes
```

Monitoring:

```text
Kafka → kafka-exporter → Prometheus → Grafana
```

---

## Components

- **Kafka** — event buffer deployed in Kubernetes.
- **Producer** — Python application generating normal traffic and burst traffic.
- **Consumer** — Python application processing Kafka messages with configurable processing delay.
- **KEDA** — scales the `consumer` deployment based on Kafka consumer lag.
- **Cluster Autoscaler** — scales EKS worker nodes when consumer pods cannot be scheduled.
- **kafka-exporter** — exposes Kafka consumer group lag metrics.
- **Prometheus + Grafana** — visual monitoring of lag, replicas and cluster behavior.

---

## Repository structure

```text
.
├── producer.py
├── consumer.py
├── Dockerfile
├── requirements.txt
├── k8s/
│   ├── configmap.yaml
│   ├── consumer-deployment.yaml
│   ├── consumer-static-deployment.yaml
│   ├── kafka.yaml
│   ├── producer-deployment.yaml
│   ├── keda/
│   │   └── consumer-scaledobject.yaml
│   └── monitoring/
│       ├── kafka-exporter.yaml
│       ├── kube-prometheus-stack-values.yaml
│       └── namespace.yaml
├── cloud/
│   ├── eks/
│   │   └── cluster.yaml
│   └── helm/
│       ├── install-keda.sh
│       └── install-cluster-autoscaler.sh
└── demo/
    ├── 00-reset-demo.sh
    ├── 01-start-demo.sh
    └── 02-watch-demo.sh
```

---

# Part 1 — Local Kubernetes / Minikube

## Prerequisites

- Docker
- kubectl
- Minikube
- Helm 3

Recommended Minikube resources:

```text
4 CPUs
8 GB RAM
```

---

## 1. Start Minikube

```bash
minikube start --cpus=4 --memory=8192
minikube addons enable metrics-server
kubectl get nodes
```

---

## 2. Build image inside Minikube

The local manifests use:

```text
event-app:latest
```

Build the image inside Minikube Docker daemon.

Linux / macOS:

```bash
eval $(minikube docker-env)
docker build -t event-app:latest .
```

Windows PowerShell:

```powershell
& minikube -p minikube docker-env --shell powershell | Invoke-Expression
docker build -t event-app:latest .
```

To return to host Docker:

```bash
eval $(minikube docker-env -u)
```

---

## 3. Deploy Kafka

```bash
kubectl create namespace kafka
kubectl apply -f k8s/kafka.yaml
kubectl get pods -n kafka -w
```

Create topic:

```bash
kubectl exec -n kafka kafka-0 -- \
  /opt/kafka/bin/kafka-topics.sh \
  --create \
  --if-not-exists \
  --topic events-test-2 \
  --partitions 18 \
  --replication-factor 1 \
  --bootstrap-server localhost:9092
```

Verify:

```bash
kubectl exec -n kafka kafka-0 -- \
  /opt/kafka/bin/kafka-topics.sh \
  --describe \
  --topic events-test-2 \
  --bootstrap-server localhost:9092
```

Important observation:

```text
The number of active Kafka consumers is effectively limited by the number of topic partitions.
```

For example:

```text
1 partition  → KEDA cannot efficiently use more than 1 consumer
6 partitions → up to 6 consumers can process in parallel
18 partitions → up to 18 consumers can process in parallel
```

---

## 4. Deploy application locally

```bash
kubectl create namespace event-autoscaling
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/producer-deployment.yaml
kubectl apply -f k8s/consumer-deployment.yaml
```

Verify:

```bash
kubectl get pods -n event-autoscaling
kubectl logs -n event-autoscaling deploy/producer --tail=50
kubectl logs -n event-autoscaling deploy/consumer --tail=50
```

---

## 5. Install KEDA locally

```bash
helm repo add kedacore https://kedacore.github.io/charts
helm repo update

helm upgrade --install keda kedacore/keda \
  --namespace keda \
  --create-namespace
```

Verify:

```bash
kubectl get pods -n keda
kubectl api-resources | grep -i scaledobject
```

Apply ScaledObject:

```bash
kubectl apply -f k8s/keda/consumer-scaledobject.yaml
```

Verify:

```bash
kubectl get scaledobject -n event-autoscaling
kubectl get hpa -n event-autoscaling
```

---

# Part 2 — AWS EKS Cloud Deployment

## AWS resources used

- AWS EKS
- AWS ECR
- EC2 worker nodes
- Auto Scaling Group
- Cluster Autoscaler
- Kubernetes managed node group

Current cloud setup:

```text
AWS region: us-east-1
EKS cluster: kafka-keda-eks
Node group: apps-ng
Namespace for app: event-autoscaling
Namespace for Kafka: kafka
Namespace for KEDA: keda
```

---

## 1. Verify AWS CLI

```bash
aws sts get-caller-identity
```

Expected result:

```json
{
  "Account": "337462094945",
  "Arn": "arn:aws:iam::337462094945:root"
}
```

---

## 2. Create EKS cluster

```yaml
apiVersion: eksctl.io/v1alpha5
kind: ClusterConfig

metadata:
  name: kafka-keda-eks
  region: us-east-1
  version: "1.30"

iam:
  withOIDC: true

managedNodeGroups:
  - name: apps-ng
    instanceType: t3.medium

    desiredCapacity: 2
    minSize: 2
    maxSize: 6

    volumeSize: 30

    labels:
      role: apps

    tags:
      k8s.io/cluster-autoscaler/enabled: "true"
      k8s.io/cluster-autoscaler/kafka-keda-eks: "owned"

    iam:
      withAddonPolicies:
        autoScaler: true
```

Create cluster:

```bash
eksctl create cluster -f cloud/eks/cluster.yaml
```

Verify:

```bash
kubectl get nodes
kubectl get pods -A
```

---

## 3. Create ECR repository

```bash
aws ecr create-repository \
  --repository-name event-app \
  --region us-east-1
```

Repository URI:

```text
337462094945.dkr.ecr.us-east-1.amazonaws.com/event-app
```

---

## 4. Build and push Docker image to ECR

```bash
AWS_REGION=us-east-1
AWS_ACCOUNT_ID=337462094945
ECR_URI=$AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/event-app
```

Login:

```bash
aws ecr get-login-password --region $AWS_REGION \
  | docker login --username AWS --password-stdin $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com
```

Build:

```bash
docker build -t event-app:eks .
```

Tag:

```bash
docker tag event-app:eks $ECR_URI:eks
```

Push:

```bash
docker push $ECR_URI:eks
```

Cloud deployments use:

```yaml
image: 337462094945.dkr.ecr.us-east-1.amazonaws.com/event-app:eks
```

---

## 5. Deploy Kafka on EKS

```bash
kubectl create namespace kafka --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f k8s/kafka.yaml
kubectl get pods -n kafka -w
```

Create / verify topic:

```bash
kubectl exec -n kafka kafka-0 -- \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --if-not-exists \
  --topic events-test-2 \
  --partitions 18 \
  --replication-factor 1
```

If topic already exists but has fewer partitions:

```bash
kubectl exec -n kafka kafka-0 -- \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --alter \
  --topic events-test-2 \
  --partitions 18
```

Verify:

```bash
kubectl exec -n kafka kafka-0 -- \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic events-test-2
```

---

## 6. Deploy application on EKS

```bash
kubectl create namespace event-autoscaling --dry-run=client -o yaml | kubectl apply -f -

kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/consumer-deployment.yaml
kubectl apply -f k8s/producer-deployment.yaml
```

Verify:

```bash
kubectl get pods -n event-autoscaling -o wide
kubectl logs -n event-autoscaling deploy/producer --tail=50
kubectl logs -n event-autoscaling deploy/consumer --tail=50
```

---

## 7. Install KEDA on EKS

KEDA version used:

```text
2.17.2
```

This version was selected because the EKS cluster runs Kubernetes `1.30`.

```bash
helm repo add kedacore https://kedacore.github.io/charts
helm repo update

helm upgrade --install keda kedacore/keda \
  --namespace keda \
  --create-namespace \
  --version 2.17.2
```

Verify:

```bash
kubectl get pods -n keda
kubectl api-resources | grep -i scaledobject
```

---

## 8. KEDA ScaledObject

Current ScaledObject:

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: consumer-scaler
  namespace: event-autoscaling
spec:
  scaleTargetRef:
    name: consumer

  minReplicaCount: 1
  maxReplicaCount: 18

  pollingInterval: 15
  cooldownPeriod: 30

  advanced:
    horizontalPodAutoscalerConfig:
      behavior:
        scaleDown:
          stabilizationWindowSeconds: 0
          policies:
            - type: Percent
              value: 100
              periodSeconds: 15
        scaleUp:
          stabilizationWindowSeconds: 0

  triggers:
    - type: kafka
      metadata:
        bootstrapServers: kafka.kafka.svc.cluster.local:9092
        consumerGroup: event-processors-autoscaled-test-2
        topic: events-test-2
        lagThreshold: "20"
        offsetResetPolicy: earliest
```

Apply:

```bash
kubectl apply -f k8s/keda/consumer-scaledobject.yaml
```

Verify:

```bash
kubectl get scaledobject -n event-autoscaling
kubectl get hpa -n event-autoscaling
```

---

## 9. Install Cluster Autoscaler

Cluster Autoscaler version used:

```text
v1.30.3
```

This version matches the Kubernetes version used by the EKS cluster.

```bash
helm repo add autoscaler https://kubernetes.github.io/autoscaler
helm repo update

helm upgrade --install cluster-autoscaler autoscaler/cluster-autoscaler \
  --namespace kube-system \
  --set image.tag=v1.30.3 \
  --set autoDiscovery.clusterName=kafka-keda-eks \
  --set awsRegion=us-east-1 \
  --set rbac.serviceAccount.create=true \
  --set extraArgs.balance-similar-node-groups=true \
  --set extraArgs.skip-nodes-with-system-pods=false \
  --set extraArgs.expander=least-waste \
  --set extraArgs.scale-down-enabled=true \
  --set extraArgs.scale-down-delay-after-add=30s \
  --set extraArgs.scale-down-unneeded-time=30s \
  --set extraArgs.scale-down-delay-after-delete=15s
```

Verify:

```bash
kubectl rollout status deployment/cluster-autoscaler-aws-cluster-autoscaler -n kube-system
```

Check arguments:

```bash
kubectl describe deployment cluster-autoscaler-aws-cluster-autoscaler \
  -n kube-system | grep scale-down
```

Expected:

```text
--scale-down-delay-after-add=30s
--scale-down-delay-after-delete=15s
--scale-down-enabled=true
--scale-down-unneeded-time=30s
```

Check ASG discovery:

```bash
kubectl -n kube-system logs deploy/cluster-autoscaler-aws-cluster-autoscaler --tail=120 \
  | grep -iE "asg|autoscaling|registered|node group"
```

Expected log fragment:

```text
Registering ASG eks-apps-ng-...
```

---

# Demo

The demo shows:

```text
Kafka lag increases
↓
KEDA scales consumers
↓
Some consumer pods become Pending
↓
Cluster Autoscaler increases EKS nodes
↓
Pending pods become Running
↓
Kafka lag starts decreasing
```

---

## Demo scripts

### Reset demo

```bash
./demo/00-reset-demo.sh
```

This script:

- disables KEDA,
- stops producer,
- scales consumer to 1 replica,
- restores normal CPU/memory requests,
- scales EKS node group to 2 nodes,
- waits until the application is in a clean state.

Expected state:

```text
consumer: 1/1 Running
producer: 0/0
nodes: 2
ASG: min=2 max=6 desired=2
```

---

### Watch demo

```bash
./demo/02-watch-demo.sh
```

The watch script displays:

- HPA / KEDA
- ScaledObject
- Deployments
- Pods
- Pending Pods
- Nodes
- AWS Auto Scaling Group
- Kafka consumer lag

Important values to observe:

```text
consumer replicas
Pending pods
node count
ASG DesiredCapacity
TOTAL_LAG
```

---

### Start demo

```bash
./demo/01-start-demo.sh
```

This script:

- enables KEDA,
- starts the producer,
- restarts the producer to trigger burst traffic.

Expected behavior:

```text
HPA: 1 → 18
consumer deployment: 1/1 → 18/18
Pending pods appear
ASG DesiredCapacity: 2 → 4 or more
nodes: 2 → 4 or more
Kafka TOTAL_LAG begins to decrease
```

---

# Monitoring with Prometheus and Grafana

## Install monitoring namespace

```bash
kubectl apply -f k8s/monitoring/namespace.yaml
```

## Install kube-prometheus-stack

```bash
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update

helm upgrade --install kube-prometheus prometheus-community/kube-prometheus-stack \
  --namespace monitoring \
  -f k8s/monitoring/kube-prometheus-stack-values.yaml
```

Verify:

```bash
kubectl get pods -n monitoring
```

## Deploy kafka-exporter

```bash
kubectl apply -f k8s/monitoring/kafka-exporter.yaml
```

Verify:

```bash
kubectl get pods -n monitoring -l app=kafka-exporter
kubectl get servicemonitor -n monitoring
```

---

## Access Grafana

Port-forward:

```bash
kubectl port-forward -n monitoring svc/kube-prometheus-stack-grafana 3000:80
```

Open:

```text
http://localhost:3000
```

Default login:

```text
admin / admin
```

If service name differs:

```bash
kubectl get svc -n monitoring
```

and use the Grafana service listed there.

---

## Example PromQL queries

### Total Kafka consumer lag

```promql
sum(kafka_consumergroup_lag{consumergroup="event-processors-autoscaled-test-2", topic="events-test-2"})
```

### Lag per partition

```promql
kafka_consumergroup_lag{consumergroup="event-processors-autoscaled-test-2", topic="events-test-2"}
```

### Consumer deployment replicas

```promql
kube_deployment_status_replicas{namespace="event-autoscaling", deployment="consumer"}
```

### Available consumer replicas

```promql
kube_deployment_status_replicas_available{namespace="event-autoscaling", deployment="consumer"}
```

### Desired HPA replicas

```promql
kube_horizontalpodautoscaler_status_desired_replicas{
  namespace="event-autoscaling",
  horizontalpodautoscaler="keda-hpa-consumer-scaler"
}
```

### Current HPA replicas

```promql
kube_horizontalpodautoscaler_status_current_replicas{
  namespace="event-autoscaling",
  horizontalpodautoscaler="keda-hpa-consumer-scaler"
}
```

### Number of Kubernetes nodes

```promql
count(kube_node_info)
```

---

# Static vs autoscaled experiment

The project can also demonstrate why static scaling is insufficient.

## Static variant

Disable KEDA:

```bash
kubectl delete scaledobject consumer-scaler \
  -n event-autoscaling \
  --ignore-not-found
```

Run static consumer deployment:

```bash
kubectl apply -f k8s/consumer-static-deployment.yaml
```

Expected behavior:

```text
Fixed number of consumers
No automatic scaling
Kafka lag keeps increasing during burst
```

## Autoscaled variant

Remove static deployment:

```bash
kubectl delete -f k8s/consumer-static-deployment.yaml
```

Enable KEDA:

```bash
kubectl apply -f k8s/consumer-deployment.yaml
kubectl apply -f k8s/keda/consumer-scaledobject.yaml
```

Expected behavior:

```text
Kafka lag increases
KEDA scales consumers
Lag eventually decreases
```

---

# Useful commands

## Check pods

```bash
kubectl get pods -n event-autoscaling -o wide
```

## Check HPA

```bash
kubectl get hpa -n event-autoscaling
```

## Check ScaledObject

```bash
kubectl get scaledobject -n event-autoscaling

kubectl describe scaledobject consumer-scaler \
  -n event-autoscaling
```

## Check nodes

```bash
kubectl get nodes
```

## Check ASG

```bash
aws autoscaling describe-auto-scaling-groups \
  --region us-east-1 \
  --query 'AutoScalingGroups[*].[AutoScalingGroupName,MinSize,MaxSize,DesiredCapacity]' \
  --output table
```

## Check Kafka lag manually

```bash
kubectl exec -n kafka kafka-0 -- \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group event-processors-autoscaled-test-2
```

## Restart producer

```bash
kubectl rollout restart deployment/producer \
  -n event-autoscaling
```

## Stop producer

```bash
kubectl scale deployment producer \
  -n event-autoscaling \
  --replicas=0
```

## Start producer

```bash
kubectl scale deployment producer \
  -n event-autoscaling \
  --replicas=1
```

# Cleanup

## Scale down demo state

```bash
./demo/00-reset-demo.sh
```

This restores the environment to its baseline state:

- producer stopped,
- consumer scaled down,
- KEDA disabled,
- node group returned to minimum size.

---

## Delete EKS cluster

When the project is no longer needed:

```bash
eksctl delete cluster \
  --name kafka-keda-eks \
  --region us-east-1
```

Monitor deletion:

```bash
eksctl get cluster --region us-east-1
```

You can also verify that the CloudFormation stacks have been removed:

```bash
aws cloudformation list-stacks \
  --stack-status-filter DELETE_COMPLETE
```

---

## Delete ECR repository

Optional cleanup of container images:

```bash
aws ecr delete-repository \
  --repository-name event-app \
  --force \
  --region us-east-1
```

Verify:

```bash
aws ecr describe-repositories \
  --region us-east-1
```

---

## Remove monitoring stack

Delete Grafana, Prometheus and exporters:

```bash
helm uninstall kube-prometheus -n monitoring
kubectl delete namespace monitoring
```

Verify:

```bash
kubectl get pods -n monitoring
```

Expected:

```text
No resources found
```

---

## Remove KEDA

```bash
helm uninstall keda -n keda
kubectl delete namespace keda
```

---

## Remove Kafka

```bash
kubectl delete namespace kafka
```

---

## Remove application namespace

```bash
kubectl delete namespace event-autoscaling
```

---

## Final verification

Ensure all namespaces have been removed:

```bash
kubectl get namespaces
```

The following namespaces should no longer exist:

```text
event-autoscaling
kafka
keda
monitoring
```

---

## Cost reminder

> **Important**
>
> Do not leave the EKS cluster running unintentionally.
>
> AWS charges may continue to accrue from:
>
> - EKS control plane
> - EC2 worker nodes
> - EBS volumes
> - Load Balancers
> - NAT Gateway (if used)
> - CloudWatch logs
> - ECR image storage

Verify that all resources have been removed before ending the project.

---

# Final Result

This project demonstrates:

```text
Kafka lag-based autoscaling
KEDA ScaledObject
HPA creation by KEDA
Consumer scaling: 1 → 18
Pending pods caused by insufficient node capacity
Cluster Autoscaler reaction
EKS node scaling: 2 → 4+
Kafka backlog processing
Prometheus and Grafana monitoring
Static vs autoscaled comparison
```

---

## Key Observations

### Application-level scaling

KEDA continuously monitors Kafka consumer lag and automatically adjusts the number of consumer replicas.

```text
Kafka lag ↑
↓
KEDA scales consumers ↑
↓
Processing throughput ↑
↓
Kafka lag ↓
```

### Infrastructure-level scaling

When Kubernetes cannot schedule new consumer pods due to insufficient resources:

```text
Consumer pods become Pending
↓
Cluster Autoscaler detects unschedulable pods
↓
AWS Auto Scaling Group capacity increases
↓
New EKS worker nodes join the cluster
↓
Pending pods become Running
```

### Kafka partitioning impact

The number of consumers that can actively process messages is limited by the number of partitions.

Example:

```text
1 partition   → max 1 active consumer
6 partitions  → max 6 active consumers
18 partitions → max 18 active consumers
```

For this reason, the topic is configured with:

```text
18 partitions
```

to allow horizontal scaling up to:

```text
18 consumer replicas
```

---

## Architectural Flow

```text
Producer
   ↓
Kafka Topic
   ↓
Consumer Group
   ↓
KEDA ScaledObject
   ↓
Horizontal Pod Autoscaler
   ↓
Consumer Replicas
   ↓
Cluster Autoscaler
   ↓
AWS EKS Worker Nodes
```

Monitoring pipeline:

```text
Kafka
   ↓
kafka-exporter
   ↓
Prometheus
   ↓
Grafana
```

---

## Main Conclusion

```text
KEDA solves application-level scaling by increasing Kafka consumers.

Cluster Autoscaler solves infrastructure-level scaling by adding EKS worker nodes when the cluster lacks capacity.

Together, they provide end-to-end event-driven autoscaling for Kubernetes workloads running on AWS EKS.
```

---

## Technologies Used

```text
AWS EKS
Amazon ECR
EC2 Auto Scaling Groups
Kubernetes
Kafka
KEDA
Horizontal Pod Autoscaler
Cluster Autoscaler
Prometheus
Grafana
Docker
Helm
Python
```