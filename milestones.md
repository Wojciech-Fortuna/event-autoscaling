# Detailed Project Plan: Event-Driven Autoscaling on Kubernetes

> Dariusz Cebula, Wojciech Fortuna

## Milestone 1: Local Environment & Student Lab

In this initial phase, the focus is on deploying the architecture for application-level autoscaling in a local environment. Due to the limitations of local clusters, infrastructure (node-level) scaling will be temporarily omitted. This milestone also includes creating an educational lab for students.

### Tasks:
- **Local Cluster Setup:** Provisioning a local Kubernetes cluster using tools like Minikube or Kind.
- **Event Streaming Platform Deployment:** Deploying Apache Kafka to act as the event source, enabling efficient handling and buffering of high-throughput data streams.
- **Application Implementation:** Developing a workload generator (producer) that sends events to a Kafka topic, and a processing application (consumer).
- **Monitoring Stack Configuration:** Installing Prometheus for metrics collection and Grafana for visualization.
- **KEDA and HPA Deployment (Pod Scaling Mechanism):** Implementing an event-driven autoscaling mechanism.
  - **Scaling Basis:** Pods will be dynamically scaled using the Horizontal Pod Autoscaler (HPA). HPA will make scaling decisions based on external metrics provided by KEDA.
  - **The Specific Metric:** The key metric used to trigger the addition of new pods is the `Kafka consumer lag`, which is defined as the number of messages waiting to be processed. KEDA will continuously monitor this backlog level and provide the metrics to the HPA, which will then scale the number of processing pods accordingly.
- **Metrics to collect:** Kafka consumer lag, per-pod CPU and memory, pod start time (container creation -> ready), message processing rate, and application error rate.
- **Student Lab Development:** Preparing instructions and a baseline environment for students. The scenario will involve providing them with a running cluster, Kafka, and the applications. Their task will be to write the deployment manifests to link the HPA with KEDA's external metrics and observe how the application reacts to the workload load.

#### Student lab tasks (suggestions):

- Provide base manifests: Kafka (Helm), consumer deployment, producer workload generator, KEDA ScaledObject, HPA stub, Prometheus, Grafana.
- Tasks for students:
  1. Connect KEDA ScaledObject to monitor Kafka lag and expose metric to HPA.
  2. Tune HPA target and KEDA trigger thresholds to meet a sample SLA.
  3. Run provided workload scenarios and record metrics.
  4. Produce a short report comparing two runs (default HPA settings vs tuned settings).
- Deliverables: updated manifests, Grafana screenshot(s), short results table with measured cold-start and average lag.

## Milestone 2: Google Cloud Platform (GCP) Migration & Evaluation

Once the local solution and the student lab are fully operational, the project will transition to Google Cloud Platform using a trial period to test the system's full elasticity and cost-efficiency.

### Tasks:
- **Cloud Setup**: Creating a GCP account and provisioning a Google Kubernetes Engine (GKE) cluster.
- **Environment Migration:** Migrating all components (Kafka, producer, consumer, KEDA, Prometheus, Grafana) to the cloud environment.
- **Cluster Autoscaler Deployment:** Configuring the Cluster Autoscaler to dynamically manage infrastructure by increasing the number of nodes when necessary.
- **Workload Profile Generation:** Programming a bursty workload generator characterized by periods of little or no activity, sudden spikes in events, and a return to an idle state.
- **Executing Variant A (Static Cluster):** Running the workload against a static cluster with a fixed number of nodes and consumer pods, without any dynamic scaling.
- **Executing Variant B (Autoscaling Cluster):** Running the same workload using dynamic pod scaling via KEDA and HPA, along with dynamic node scaling via the Cluster Autoscaler.
- **Workload profiles:**
  - *Steady low:* 10 msgs/s for 30 minutes
  - *Short burst:* 1000 msgs/s for 1 minute, every 30 minutes (repeat 4 times)
  - *Long burst:* 500 msgs/s for 10 minutes, single run
- **Evaluation and Comparison:** Using Grafana dashboards to analyze key metrics:
  - Cold-start latency (time required to react to workload increases).
  - Kafka consumer lag over time.
  - Number of active pods and cluster nodes.
  - Resource utilization (CPU and memory).
  - Estimated infrastructure cost over time.
- **Final Summary:** Comparing both variants to evaluate the trade-off between system responsiveness, resource utilization, and overall infrastructure costs