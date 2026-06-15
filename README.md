# Real-Time Predictive Maintenance Data Pipeline

-Project Summary
This project is a high-throughput, real-time distributed data pipeline designed to simulate, process, and visualize industrial IoT telemetry. Built as a thesis project, it ingests machine sensor data, applies streaming logic to categorize operational risk, and persists the data for live monitoring to prevent catastrophic equipment failure.

The system acts as a "Digital Twin" for Machine `M23793`, generating real-time metrics (Torque, RPM) and processing them with sub-2-second latency.

-Next Phase of Development
The pipeline is currently stable with single-variable risk thresholds. The next phase of development focuses on implementing **Multi-Variable Physics Correlations** within the Apache PySpark streaming engine, including:
* **Power Failure (PWF) Prediction:** Correlating `Torque * Rotational speed`.
* **Heat Dissipation Failure (HDF) Prediction:** Mapping `Process temp - Air temp` against `Rotational speed`.
* **Overstrain Failure (OSF) Prediction:** Calculating dynamic thresholds based on `Tool wear * Torque`.

-System Architecture
1. **Ingestion (Apache Kafka):** Python-based producer simulating live telemetry.
2. **Processing (Apache PySpark):** Structured Streaming engine for real-time risk classification.
3. **Storage (Apache Cassandra):** NoSQL time-series database.
4. **Visualization (Grafana):** Live control room dashboard.

---

-How to Run This System

### 1. Prerequisites
Ensure your system has the following installed:
* **Docker & Docker Compose**
* **Python 3.8+**
* **Java 8 or 11** (Required for PySpark to run locally)

### 2. Install Python Dependencies
Clone this repository and install the required libraries:
```bash
git clone [https://github.com/cOrlbr/predictive-maintenance-pipeline.git](https://github.com/cOrlbr/predictive-maintenance-pipeline.git)
cd predictive-maintenance-pipeline
pip install -r requirements.txt
