# Real-Time Predictive Maintenance Data Pipeline

## Project Summary
This project is a high-throughput, real-time distributed data pipeline designed to simulate, process, and visualize industrial IoT telemetry. Built as a school project, it ingests machine sensor data, applies streaming logic to categorize operational risk, and persists the data for live monitoring to prevent catastrophic equipment failure.

The system acts as a "Digital Twin" for Machine `M23793`, generating real-time metrics (Torque, RPM) and processing them with sub-2-second latency.

## Next Phase of Development
The pipeline is currently stable with single-variable risk thresholds. The next phase of development focuses on implementing **Multi-Variable Physics Correlations** within the Apache PySpark streaming engine, including:
* **Usage of Existing Dataset** Instead of `Digital Twin`.
* **Power Failure (PWF) Prediction:** Correlating `Torque * Rotational speed`.
* **Heat Dissipation Failure (HDF) Prediction:** Mapping `Process temp - Air temp` against `Rotational speed`.
* **Overstrain Failure (OSF) Prediction:** Calculating dynamic thresholds based on `Tool wear * Torque`.

## System Architecture
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
```
### 3. Launch Infrastructure (Terminal 1)
Start the Kafka broker, Zookeeper, Cassandra database, and Grafana server.
Note: Wait about 30 seconds after running this command for Cassandra to fully initialize.
```bash
docker-compose up -d
```
### 4. Start the AI Processing Brain (Terminal 2)
Launch the PySpark structured streaming job. It will wait for incoming Kafka payloads.
```bash
python3 2_pyspark_processor.py
```
### 5. Start the Factory Simulation (Terminal 3)
Launch the digital twin to begin producing and streaming telemetry data.
```bash
python3 1_kafka_producer.py
```
### 6. View the Dashboard
Open a browser and navigate to http://localhost:3000.
The live state timeline, operator status, and raw metric charts will update continuously.
To safely stop the system, press Ctrl + C in the Python terminals, then run docker-compose down to stop the containers.
