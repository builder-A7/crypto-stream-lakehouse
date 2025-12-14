# ⚡️ CryptoStream: Real-Time HFT Data Lakehouse

![Python](https://img.shields.io/badge/Python-3.11-blue?style=for-the-badge&logo=python)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-Streaming-orange?style=for-the-badge&logo=apachespark)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-Ingestion-black?style=for-the-badge&logo=apachekafka)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-Storage-cyan?style=for-the-badge&logo=deltalake)
![Docker](https://img.shields.io/badge/Docker-Containerized-blue?style=for-the-badge&logo=docker)

## 📖 Overview
**CryptoStream** is a high-throughput financial data pipeline designed to ingest, process, and store cryptocurrency trade data in real-time. 

It simulates a **High-Frequency Trading (HFT)** environment where market data (ticks) must be captured with millisecond latency, aggregated into OHLCV (Open, High, Low, Close, Volume) metrics, and persisted with ACID properties using **Delta Lake**.

### 🏗 Architecture
The system follows the **Write-Audit-Publish** pattern:

1.  **Source:** Kraken WebSocket API (Live BTC/USD trades).
2.  **Ingestion:** **Apache Kafka** decouples producers from consumers to handle backpressure.
3.  **Processing:** **Spark Structured Streaming** performs windowed aggregations (VWAP, Volatility) on 1-minute intervals.
4.  **Storage:** **Delta Lake** provides versioned Parquet storage with snapshot isolation.
5.  **Serving:** Ad-hoc analytics via Spark SQL readers.

---

## 🚀 Quick Start

### Prerequisites
* Docker Desktop
* Java 11 (Required for PySpark)
* Python 3.9+

### 1. Infrastructure Setup
Spin up the Kafka Broker and Zookeeper containers:
```bash
make up
# Or: docker-compose up -d