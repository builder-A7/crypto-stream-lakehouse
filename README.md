# ⚡️ CryptoStream: Real-Time HFT Data Lakehouse

![Python](https://img.shields.io/badge/Python-3.9%2B-blue?style=for-the-badge&logo=python)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-Streaming-orange?style=for-the-badge&logo=apachespark)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-Ingestion-black?style=for-the-badge&logo=apachekafka)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-Storage-cyan?style=for-the-badge&logo=deltalake)
![Docker](https://img.shields.io/badge/Docker-Containerized-blue?style=for-the-badge&logo=docker)

## 📖 Overview
**CryptoStream** is a high-throughput financial data pipeline designed to ingest, process, and store cryptocurrency trade data in real-time. 

It simulates a **High-Frequency Trading (HFT)** environment where market data (ticks) must be captured with millisecond latency, aggregated into OHLCV (Open, High, Low, Close, Volume) metrics, and persisted with ACID properties using **Delta Lake**.

---

## 🏗 Architecture
The system follows the **Write-Audit-Publish** pattern:

1.  **Source:** Kraken WebSocket API (Live BTC/USD trades).
2.  **Ingestion:** **Apache Kafka** decouples producers from consumers to handle backpressure.
3.  **Processing:** **Spark Structured Streaming** performs windowed aggregations (VWAP, Volatility) on 1-minute intervals.
4.  **Storage:** **Delta Lake** provides versioned Parquet storage with snapshot isolation.
5.  **Serving:** Ad-hoc analytics via Spark SQL readers.



---

## 🚀 Quick Start (TL;DR)

> **New to this project?** > 👉 **[Read the Detailed Setup Guide & Troubleshooting](./SETUP.md)**

### Prerequisites
* Docker Desktop
* Java 11 (Strict Requirement for Spark)
* Python 3.9+

### Run with Make
Open **3 Terminal Windows** and run these commands in order:

```bash
# Terminal 1: Infrastructure
make up

# Terminal 2: Ingestion
make stream-data

# Terminal 3: Processing
make process-data