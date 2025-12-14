# Developer Guide & Setup Instructions

Welcome to the **CryptoStream** project! This guide details how to set up your local development environment, run the pipeline, and contribute to the codebase.

## 🛠 Prerequisites

Ensure you have the following installed before proceeding:

1.  **Docker Desktop** (Required for Kafka/Zookeeper infrastructure).
2.  **Python 3.9+**
3.  **Java 11 (JDK)** (Strictly required for PySpark 3.5.0).
    * *Mac Users:* `brew install openjdk@11`
    * *Verify:* Run `java -version` to confirm 11.x is active.

---

## ⚙️ Environment Setup

We recommend using a virtual environment to manage dependencies.

### 1. Clone the Repository
```bash
git clone [https://github.com/builder-A7/crypto-stream-lakehouse.git](https://github.com/builder-A7/crypto-stream-lakehouse.git)
cd crypto-stream-lakehouse
```

### 2. Create Virtual Environment
```bash
# Create the environment named 'venv'
python3 -m venv venv

# Activate it (Mac/Linux)
source venv/bin/activate

# Activate it (Windows)
# venv\Scripts\activate
```

### 3. Install Dependencies
```bash
# Using Makefile shortcut
make install

# OR manual pip command
pip install -r requirements.txt
```

---

## 🏃‍♂️ How to Run the Pipeline

This system requires three concurrent processes. We use `Make` to simplify commands.

### Step 1: Start Infrastructure (Docker)
Start the Kafka Broker and Zookeeper services. 
*Wait 30 seconds for the containers to initialize.*
*Verify accessibility at: [http://localhost:8080](http://localhost:8080) (Kafka UI).*

```bash
make up
```

### Step 2: Start Data Ingestion (Producer)
In a **new terminal window** (with `venv` activated), connect to the Kraken WebSocket and push ticks to the `crypto_ticks` topic.

```bash
make stream-data
```

### Step 3: Start Stream Processing (Spark)
In a **third terminal window** (with `venv` activated), start the Spark Streaming job to calculate volatility and write to Delta Lake.

```bash
make process-data
```

---

## 🔎 Verification & Analytics

To query the data landing in the Lakehouse without stopping the stream:

```bash
make read-data
```

---

## 🧹 Cleanup

To stop all services and remove the Docker containers:

```bash
make down
```

To **completely reset** the project (delete all data and logs) for a fresh start:

```bash
make clean
```