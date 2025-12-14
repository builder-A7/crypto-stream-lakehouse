import json
import time
from datetime import datetime
import websocket # pip install websocket-client
from confluent_kafka import Producer

# --- Configuration ---
KAFKA_CONF = {
    'bootstrap.servers': 'localhost:29092', # Connects to the external listener defined in Docker
    'client.id': 'crypto-producer-1'
}
TOPIC_NAME = 'crypto_ticks'
KRAKEN_WS_URL = 'wss://ws.kraken.com'

# --- Kafka Setup ---
producer = Producer(KAFKA_CONF)

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by producer.poll() or producer.flush(). """
    if err is not None:
        print(f'❌ Message delivery failed: {err}')
    else:
        # Complex projects need logging. We print the offset to know WHERE in the queue it landed.
        print(f'✅ Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

# --- WebSocket Event Handlers ---

def on_open(ws):
    """ Subscribe to the Bitcoin/USD trade feed upon connection. """
    print("🔌 Connected to Kraken WebSocket.")
    subscribe_msg = {
        "event": "subscribe",
        "pair": ["XBT/USD"],
        "subscription": {"name": "trade"}
    }
    ws.send(json.dumps(subscribe_msg))
    print(f"📡 Subscribed to {subscribe_msg['pair']}")

def on_message(ws, message):
    """
    Receives raw data, transforms it, and pushes to Kafka.
    Kraken Payload Format: [channelID, [[price, volume, time, side, type, misc]], channelName, pair]
    """
    data = json.loads(message)

    # Filter out heartbeat events (lists are data, dicts are status messages)
    if isinstance(data, list):
        # The trade data is usually the second element, which is a list of trades
        # Example: [123, [['42000.50', '0.01', '1630000000.123', 'b', 'l', '']], 'trade', 'XBT/USD']
        trades = data[1]
        pair = data[-1]

        for trade in trades:
            # 1. ETL: Extract & Transform
            # We explicitly cast types here to ensure Schema consistency for Spark later
            record = {
                "pair": pair,
                "price": float(trade[0]),
                "volume": float(trade[1]),
                "timestamp": float(trade[2]),
                "side": trade[3], # 'b' = buy, 's' = sell
                "ingest_ts": datetime.utcnow().isoformat() # Audit trail
            }

            # 2. Serialize to JSON string
            record_value = json.dumps(record)
            
            # 3. Load to Kafka
            # We use 'pair' as the key to ensure all BTC data goes to the same partition if we scale up
            producer.produce(
                TOPIC_NAME, 
                key=pair, 
                value=record_value, 
                callback=delivery_report
            )
            
            # Serve delivery reports (callbacks) from previous produce() calls
            producer.poll(0)

def on_error(ws, error):
    print(f"⚠️ Error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("🛑 Connection Closed.")
    # Ensure any buffered messages are sent before exiting
    producer.flush()

# --- Main Execution ---
if __name__ == "__main__":
    # Create the Websocket App with our defined hooks
    ws_app = websocket.WebSocketApp(
        KRAKEN_WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    
    # Run the persistent connection
    try:
        ws_app.run_forever()
    except KeyboardInterrupt:
        print("Interrupted by user")