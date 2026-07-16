import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

def generate_stock_tick(symbol):

    return {

        "ticker": symbol,

        "price": round(random.uniform(100.0, 500.0), 2),

        "volume": random.randint(1000, 10000),

        "timestamp": datetime.now().isoformat()

    }

SYMBOLS = ["AAPL", "GOOG", "TSLA"]

# Connects to localhost:9092 

producer = KafkaProducer(

    bootstrap_servers=['localhost:9092'],

    value_serializer=lambda v: json.dumps(v).encode('utf-8'),

    key_serializer=lambda k: k.encode('utf-8')

)

TOPIC_NAME = "stock_prices"

print("Starting streaming stock ticks to Kafka... Press Ctrl+C to stop.")

try:
    while True:
        chosen_symbol = random.choice(SYMBOLS)

        tick_data = generate_stock_tick(chosen_symbol)

        producer.send(

            topic=TOPIC_NAME,

            key=chosen_symbol,

            value=tick_data

        ) 
        print(f"Sent: {tick_data}")
        time.sleep(1)

except KeyboardInterrupt:
    print("\nStreaming stopped by user.")

finally:
    producer.flush()
    producer.close()