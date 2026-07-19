import json
from datetime import datetime
from kafka import KafkaConsumer


consumer = KafkaConsumer(
    'stock_prices',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest', 
    value_deserializer=lambda x: json.loads(x.decode('utf-8')) 
)

print("[-] Consumer started. Waiting for stock ticks from Kafka...")

try:
    # Continuously poll Kafka for new messages
    for message in consumer:
        # Extract the python dictionary from the message value
        data = message.value
        
        # Get the current timestamp for the [RECEIVING] log
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Extract specific fields from the stock data
        ticker = data.get('ticker')
        price = data.get('price')
        volume = data.get('volume')
        
        # Print exactly in your defined format
        print(f"[RECEIVING] {current_time} | Ticker: {ticker} | Price: ${price:.2f} | Volume: {volume}")

except KeyboardInterrupt:
    print("\n[-] Consumer stopped by user.")