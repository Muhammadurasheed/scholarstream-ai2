import sys
from pathlib import Path
import json
import structlog
from confluent_kafka import Consumer, KafkaError

# Add backend to path
backend_dir = Path(__file__).parent
sys.path.insert(0, str(backend_dir))

from app.services.kafka_config import KafkaConfig

logger = structlog.get_logger()

def debug_consumer():
    print("================================================================================")
    print("DEBUG KAFKA CONSUMER")
    print("================================================================================")
    
    config = KafkaConfig()
    consumer_config = config.get_consumer_config(group_id="debug-consumer-group-v1")
    
    if not consumer_config:
        print("Kafka disabled or missing credentials.")
        return

    # Add auto.offset.reset to earliest to read past messages
    consumer_config['auto.offset.reset'] = 'earliest'
    
    consumer = Consumer(consumer_config)
    
    topics = [KafkaConfig.ENRICHED_OPPORTUNITIES_TOPIC]
    consumer.subscribe(topics)
    print(f"Subscribed to topics: {topics}")
    print("Waiting for messages... (Ctrl+C to stop)")
    
    try:
        count = 0
        while True:
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Consumer error: {msg.error()}")
                    continue
            
            try:
                key = msg.key().decode('utf-8') if msg.key() else None
                value = json.loads(msg.value().decode('utf-8'))
                
                print(f"[{count+1}] Received: Key={key}, Topic={msg.topic()}")
                print(f"    Name: {value.get('raw_data', {}).get('name', 'Unknown')}")
                print(f"    Source: {value.get('source')}")
                print("-" * 50)
                
                count += 1
                if count >= 5:
                    print("Received 5 sample messages. Exiting.")
                    break
            except Exception as e:
                print(f"Error parsing message: {e}")
                
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        print("Consumer closed.")

if __name__ == "__main__":
    debug_consumer()
