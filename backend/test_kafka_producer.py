import sys
from pathlib import Path
import time
import os
import structlog

# Add backend to path
backend_dir = Path(__file__).parent
sys.path.insert(0, str(backend_dir))

from app.services.kafka_config import kafka_producer_manager, KafkaConfig

logger = structlog.get_logger()

def test_producer():
    print("Testing Kafka Producer...")
    
    # Check config first
    print(f"Bootstrap Servers: {os.getenv('CONFLUENT_BOOTSTRAP_SERVERS')}")
    print(f"API Key Present: {bool(os.getenv('CONFLUENT_API_KEY'))}")
    
    # Initialize
    if not kafka_producer_manager.initialize():
        print("❌ Producer initialization failed. Check credentials.")
        return
        
    print("Producer initialized.")
    
    # Send test message
    topic = KafkaConfig.RAW_OPPORTUNITIES_TOPIC
    key = "test_source"
    message = {
        "id": "test_id_123",
        "name": "Test Opportunity",
        "timestamp": time.time()
    }
    
    print(f"Sending message to topic '{topic}'...")
    
    def report_delivery(err, msg):
        if err:
            print(f"Message delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}")
    
    success = kafka_producer_manager.publish_to_stream(
        topic=topic,
        key=key,
        value=message,
        callback=report_delivery
    )

    if success:
        print("Message queued. Flushing...")
        kafka_producer_manager.flush(timeout=10.0)
    else:
        print("Failed to queue message.")

if __name__ == "__main__":
    test_producer()
