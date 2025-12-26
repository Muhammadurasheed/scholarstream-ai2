"""
Kafka to Cloud Function Bridge
Consumes from raw-opportunities-stream and calls Cloud Function via HTTP
"""
import os
import json
import time
import base64
import requests
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv
import structlog

load_dotenv()

logger = structlog.get_logger()

# Configuration
FUNCTION_URL = os.getenv('CLOUD_FUNCTION_URL')
CONFLUENT_BOOTSTRAP = os.getenv('CONFLUENT_BOOTSTRAP_SERVERS')
CONFLUENT_API_KEY = os.getenv('CONFLUENT_API_KEY')
CONFLUENT_API_SECRET = os.getenv('CONFLUENT_API_SECRET')
RAW_TOPIC = os.getenv('KAFKA_RAW_TOPIC', 'raw-opportunities-stream')

def create_consumer():
    """Create Kafka consumer"""
    config = {
        'bootstrap.servers': CONFLUENT_BOOTSTRAP,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': CONFLUENT_API_KEY,
        'sasl.password': CONFLUENT_API_SECRET,
        'group.id': 'scholarstream-cloud-function-bridge',
        'auto.offset.reset': 'latest',
        'enable.auto.commit': True
    }
    
    consumer = Consumer(config)
    consumer.subscribe([RAW_TOPIC])
    logger.info(f"Subscribed to {RAW_TOPIC}")
    
    return consumer

def call_cloud_function(message_data):
    """Call Cloud Function with message data (formatted like Pub/Sub)"""
    try:
        # Base64 encode the message (like Pub/Sub does)
        encoded_data = base64.b64encode(message_data.encode('utf-8')).decode('utf-8')
        
        # Format like Pub/Sub message
        payload = {
            "message": {
                "data": encoded_data
            }
        }
        
        logger.info("Calling Cloud Function...")
        
        response = requests.post(
            FUNCTION_URL,
            json=payload,
            timeout=60  # Cloud Function can take up to 60s
        )
        
        if response.status_code == 200:
            logger.info("Cloud Function processed successfully", 
                       response=response.text[:200])
            return True
        else:
            logger.error(f"Cloud Function error: {response.status_code}", 
                        response=response.text[:500])
            return False
            
    except Exception as e:
        logger.error(f"Failed to call Cloud Function: {e}", exc_info=True)
        return False

def main():
    """Main consumer loop"""
    print("=" * 60)
    print("Kafka to Cloud Function Bridge")
    print("=" * 60)
    print()
    
    if not FUNCTION_URL:
        print("[ERROR] CLOUD_FUNCTION_URL not set in .env")
        print("Add this line to backend/.env:")
        print("CLOUD_FUNCTION_URL=https://us-central1-scholarstream-i4i.cloudfunctions.net/stream-processor")
        return
    
    print(f"[*] Cloud Function URL: {FUNCTION_URL}")
    print(f"[*] Kafka Topic: {RAW_TOPIC}")
    print(f"[*] Bootstrap Server: {CONFLUENT_BOOTSTRAP}")
    print()
    print("[*] Starting consumer...")
    print()
    
    consumer = create_consumer()
    
    messages_processed = 0
    
    try:
        print("[*] Waiting for messages... (Press Ctrl+C to stop)")
        print()
        
        while True:
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error(f"Kafka error: {msg.error()}")
                    continue
            
            # Process message
            message_value = msg.value().decode('utf-8')
            messages_processed += 1
            
            print(f"[{messages_processed}] Received message from Kafka")
            print(f"    Message preview: {message_value[:100]}...")
            
            # Call Cloud Function
            success = call_cloud_function(message_value)
            
            if success:
                print(f"    [SUCCESS] Message enriched and published")
            else:
                print(f"    [FAILED] Message processing failed")
            
            print()
            
            time.sleep(0.5)  # Small delay to avoid overwhelming Cloud Function
            
    except KeyboardInterrupt:
        print()
        print(f"[*] Shutting down... (Processed {messages_processed} messages)")
    finally:
        consumer.close()
        print("[*] Consumer closed")

if __name__ == "__main__":
    main()
