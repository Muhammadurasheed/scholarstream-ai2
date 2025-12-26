"""
Test Kafka Connection and Publishing
Verifies Confluent Cloud connectivity and message publishing
"""
import os
import sys
import json
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from confluent_kafka import Producer
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def main():
    print("=" * 60)
    print("ScholarStream - Kafka Connection Test")
    print("=" * 60)
    print()
    
    # Get configuration from environment
    bootstrap_servers = os.getenv('CONFLUENT_BOOTSTRAP_SERVERS')
    api_key = os.getenv('CONFLUENT_API_KEY')
    api_secret = os.getenv('CONFLUENT_API_SECRET')
    raw_topic = os.getenv('KAFKA_RAW_TOPIC', 'raw-opportunities-stream')
    
    print("Configuration:")
    print(f"   Bootstrap Servers: {bootstrap_servers}")
    print(f"   API Key: {api_key[:10]}..." if api_key else "   API Key: NOT SET")
    print(f"   Topic: {raw_topic}")
    print()
    
    if not all([bootstrap_servers, api_key, api_secret]):
        print("[ERROR] Missing Confluent Cloud credentials in .env file")
        print()
        print("Required environment variables:")
        print("  - CONFLUENT_BOOTSTRAP_SERVERS")
        print("  - CONFLUENT_API_KEY")
        print("  - CONFLUENT_API_SECRET")
        print("  - KAFKA_RAW_TOPIC (optional, defaults to 'raw-opportunities-stream')")
        return 1
    
    # Configure Kafka producer
    config = {
        'bootstrap.servers': bootstrap_servers,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': api_key,
        'sasl.password': api_secret,
        'acks': 'all',
        'client.id': 'scholarstream-test-client'
    }
    
    print("[*] Connecting to Confluent Cloud...")
    
    try:
        producer = Producer(config)
        print("[SUCCESS] Producer initialized successfully!")
        print()
        
        # Create test message
        test_message = {
            "source": "test",
            "raw_data": {
                "name": "Test Scholarship - Kafka Connection Verified",
                "organization": "ScholarStream Test",
                "amount": 5000,
                "amount_display": "$5,000",
                "type": "scholarship",
                "description": "This is a test message to verify Kafka connectivity",
                "url": "https://scholarstream.vercel.app",
                "deadline": datetime.utcnow().isoformat(),
                "tags": ["Test", "Kafka", "Confluent"]
            },
            "scraped_at": datetime.utcnow().isoformat(),
            "metadata": {
                "test": True,
                "environment": "development"
            }
        }
        
        print("[*] Publishing test message to Kafka...")
        print(f"   Topic: {raw_topic}")
        print(f"   Message: {json.dumps(test_message, indent=2)[:200]}...")
        print()
        
        # Delivery callback
        def delivery_callback(err, msg):
            if err:
                print(f"[ERROR] Message delivery FAILED: {err}")
            else:
                print(f"[SUCCESS] Message delivered successfully!")
                print(f"   Topic: {msg.topic()}")
                print(f"   Partition: {msg.partition()}")
                print(f"   Offset: {msg.offset()}")
                print(f"   Timestamp: {msg.timestamp()}")
        
        # Produce message
        producer.produce(
            topic=raw_topic,
            key='test'.encode('utf-8'),
            value=json.dumps(test_message).encode('utf-8'),
            callback=delivery_callback
        )
        
        # Wait for delivery
        print("[*] Waiting for delivery confirmation...")
        producer.flush(timeout=10)
        print()
        
        print("=" * 60)
        print("[SUCCESS!] Kafka connection is working!")
        print("=" * 60)
        print()
        print("Next steps:")
        print("  1. Check Confluent Cloud UI to see the message")
        print("  2. Deploy scrapers to Cloud Run")
        print("  3. Deploy Cloud Function for AI enrichment")
        print()
        
        return 0
        
    except Exception as e:
        print(f"[ERROR] {e}")
        print()
        print("Troubleshooting:")
        print("  1. Verify bootstrap server URL is correct")
        print("  2. Check API key and secret are valid")
        print("  3. Ensure topics exist in Confluent Cloud")
        print("  4. Check network connectivity")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
