"""
Test script to verify Kafka producer can write to enriched-opportunities-stream
Run this locally to test if credentials work
"""
import os
import json
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

# Configuration
CONFLUENT_BOOTSTRAP = os.getenv('CONFLUENT_BOOTSTRAP_SERVERS')
CONFLUENT_API_KEY = os.getenv('CONFLUENT_API_KEY')
CONFLUENT_API_SECRET = os.getenv('CONFLUENT_API_SECRET')
ENRICHED_TOPIC = 'enriched-opportunities-stream'

producer_config = {
    'bootstrap.servers': CONFLUENT_BOOTSTRAP,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': CONFLUENT_API_KEY,
    'sasl.password': CONFLUENT_API_SECRET,
    'acks': 'all',
}

print("=" * 60)
print("Testing Kafka Producer for Enriched Stream")
print("=" * 60)
print()
print(f"Bootstrap: {CONFLUENT_BOOTSTRAP}")
print(f"Topic: {ENRICHED_TOPIC}")
print()

producer = Producer(producer_config)

def delivery_callback(err, msg):
    if err:
        print(f"❌ DELIVERY FAILED: {err}")
    else:
        print(f"✅ DELIVERED: topic={msg.topic()}, partition={msg.partition()}, offset={msg.offset()}")

# Test message
test_message = {
    "id": "test_enriched_123",
    "name": "Test Enriched Scholarship",
    "source": "test",
    "embedding": [0.1] * 768,
    "enriched_at": "2025-12-07T12:00:00Z"
}

print("[*] Publishing test message to enriched-opportunities-stream...")
print()

try:
    producer.produce(
        topic=ENRICHED_TOPIC,
        key=b"test",
        value=json.dumps(test_message).encode('utf-8'),
        callback=delivery_callback
    )
    
    print("[*] Flushing producer...")
    num_remaining = producer.flush(timeout=10)
    
    if num_remaining > 0:
        print(f"❌ WARNING: {num_remaining} messages still in queue!")
    else:
        print("✅ All messages flushed successfully")
    
    print()
    print("=" * 60)
    print("Test complete! Check Confluent UI for the message.")
    print("=" * 60)
    
except Exception as e:
    print(f"❌ ERROR: {e}")
    import traceback
    traceback.print_exc()
