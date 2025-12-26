import asyncio
import sys
import os
import json
from datetime import datetime

# Add backend directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from confluent_kafka import Producer
from app.services.kafka_config import KafkaConfig
import structlog

logger = structlog.get_logger()

async def inject_bad_data():
    """
    Injects LEGACY format data (using 'title' instead of 'name') into Kafka.
    This tests if the new WebSocket 'Self-Healing' logic works.
    """
    print("🧪 STARTING CHAOS TEST: Injecting Legacy Data")
    
    kafka_config = KafkaConfig()
    conf = kafka_config.get_producer_config()
    producer = Producer(conf)
    
    # Legacy Format (No 'name', No 'description', No 'id')
    bad_record = {
        "title": "CHAOS TEST: Legacy Opportunity",
        "url": "https://test.com/chaos",
        "amount": 5000,
        "deadline": datetime.utcnow().isoformat()
        # MISSING: name, description, id, source_url
    }
    
    print(f"   -> Injecting: {bad_record['title']}")
    
    try:
        producer.produce(
            KafkaConfig.TOPIC_OPPORTUNITY_ENRICHED,
            key="test_key",
            value=json.dumps(bad_record).encode('utf-8')
        )
        producer.flush()
        print("✅ Injection Complete. Check Backend Logs for 'Received Enriched Opportunity' (Should not crash).")
    except Exception as e:
        print(f"❌ Injection Failed: {e}")

if __name__ == "__main__":
    asyncio.run(inject_bad_data())
