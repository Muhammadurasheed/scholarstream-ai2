
import asyncio
import json
import structlog
from confluent_kafka import Consumer, KafkaException

from app.config import settings
from app.services.kafka_config import KafkaConfig

# Configure simple logger
structlog.configure(
    processors=[structlog.processors.JSONRenderer()],
)
logger = structlog.get_logger()

async def verify_stream():
    """
    Consumes from 'raw-html-stream' to verify data flow.
    """
    print("🔭 ScholarStream Data Scope Configured...")
    print(f"   Topic: {KafkaConfig.RAW_HTML_TOPIC}")
    print(f"   Broker: {settings.confluent_bootstrap_servers}")
    print("   Waiting for crawled data... (Press Ctrl+C to stop)")

    conf = {
        'bootstrap.servers': settings.confluent_bootstrap_servers,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': settings.confluent_api_key,
        'sasl.password': settings.confluent_api_secret,
        'group.id': 'verifier-script-v1',
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(conf)
    consumer.subscribe([KafkaConfig.RAW_HTML_TOPIC])

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"❌ Kafka Error: {msg.error()}")
                    continue

            # Process Message
            try:
                data = json.loads(msg.value().decode('utf-8'))
                url = data.get('url', 'Unknown')
                source = data.get('source', 'Unknown')
                size = len(data.get('html', ''))
                
                print(f"\n✅ DATA PACKET RECEIVED:")
                print(f"   🌍 URL: {url}")
                print(f"   📦 Size: {size} bytes")
                print(f"   🔍 Source: {source}")
                print(f"   🤖 Method: {data.get('method')}")
                print("-" * 50)
                
            except Exception as e:
                print(f"⚠️  Parse Error: {e}")

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    if not settings.confluent_api_key:
        print("❌ Error: .env environment variables not loaded/found.")
    else:
        asyncio.run(verify_stream())
