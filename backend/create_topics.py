
from confluent_kafka.admin import AdminClient, NewTopic
from app.services.kafka_config import KafkaConfig, logger

def create_raw_html_topic():
    config = KafkaConfig()
    admin_client = AdminClient({
        'bootstrap.servers': config.bootstrap_servers,
        'sasl.mechanism': 'PLAIN',
        'security.protocol': 'SASL_SSL',
        'sasl.username': config.api_key,
        'sasl.password': config.api_secret
    })

    topic_name = KafkaConfig.RAW_HTML_TOPIC
    new_topic = NewTopic(topic_name, num_partitions=3, replication_factor=3)
    
    fs = admin_client.create_topics([new_topic])
    
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            logger.info(f"Topic {topic} created")
        except Exception as e:
            logger.warning(f"Failed to create topic {topic}: {e}")

if __name__ == "__main__":
    create_raw_html_topic()
