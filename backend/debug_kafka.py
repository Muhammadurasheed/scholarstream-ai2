import os
import sys
from confluent_kafka import AdminClient
import os
import sys
from confluent_kafka import AdminClient
from dotenv import load_dotenv

# Load env from backend folder
load_dotenv(os.path.join(os.getcwd(), 'backend', '.env'))

bootstrap_servers = os.getenv("CONFLUENT_BOOTSTRAP_SERVERS")
api_key = os.getenv("CONFLUENT_API_KEY")
api_secret = os.getenv("CONFLUENT_API_SECRET")

print(f"DEBUG: Bootstrap Servers: {bootstrap_servers}")
print(f"DEBUG: API Key present: {bool(api_key)}")
print(f"DEBUG: API Secret present: {bool(api_secret)}")

if not all([bootstrap_servers, api_key, api_secret]):
    print("❌ MISSING CREDENTIALS. Aborting.")
    sys.exit(1)

conf = {
    'bootstrap.servers': bootstrap_servers,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': api_key,
    'sasl.password': api_secret,
}

print("Attempting to connect to Confluent Cloud...")

try:
    admin_client = AdminClient(conf)
    # This call actually hits the network
    cluster_metadata = admin_client.list_topics(timeout=10)
    
    print(f"✅ CONNECTION SUCCESSFUL!")
    print(f"Brokers: {len(cluster_metadata.brokers)}")
    print(f"Topics found: {len(cluster_metadata.topics)}")
    for topic in list(cluster_metadata.topics.keys())[:5]:
        print(f" - {topic}")
        
except Exception as e:
    print(f"❌ CONNECTION FAILED: {str(e)}")
