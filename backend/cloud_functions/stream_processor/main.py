"""
Google Cloud Function: Kafka Stream Processor
Enriches raw data using Vertex AI (Gemini) and publishes to enriched-opportunities-stream
Supports both HTTP and CloudEvent (Pub/Sub) triggers
"""
import json
import base64
import os
from typing import Dict, Any, List
from datetime import datetime
from confluent_kafka import Producer
import functions_framework
from google.cloud import aiplatform
from vertexai.generative_models import GenerativeModel
from vertexai.language_models import TextEmbeddingModel


# Configuration
GCP_PROJECT = os.getenv('GCP_PROJECT_ID')
GCP_LOCATION = os.getenv('GCP_LOCATION', 'us-central1')
GEMINI_MODEL = 'gemini-2.0-flash-exp'
EMBEDDING_MODEL = 'text-embedding-004'

CONFLUENT_BOOTSTRAP = os.getenv('CONFLUENT_BOOTSTRAP_SERVERS')
CONFLUENT_API_KEY = os.getenv('CONFLUENT_API_KEY')
CONFLUENT_API_SECRET = os.getenv('CONFLUENT_API_SECRET')
ENRICHED_TOPIC = 'enriched-opportunities-stream'

# Kafka producer config (will be initialized per request)
producer_config = {
    'bootstrap.servers': CONFLUENT_BOOTSTRAP,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': CONFLUENT_API_KEY,
    'sasl.password': CONFLUENT_API_SECRET,
    'acks': 'all',
}

# Initialize Vertex AI
aiplatform.init(project=GCP_PROJECT, location=GCP_LOCATION)

# Global producer instance (will be created on first use)
_producer = None

def get_producer():
    """Get or create Kafka producer instance"""
    global _producer
    if _producer is None:
        print("Initializing Kafka producer...")
        _producer = Producer(producer_config)
        print(f"Producer initialized with bootstrap: {CONFLUENT_BOOTSTRAP}")
    return _producer


def extract_eligibility_criteria(raw_data: Dict[str, Any], gemini_model) -> Dict[str, Any]:
    """Use Gemini to extract structured eligibility from raw opportunity data"""
    prompt = f"""
You are an expert at analyzing scholarship and opportunity data. Extract structured eligibility criteria.

OPPORTUNITY DATA:
Name: {raw_data.get('name', 'Unknown')}
Organization: {raw_data.get('organization', 'Unknown')}
Description: {raw_data.get('description', '')}
Type: {raw_data.get('type', 'scholarship')}

TASK:
Provide a JSON response with the following structure:
{{
    "gpa_min": float or null,
    "grade_levels": ["High School", "Undergraduate", "Graduate"],
    "majors": ["Computer Science", "Engineering", ...],
    "citizenship": ["US Citizen", "International", "Any"],
    "geographic": ["Nationwide", "California", "Online", ...]
}}

Only include fields with actual requirements. Use null or empty arrays for unspecified criteria.
"""

    try:
        response = gemini_model.generate_content(prompt)
        text = response.text.strip()

        if text.startswith('```json'):
            text = text[7:]
        if text.endswith('```'):
            text = text[:-3]
        text = text.strip()

        eligibility = json.loads(text)
        return eligibility

    except Exception as e:
        print(f"Gemini eligibility extraction failed: {e}")
        return raw_data.get('eligibility', {})


def generate_embeddings(text: str) -> List[float]:
    """Generate vector embeddings using Vertex AI Text Embedding model"""
    try:
        model = TextEmbeddingModel.from_pretrained(EMBEDDING_MODEL)
        embeddings = model.get_embeddings([text])

        if embeddings and len(embeddings) > 0:
            return embeddings[0].values

        return []

    except Exception as e:
        print(f"Embedding generation failed: {e}")
        return []


def enrich_opportunity(raw_message: Dict[str, Any]) -> Dict[str, Any]:
    """Main enrichment pipeline: structure data + generate embeddings"""
    source = raw_message.get('source', 'unknown')
    raw_data = raw_message.get('raw_data', {})
    scraped_at = raw_message.get('scraped_at')

    print(f"Enriching opportunity from {source}: {raw_data.get('name', 'Unknown')}")

    gemini_model = GenerativeModel(GEMINI_MODEL)
    eligibility = extract_eligibility_criteria(raw_data, gemini_model)

    description_text = (
        f"{raw_data.get('name', '')} "
        f"{raw_data.get('description', '')} "
        f"{' '.join(raw_data.get('tags', []))}"
    )

    embedding_vector = generate_embeddings(description_text[:1000])

    enriched_opportunity = {
        'id': f"{source}_{raw_data.get('name', '').lower().replace(' ', '_')}_{int(datetime.utcnow().timestamp())}",
        'name': raw_data.get('name', 'Unknown Opportunity'),
        'organization': raw_data.get('organization', 'Unknown'),
        'type': raw_data.get('type', 'scholarship'),
        'amount': raw_data.get('amount', 0),
        'amount_display': raw_data.get('amount_display', '$0'),
        'deadline': raw_data.get('deadline'),
        'deadline_type': raw_data.get('deadline_type', 'fixed'),
        'url': raw_data.get('url') or raw_data.get('source_url', ''),
        'description': raw_data.get('description', ''),
        'eligibility': eligibility,
        'requirements': raw_data.get('requirements', {}),
        'tags': raw_data.get('tags', []),
        'urgency': raw_data.get('urgency', 'future'),
        'competition_level': raw_data.get('competition_level', 'Medium'),
        'embedding': embedding_vector,
        'source': source,
        'scraped_at': scraped_at,
        'enriched_at': datetime.utcnow().isoformat(),
    }

    print(f"Enrichment complete. Embedding size: {len(embedding_vector)}")

    return enriched_opportunity


def publish_to_enriched_stream(enriched_data: Dict[str, Any]):
    """Publish enriched opportunity to Confluent enriched-opportunities-stream"""
    try:
        producer = get_producer()
        
        message_value = json.dumps(enriched_data).encode('utf-8')
        message_key = enriched_data['source'].encode('utf-8')

        print(f"Publishing to {ENRICHED_TOPIC}...")
        producer.produce(
            topic=ENRICHED_TOPIC,
            key=message_key,
            value=message_value,
            callback=delivery_callback
        )

        producer.poll(0)
        print(f"Message queued for {ENRICHED_TOPIC}: {enriched_data['name']}")

    except Exception as e:
        print(f"Failed to publish to Kafka: {e}")
        import traceback
        traceback.print_exc()
        raise


def delivery_callback(err, msg):
    """Kafka delivery callback"""
    if err:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}")


# HTTP Entry Point (for testing and Kafka bridge)
@functions_framework.http
def process_kafka_message(request):
    """
    HTTP entry point - handles both direct JSON and Pub/Sub format
    This is the main entry point used by deploy.bat
    """
    try:
        # Get JSON data from request
        request_json = request.get_json(silent=True)
        
        if not request_json:
            return {"error": "No JSON data provided"}, 400
        
        # Check if it's Pub/Sub format (has "message" wrapper)
        if "message" in request_json and "data" in request_json["message"]:
            # Pub/Sub format - decode base64
            message_data = base64.b64decode(request_json["message"]["data"]).decode('utf-8')
            raw_message = json.loads(message_data)
        else:
            # Direct JSON format
            raw_message = request_json
        
        print(f"Received HTTP message from source: {raw_message.get('source')}")
        
        # Process the message
        enriched = enrich_opportunity(raw_message)
        
        # Publish to Kafka
        publish_to_enriched_stream(enriched)
        
        # CRITICAL: Flush producer to ensure message is delivered
        producer = get_producer()
        print("Flushing Kafka producer...")
        
        # Poll for delivery callbacks before flush
        print(f"Polling for delivery reports...")
        num_messages = producer.flush(timeout=10)
        
        if num_messages > 0:
            print(f"WARNING: {num_messages} messages still in queue after flush!")
            return {"status": "error", "message": f"{num_messages} messages failed to deliver"}, 500
        else:
            print("Kafka producer flushed successfully - all messages delivered")
        
        return {"status": "success", "enriched": enriched}, 200
        
    except Exception as e:
        print(f"Error processing HTTP request: {e}")
        import traceback
        traceback.print_exc()
        return {"status": "error", "message": str(e)}, 500
