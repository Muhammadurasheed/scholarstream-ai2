# Alternative Approach: Direct Kafka Consumer in Cloud Function

Since Confluent Cloud doesn't have a Pub/Sub **Sink** connector (only Source), we have **two better alternatives**:

## Option 1: HTTP-Triggered Cloud Function (RECOMMENDED FOR NOW)

**How it works:**
1. Deploy Cloud Function with HTTP trigger ✅ (what we just did)
2. Create a simple Python script that runs continuously
3. Script consumes from `raw-opportunities-stream` 
4. For each message, calls the Cloud Function via HTTP
5. Cloud Function enriches and publishes to `enriched-opportunities-stream`

**Advantages:**
- ✅ No need for Pub/Sub connector
- ✅ Simpler architecture
- ✅ Works immediately
- ✅ Easy to test and debug

**Implementation:**

### Step 1: Deploy Cloud Function (Already Done!)

Your Cloud Function is now deployed with HTTP trigger.

### Step 2: Create Kafka-to-HTTP Bridge

Create this file: `backend/kafka_to_cloud_function.py`

```python
"""
Kafka to Cloud Function Bridge
Consumes from raw-opportunities-stream and calls Cloud Function
"""
import os
import json
import time
import requests
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv
import structlog

load_dotenv()

logger = structlog.get_logger()

# Configuration
FUNCTION_URL = os.getenv('CLOUD_FUNCTION_URL')  # Get from deploy.bat output
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
    """Call Cloud Function with message data"""
    try:
        # Format message like Pub/Sub would
        payload = {
            "message": {
                "data": message_data  # Already base64 encoded by Kafka
            }
        }
        
        response = requests.post(
            FUNCTION_URL,
            json=payload,
            timeout=30
        )
        
        if response.status_code == 200:
            logger.info("Cloud Function called successfully")
            return True
        else:
            logger.error(f"Cloud Function error: {response.status_code} - {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"Failed to call Cloud Function: {e}")
        return False

def main():
    """Main consumer loop"""
    logger.info("Starting Kafka to Cloud Function bridge")
    
    if not FUNCTION_URL:
        logger.error("CLOUD_FUNCTION_URL not set in .env")
        return
    
    consumer = create_consumer()
    
    try:
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
            logger.info(f"Received message from Kafka")
            
            # Call Cloud Function
            success = call_cloud_function(message_value)
            
            if success:
                logger.info("Message processed successfully")
            else:
                logger.warning("Message processing failed")
            
            time.sleep(0.1)  # Small delay to avoid overwhelming Cloud Function
            
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
```

### Step 3: Add to .env

```bash
# Add this line to backend/.env
CLOUD_FUNCTION_URL=https://us-central1-scholarstream-i4i.cloudfunctions.net/stream-processor
```

### Step 4: Run the Bridge

```cmd
cd backend
python kafka_to_cloud_function.py
```

This will:
1. ✅ Consume messages from `raw-opportunities-stream`
2. ✅ Call your Cloud Function for each message
3. ✅ Cloud Function enriches with Gemini
4. ✅ Cloud Function publishes to `enriched-opportunities-stream`

---

## Option 2: Cloud Run Service (PRODUCTION RECOMMENDED)

**For production**, deploy the bridge as a Cloud Run service that runs 24/7.

### Benefits:
- ✅ Always running (no need to keep your computer on)
- ✅ Auto-scales
- ✅ Managed by Google Cloud
- ✅ Integrated logging and monitoring

### Quick Deploy:

1. Create `backend/kafka_bridge/Dockerfile`:
```dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY kafka_to_cloud_function.py .

CMD ["python", "kafka_to_cloud_function.py"]
```

2. Deploy to Cloud Run:
```cmd
gcloud run deploy kafka-bridge ^
  --source=backend/kafka_bridge ^
  --region=us-central1 ^
  --set-env-vars="CLOUD_FUNCTION_URL=https://...,CONFLUENT_BOOTSTRAP_SERVERS=...,CONFLUENT_API_KEY=...,CONFLUENT_API_SECRET=..." ^
  --min-instances=1 ^
  --max-instances=1
```

---

## Why This is Better Than Pub/Sub Connector

1. **Simpler**: No need for Pub/Sub topic or connector configuration
2. **Direct**: Kafka → Cloud Function → Kafka (fewer hops)
3. **Flexible**: Easy to add retry logic, batching, or filtering
4. **Cost-effective**: No Pub/Sub costs
5. **Easier debugging**: All logs in one place

---

## Testing the Complete Flow

### 1. Deploy Cloud Function
```cmd
cd backend\cloud_functions\stream_processor
deploy.bat
```

### 2. Get Function URL
The deploy script will output the URL. Copy it.

### 3. Add to .env
```bash
CLOUD_FUNCTION_URL=https://us-central1-scholarstream-i4i.cloudfunctions.net/stream-processor
```

### 4. Test Cloud Function Directly
```cmd
test_cloud_function.bat
```

### 5. Run Kafka Bridge
```cmd
cd backend
python kafka_to_cloud_function.py
```

### 6. Publish Test Message
```cmd
python test_kafka_connection.py
```

### 7. Verify Flow
1. Check Kafka bridge logs - should show "Received message from Kafka"
2. Check Cloud Function logs - should show "Enriching opportunity"
3. Check Confluent UI - `enriched-opportunities-stream` should have new message

---

## Next Steps After This Works

Once you verify the flow works:

1. **Deploy to Cloud Run** (so it runs 24/7)
2. **Add monitoring** (Cloud Monitoring alerts)
3. **Optimize** (batching, caching, etc.)
4. **Move to Phase 3** (WebSocket real-time dashboard)

---

## Summary

**What we're doing:**
- ❌ NOT using Pub/Sub Sink connector (doesn't exist in Confluent)
- ✅ Using HTTP-triggered Cloud Function
- ✅ Creating a simple Kafka consumer bridge
- ✅ Bridge calls Cloud Function for each message
- ✅ Cloud Function enriches and publishes to Kafka

**This is actually BETTER because:**
- Simpler architecture
- Fewer moving parts
- Easier to debug
- No Pub/Sub costs
- More control over message flow

Bismillah, let's get this working! 🚀
