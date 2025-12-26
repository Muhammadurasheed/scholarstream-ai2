# Confluent Cloud Configuration Guide

**Date:** December 5, 2025  
**Status:** ✅ Credentials Confirmed - Ready to Configure

---

## 🔑 Your Confluent Cloud Credentials

Based on the `ccloud-python-client` folder and your provided credentials:

```bash
CONFLUENT_API_KEY=LIDMJJ7VE5BHVPMZ
CONFLUENT_API_SECRET=cflt2UhtDD1Dm2tl/nbsoeGrJ1EADPkuaeVwo0V6TzpIWxeNWv866ko1fTRcCYnQ
```

---

## 📝 Step 1: Get Bootstrap Server URL

You need to get your Confluent Cloud bootstrap server URL. Here's how:

### Option A: From Confluent Cloud UI

1. Go to https://confluent.cloud
2. Navigate to your cluster
3. Click on "Cluster settings" or "Cluster overview"
4. Look for **"Bootstrap server"** - it will look like:
   ```
   pkc-xxxxx.us-central1.gcp.confluent.cloud:9092
   ```
   OR
   ```
   pkc-xxxxx.region.provider.confluent.cloud:9092
   ```

### Option B: From client.properties File

The `ccloud-python-client/client.properties` file (which is gitignored) should contain:
```
bootstrap.servers=pkc-xxxxx.region.provider.confluent.cloud:9092
```

**ACTION REQUIRED:** Please check your Confluent Cloud dashboard or the `client.properties` file and provide the bootstrap server URL.

---

## 📝 Step 2: Update backend/.env File

Once you have the bootstrap server URL, add these lines to your `backend/.env` file:

```bash
# Confluent Cloud (Kafka Event Streaming)
CONFLUENT_BOOTSTRAP_SERVERS=pkc-xxxxx.us-central1.gcp.confluent.cloud:9092  # ← Replace with your actual bootstrap server
CONFLUENT_API_KEY=LIDMJJ7VE5BHVPMZ
CONFLUENT_API_SECRET=cflt2UhtDD1Dm2tl/nbsoeGrJ1EADPkuaeVwo0V6TzpIWxeNWv866ko1fTRcCYnQ
KAFKA_RAW_TOPIC=raw-opportunities-stream
KAFKA_ENRICHED_TOPIC=enriched-opportunities-stream
KAFKA_CONSUMER_GROUP_ID=scholarstream-websocket-consumer

# WebSocket Configuration (for real-time dashboard updates)
WEBSOCKET_HEARTBEAT_INTERVAL=30
WEBSOCKET_RECONNECT_MAX_ATTEMPTS=10
```

---

## 📝 Step 3: Verify Topics Exist

Make sure these topics are created in your Confluent Cloud cluster:

1. **raw-opportunities-stream**
   - Partitions: 3
   - Retention: 7 days (168 hours)
   - Cleanup policy: delete

2. **enriched-opportunities-stream**
   - Partitions: 3
   - Retention: 30 days (720 hours)
   - Cleanup policy: delete

### How to Create Topics (if not already created):

**Via Confluent Cloud UI:**
1. Go to your cluster
2. Click "Topics" in the left menu
3. Click "Create topic"
4. Enter topic name: `raw-opportunities-stream`
5. Set partitions: 3
6. Click "Create with defaults"
7. Repeat for `enriched-opportunities-stream`

**Via CLI (if you have confluent CLI installed):**
```bash
confluent kafka topic create raw-opportunities-stream \
  --partitions 3 \
  --config retention.ms=604800000

confluent kafka topic create enriched-opportunities-stream \
  --partitions 3 \
  --config retention.ms=2592000000
```

---

## ✅ Verification Steps

### Test 1: Local Connection Test

Create a test file `backend/test_kafka_connection.py`:

```python
from confluent_kafka import Producer
import os
from dotenv import load_dotenv

load_dotenv()

config = {
    'bootstrap.servers': os.getenv('CONFLUENT_BOOTSTRAP_SERVERS'),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': os.getenv('CONFLUENT_API_KEY'),
    'sasl.password': os.getenv('CONFLUENT_API_SECRET'),
}

try:
    producer = Producer(config)
    print("✅ Successfully connected to Confluent Cloud!")
    print(f"Bootstrap servers: {config['bootstrap.servers']}")
    
    # Test message
    producer.produce(
        topic='raw-opportunities-stream',
        key='test',
        value='{"test": "connection successful"}'
    )
    producer.flush()
    print("✅ Test message sent successfully!")
    
except Exception as e:
    print(f"❌ Connection failed: {e}")
```

Run it:
```bash
cd backend
python test_kafka_connection.py
```

Expected output:
```
✅ Successfully connected to Confluent Cloud!
Bootstrap servers: pkc-xxxxx.us-central1.gcp.confluent.cloud:9092
✅ Test message sent successfully!
```

### Test 2: Verify Message in Confluent Cloud

1. Go to Confluent Cloud UI
2. Navigate to Topics → `raw-opportunities-stream`
3. Click "Messages" tab
4. You should see your test message

---

## 🚀 Next Steps After Configuration

Once `.env` is configured and verified:

1. ✅ **Test Devpost Scraper Locally**
   ```bash
   cd backend/scrapers/devpost
   python main.py
   ```

2. ✅ **Deploy to Cloud Run**
   ```bash
   gcloud builds submit --tag gcr.io/YOUR_PROJECT/devpost-scraper
   gcloud run deploy devpost-scraper \
     --image gcr.io/YOUR_PROJECT/devpost-scraper \
     --region us-central1 \
     --set-env-vars="CONFLUENT_BOOTSTRAP_SERVERS=pkc-xxxxx...,CONFLUENT_API_KEY=LIDMJJ7VE5BHVPMZ,CONFLUENT_API_SECRET=cflt2UhtDD1Dm2tl/nbsoeGrJ1EADPkuaeVwo0V6TzpIWxeNWv866ko1fTRcCYnQ"
   ```

3. ✅ **Proceed to Phase 2: Cloud Function Deployment**

---

## 🔒 Security Notes

- ✅ `.env` file is gitignored (credentials safe)
- ✅ Never commit credentials to Git
- ✅ Use environment variables for Cloud Run deployment
- ✅ Rotate API keys if accidentally exposed

---

**Bismillah. Ready to proceed once bootstrap server is confirmed! 🚀**
