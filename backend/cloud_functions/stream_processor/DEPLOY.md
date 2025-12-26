# Google Cloud Function: Kafka Stream Processor

## Purpose
This Cloud Function processes raw opportunities from the `raw-opportunities-stream` Confluent Kafka topic, enriches them using Vertex AI (Gemini + Embeddings), and publishes the enriched data to `enriched-opportunities-stream`.

## Architecture Flow
```
Confluent: raw-opportunities-stream
          ↓
    Cloud Function (this)
          ↓
    Vertex AI (Gemini) → Structure Eligibility
    Vertex AI (Embeddings) → Generate Vectors
          ↓
Confluent: enriched-opportunities-stream
```

## Prerequisites

1. **Google Cloud Project** with billing enabled
2. **Confluent Cloud** cluster with topics created:
   - `raw-opportunities-stream` (input)
   - `enriched-opportunities-stream` (output)
3. **Vertex AI API** enabled in GCP
4. **Cloud Functions API** enabled in GCP

## Environment Variables

Set these when deploying the function:

```bash
GCP_PROJECT_ID=your-gcp-project-id
GCP_LOCATION=us-central1
CONFLUENT_BOOTSTRAP_SERVERS=pkc-xxxxx.region.provider.confluent.cloud:9092
CONFLUENT_API_KEY=your-confluent-api-key
CONFLUENT_API_SECRET=your-confluent-api-secret
```

## Deployment via gcloud CLI

### 1. Authenticate
```bash
gcloud auth login
gcloud config set project YOUR_PROJECT_ID
```

### 2. Create Confluent Trigger

First, create an Eventarc trigger for Confluent Cloud:

```bash
gcloud eventarc triggers create scholarstream-kafka-trigger \
  --destination-run-service=stream-processor \
  --destination-run-region=us-central1 \
  --event-filters="type=google.cloud.pubsub.topic.v1.messagePublished" \
  --service-account=YOUR_SERVICE_ACCOUNT@YOUR_PROJECT.iam.gserviceaccount.com
```

**Note**: For direct Kafka trigger integration, you'll need to set up a Confluent Cloud Source Connector to push messages to Pub/Sub or use Confluent's Kafka trigger for Cloud Functions (requires Confluent partnership setup).

### 3. Deploy Cloud Function

```bash
gcloud functions deploy stream-processor \
  --gen2 \
  --runtime=python311 \
  --region=us-central1 \
  --source=. \
  --entry-point=process_kafka_message \
  --trigger-topic=YOUR_PUBSUB_TOPIC \
  --set-env-vars=GCP_PROJECT_ID=your-project,GCP_LOCATION=us-central1,CONFLUENT_BOOTSTRAP_SERVERS=pkc-xxxxx.region.provider.confluent.cloud:9092,CONFLUENT_API_KEY=your-key,CONFLUENT_API_SECRET=your-secret \
  --max-instances=10 \
  --memory=512MB \
  --timeout=300s
```

### 4. Alternative: Deploy with Confluent Kafka Trigger (Recommended)

If using Confluent's native Cloud Functions integration:

```bash
gcloud functions deploy stream-processor \
  --gen2 \
  --runtime=python311 \
  --region=us-central1 \
  --source=. \
  --entry-point=process_kafka_message \
  --trigger-event-filters="type=com.confluent.kafka.message" \
  --trigger-event-filters="source=//confluent.cloud/YOUR_CLUSTER_ID" \
  --set-env-vars=GCP_PROJECT_ID=your-project,GCP_LOCATION=us-central1,CONFLUENT_BOOTSTRAP_SERVERS=pkc-xxxxx.region.provider.confluent.cloud:9092,CONFLUENT_API_KEY=your-key,CONFLUENT_API_SECRET=your-secret \
  --max-instances=50 \
  --memory=1024MB \
  --timeout=540s \
  --min-instances=1
```

## Testing

### Test with gcloud
```bash
gcloud functions call stream-processor \
  --data='{
    "source": "test",
    "raw_data": {
      "name": "Test Scholarship",
      "organization": "Test Org",
      "amount": 5000,
      "type": "scholarship",
      "description": "A test scholarship for computer science students"
    },
    "scraped_at": "2024-12-05T10:00:00Z"
  }'
```

### Monitor Logs
```bash
gcloud functions logs read stream-processor --region=us-central1 --limit=50
```

## IAM Permissions

Ensure your Cloud Function service account has:
- `roles/aiplatform.user` (for Vertex AI)
- `roles/pubsub.publisher` (if using Pub/Sub)
- `roles/eventarc.eventReceiver` (for Eventarc triggers)

```bash
gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member="serviceAccount:YOUR_SERVICE_ACCOUNT@YOUR_PROJECT.iam.gserviceaccount.com" \
  --role="roles/aiplatform.user"
```

## Cost Estimation

- **Cloud Functions**: ~$0.40 per million invocations
- **Vertex AI Gemini**: ~$0.00025 per 1K characters
- **Vertex AI Embeddings**: ~$0.0001 per 1K characters
- **Estimated Cost**: Processing 1000 opportunities = ~$0.80

## Monitoring

View function metrics in GCP Console:
- https://console.cloud.google.com/functions/list
- Select `stream-processor` function
- View "Metrics" and "Logs" tabs

## Troubleshooting

### Function not triggering
- Verify Confluent connector is pushing to correct topic
- Check Eventarc trigger configuration
- Review Cloud Functions logs for errors

### Vertex AI errors
- Ensure Vertex AI API is enabled
- Verify service account permissions
- Check quota limits in GCP Console

### Kafka connection issues
- Verify Confluent credentials
- Check network/firewall rules
- Ensure correct bootstrap server address

## Cleanup

Delete the function:
```bash
gcloud functions delete stream-processor --region=us-central1
```

Delete the trigger:
```bash
gcloud eventarc triggers delete scholarstream-kafka-trigger
```
