# Devpost Continuous Scraper - Deployment Guide

## Overview
This service runs continuously on Google Cloud Run, scraping Devpost hackathons every 5 minutes and streaming them to Confluent Kafka in real-time.

## Prerequisites
1. Google Cloud Project with billing enabled
2. Confluent Cloud cluster with topics created:
   - `raw-opportunities-stream` (3 partitions)
   - `enriched-opportunities-stream` (3 partitions)
3. gcloud CLI installed and authenticated

## Local Testing

### 1. Set Environment Variables
```bash
export CONFLUENT_BOOTSTRAP_SERVERS="pkc-xxxxx.us-central1.gcp.confluent.cloud:9092"
export CONFLUENT_API_KEY="your_api_key"
export CONFLUENT_API_SECRET="your_api_secret"
export ENVIRONMENT="development"
```

### 2. Install Dependencies
```bash
cd backend/scrapers/devpost
pip install -r requirements.txt
```

### 3. Run Locally
```bash
python main.py
```

You should see:
- Health check server starting on port 8080
- Scraping cycles every 5 minutes
- Opportunities being published to Kafka

### 4. Test Health Endpoint
```bash
curl http://localhost:8080/health
```

Expected response:
```json
{
  "status": "healthy",
  "service": "devpost-scraper",
  "timestamp": "2025-12-05T05:00:00Z"
}
```

## Cloud Run Deployment

### Option 1: Manual Deployment

```bash
# Set your GCP project
export PROJECT_ID="scholarstream-prod"
gcloud config set project $PROJECT_ID

# Build and push Docker image
cd backend/scrapers/devpost
gcloud builds submit --tag gcr.io/$PROJECT_ID/devpost-scraper

# Deploy to Cloud Run
gcloud run deploy devpost-scraper \
  --image gcr.io/$PROJECT_ID/devpost-scraper \
  --region us-central1 \
  --platform managed \
  --allow-unauthenticated \
  --min-instances 1 \
  --max-instances 1 \
  --memory 512Mi \
  --cpu 1 \
  --timeout 3600 \
  --set-env-vars="CONFLUENT_BOOTSTRAP_SERVERS=pkc-xxxxx.us-central1.gcp.confluent.cloud:9092" \
  --set-env-vars="CONFLUENT_API_KEY=your_key" \
  --set-env-vars="CONFLUENT_API_SECRET=your_secret" \
  --set-env-vars="ENVIRONMENT=production"
```

### Option 2: Automated CI/CD with Cloud Build

1. **Connect GitHub Repository**
```bash
gcloud beta builds triggers create github \
  --repo-name=scholarstream \
  --repo-owner=your-github-username \
  --branch-pattern="^main$" \
  --build-config=backend/scrapers/devpost/cloudbuild.yaml
```

2. **Push to main branch** - deployment happens automatically!

## Monitoring

### View Logs
```bash
gcloud run services logs read devpost-scraper \
  --region us-central1 \
  --limit 50
```

### Check Service Status
```bash
gcloud run services describe devpost-scraper --region us-central1
```

### Monitor in Confluent Cloud
1. Go to https://confluent.cloud
2. Navigate to your cluster
3. Click on `raw-opportunities-stream` topic
4. View "Messages" tab
5. You should see new messages every 5 minutes

## Troubleshooting

### Service keeps restarting
- Check logs: `gcloud run services logs read devpost-scraper`
- Verify Confluent credentials are correct
- Ensure topics exist in Confluent Cloud

### No messages in Kafka
- Check if streaming is enabled in logs
- Verify CONFLUENT_* environment variables are set
- Test Kafka connection locally first

### High memory usage
- Increase memory limit: `--memory 1Gi`
- Check for memory leaks in logs

## Cost Optimization

**Current Configuration:**
- 1 instance always running
- 512MB memory
- 1 vCPU
- **Estimated cost:** ~$15/month

**To reduce costs:**
- Set `--min-instances 0` (cold starts acceptable)
- Reduce scraping frequency to 15 minutes
- Use smaller memory: `--memory 256Mi`

## Scaling to Multiple Sources

To add Gitcoin, MLH, etc.:

1. Copy this directory structure
2. Replace `DevpostScraper` with appropriate scraper
3. Deploy as separate services:
   - `gitcoin-scraper`
   - `mlh-scraper`
   - `kaggle-scraper`

Each service streams to the same `raw-opportunities-stream` topic!

## Next Steps

After deployment:
1. ✅ Verify messages in Confluent Cloud
2. ✅ Deploy Cloud Function for AI enrichment (Phase 2)
3. ✅ Set up WebSocket backend (Phase 3)
4. ✅ Connect frontend real-time dashboard (Phase 3)

---

**Bismillah. May this service run flawlessly. 🚀**
