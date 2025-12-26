# Manual Cloud Function Deployment Commands

If the batch script isn't working, run these commands one by one in Command Prompt:

## Step 1: Set Project
```cmd
gcloud config set project scholarstream-i4i
```

## Step 2: Enable APIs
```cmd
gcloud services enable cloudfunctions.googleapis.com
gcloud services enable cloudbuild.googleapis.com
gcloud services enable aiplatform.googleapis.com
gcloud services enable eventarc.googleapis.com
gcloud services enable run.googleapis.com
```

## Step 3: Deploy Cloud Function

**IMPORTANT:** Run this from the `stream_processor` directory:

```cmd
cd C:\Users\HP\Documents\scholarstream\backend\cloud_functions\stream_processor

gcloud functions deploy stream-processor --gen2 --runtime=python311 --region=us-central1 --source=. --entry-point=process_kafka_message --trigger-http --allow-unauthenticated --set-env-vars="GCP_PROJECT_ID=scholarstream-i4i,GCP_LOCATION=us-central1,CONFLUENT_BOOTSTRAP_SERVERS=pkc-619z3.us-east1.gcp.confluent.cloud:9092,CONFLUENT_API_KEY=LIDMJJ7VE5BHVPMZ,CONFLUENT_API_SECRET=cflt2UhtDD1Dm2tl/nbsoeGrJ1EADPkuaeVwo0V6TzpIWxeNWv866ko1fTRcCYnQ" --max-instances=10 --min-instances=0 --memory=1024MB --timeout=540s
```

## Step 4: Get Function URL

```cmd
gcloud functions describe stream-processor --region=us-central1 --gen2 --format="value(serviceConfig.uri)"
```

## Step 5: Test Function

```cmd
curl -X POST [FUNCTION_URL_FROM_STEP_4] -H "Content-Type: application/json" -d "{\"message\":{\"data\":\"eyJzb3VyY2UiOiJ0ZXN0In0=\"}}"
```

---

## Expected Output

When deployment succeeds, you should see:

```
Deploying function (may take a while - up to 2 minutes)...
✓ Deploying function... Done.
  
Function URL: https://us-central1-scholarstream-i4i.cloudfunctions.net/stream-processor
```

---

## Troubleshooting

### Error: "Permission denied"
**Solution:** Make sure you're logged in:
```cmd
gcloud auth login
```

### Error: "API not enabled"
**Solution:** Enable the API manually in GCP Console or run:
```cmd
gcloud services enable [API_NAME]
```

### Error: "Quota exceeded"
**Solution:** Check your GCP quotas in Console → IAM & Admin → Quotas

---

## After Successful Deployment

1. Copy the Function URL
2. Add to `backend/.env`:
   ```
   CLOUD_FUNCTION_URL=https://us-central1-scholarstream-i4i.cloudfunctions.net/stream-processor
   ```
3. Test with: `python test_kafka_connection.py`
4. Run bridge: `python kafka_to_cloud_function.py`
