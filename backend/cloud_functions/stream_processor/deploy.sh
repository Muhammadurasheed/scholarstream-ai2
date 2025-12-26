#!/bin/bash
# Cloud Function Deployment Script for ScholarStream
# Deploys AI-powered stream processor to Google Cloud Functions Gen 2

set -e  # Exit on error

echo "🚀 ScholarStream Cloud Function Deployment"
echo "==========================================="
echo ""

# Configuration
PROJECT_ID="scholarstream-prod"  # Replace with your GCP project ID
REGION="us-central1"
FUNCTION_NAME="stream-processor"
ENTRY_POINT="process_kafka_message"

# Confluent Cloud Configuration
CONFLUENT_BOOTSTRAP="pkc-619z3.us-east1.gcp.confluent.cloud:9092"
CONFLUENT_API_KEY="LIDMJJ7VE5BHVPMZ"
CONFLUENT_API_SECRET="cflt2UhtDD1Dm2tl/nbsoeGrJ1EADPkuaeVwo0V6TzpIWxeNWv866ko1fTRcCYnQ"

# Check if gcloud is installed
if ! command -v gcloud &> /dev/null; then
    echo "❌ gcloud CLI not found. Please install: https://cloud.google.com/sdk/docs/install"
    exit 1
fi

echo "✅ gcloud CLI found"
echo ""

# Authenticate (if needed)
echo "📝 Checking authentication..."
gcloud auth list
echo ""

# Set project
echo "🔧 Setting GCP project to: $PROJECT_ID"
gcloud config set project $PROJECT_ID
echo ""

# Enable required APIs
echo "🔌 Enabling required Google Cloud APIs..."
gcloud services enable cloudfunctions.googleapis.com
gcloud services enable cloudbuild.googleapis.com
gcloud services enable aiplatform.googleapis.com
gcloud services enable eventarc.googleapis.com
gcloud services enable run.googleapis.com
echo "✅ APIs enabled"
echo ""

# Create service account if it doesn't exist
SERVICE_ACCOUNT="scholarstream-cf@${PROJECT_ID}.iam.gserviceaccount.com"
echo "👤 Checking service account: $SERVICE_ACCOUNT"

if ! gcloud iam service-accounts describe $SERVICE_ACCOUNT &> /dev/null; then
    echo "Creating service account..."
    gcloud iam service-accounts create scholarstream-cf \
        --display-name="ScholarStream Cloud Functions" \
        --description="Service account for ScholarStream stream processor"
    
    # Grant necessary roles
    echo "Granting IAM roles..."
    gcloud projects add-iam-policy-binding $PROJECT_ID \
        --member="serviceAccount:$SERVICE_ACCOUNT" \
        --role="roles/aiplatform.user"
    
    gcloud projects add-iam-policy-binding $PROJECT_ID \
        --member="serviceAccount:$SERVICE_ACCOUNT" \
        --role="roles/eventarc.eventReceiver"
    
    echo "✅ Service account created and configured"
else
    echo "✅ Service account already exists"
fi
echo ""

# Deploy Cloud Function
echo "🚀 Deploying Cloud Function: $FUNCTION_NAME"
echo "   Region: $REGION"
echo "   Runtime: python311"
echo "   Entry point: $ENTRY_POINT"
echo ""

cd "$(dirname "$0")"  # Change to script directory

gcloud functions deploy $FUNCTION_NAME \
    --gen2 \
    --runtime=python311 \
    --region=$REGION \
    --source=. \
    --entry-point=$ENTRY_POINT \
    --trigger-http \
    --allow-unauthenticated \
    --set-env-vars="GCP_PROJECT_ID=$PROJECT_ID,GCP_LOCATION=$REGION,CONFLUENT_BOOTSTRAP_SERVERS=$CONFLUENT_BOOTSTRAP,CONFLUENT_API_KEY=$CONFLUENT_API_KEY,CONFLUENT_API_SECRET=$CONFLUENT_API_SECRET" \
    --max-instances=10 \
    --min-instances=0 \
    --memory=1024MB \
    --timeout=540s \
    --service-account=$SERVICE_ACCOUNT

echo ""
echo "✅ Cloud Function deployed successfully!"
echo ""

# Get function URL
FUNCTION_URL=$(gcloud functions describe $FUNCTION_NAME --region=$REGION --gen2 --format="value(serviceConfig.uri)")
echo "📍 Function URL: $FUNCTION_URL"
echo ""

# Test the function
echo "🧪 Testing function with sample data..."
curl -X POST $FUNCTION_URL \
    -H "Content-Type: application/json" \
    -d '{
        "data": {
            "message": {
                "data": "'$(echo -n '{"source":"test","raw_data":{"name":"Test Scholarship","organization":"Test Org","amount":5000,"type":"scholarship","description":"A test scholarship for computer science students"},"scraped_at":"2025-12-05T10:00:00Z"}' | base64)'"
            }
        }
    }'

echo ""
echo ""
echo "✅ Deployment Complete!"
echo ""
echo "Next Steps:"
echo "1. Set up Confluent-to-Pub/Sub connector (or direct Kafka trigger)"
echo "2. Verify messages flow from raw-opportunities-stream to enriched-opportunities-stream"
echo "3. Monitor function logs: gcloud functions logs read $FUNCTION_NAME --region=$REGION --limit=50"
echo ""
echo "Bismillah. May this function serve students well! 🚀"
