@echo off
REM Cloud Function Deployment Script for ScholarStream (Windows)
REM Deploys AI-powered stream processor to Google Cloud Functions Gen 2

echo.
echo ============================================================
echo ScholarStream Cloud Function Deployment
echo ============================================================
echo.

REM Configuration
set PROJECT_ID=scholarstream-i4i
set REGION=us-central1
set FUNCTION_NAME=stream-processor
set ENTRY_POINT=process_kafka_message

REM Confluent Cloud Configuration
set CONFLUENT_BOOTSTRAP=pkc-619z3.us-east1.gcp.confluent.cloud:9092
set CONFLUENT_API_KEY=LIDMJJ7VE5BHVPMZ
set CONFLUENT_API_SECRET=cflt2UhtDD1Dm2tl/nbsoeGrJ1EADPkuaeVwo0V6TzpIWxeNWv866ko1fTRcCYnQ

REM Check if gcloud is installed
where gcloud >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] gcloud CLI not found
    echo Please install: https://cloud.google.com/sdk/docs/install
    pause
    exit /b 1
)

echo [OK] gcloud CLI found
echo.

REM Set project
echo [*] Setting GCP project to: %PROJECT_ID%
gcloud config set project %PROJECT_ID% 2>nul
echo [OK] Project set
echo.

REM Enable required APIs
echo [*] Enabling required Google Cloud APIs...
echo    - Cloud Functions API
gcloud services enable cloudfunctions.googleapis.com --quiet
echo    - Cloud Build API
gcloud services enable cloudbuild.googleapis.com --quiet
echo    - Vertex AI API
gcloud services enable aiplatform.googleapis.com --quiet
echo    - Eventarc API
gcloud services enable eventarc.googleapis.com --quiet
echo    - Cloud Run API
gcloud services enable run.googleapis.com --quiet
echo [OK] APIs enabled
echo.

REM Deploy Cloud Function
echo [*] Deploying Cloud Function: %FUNCTION_NAME%
echo    Region: %REGION%
echo    Runtime: python311
echo    Entry point: %ENTRY_POINT%
echo    Trigger: HTTP (will change to Pub/Sub later)
echo.
echo This may take 2-3 minutes...
echo.

cd /d "%~dp0"

gcloud functions deploy %FUNCTION_NAME% ^
    --gen2 ^
    --runtime=python311 ^
    --region=%REGION% ^
    --source=. ^
    --entry-point=%ENTRY_POINT% ^
    --trigger-http ^
    --allow-unauthenticated ^
    --set-env-vars="GCP_PROJECT_ID=%PROJECT_ID%,GCP_LOCATION=%REGION%,CONFLUENT_BOOTSTRAP_SERVERS=%CONFLUENT_BOOTSTRAP%,CONFLUENT_API_KEY=%CONFLUENT_API_KEY%,CONFLUENT_API_SECRET=%CONFLUENT_API_SECRET%" ^
    --max-instances=10 ^
    --min-instances=0 ^
    --memory=1024MB ^
    --timeout=540s

if %ERRORLEVEL% NEQ 0 (
    echo.
    echo [ERROR] Deployment failed!
    echo Check the error message above for details.
    pause
    exit /b 1
)

echo.
echo ============================================================
echo [SUCCESS] Cloud Function deployed successfully!
echo ============================================================
echo.

REM Get function URL
echo [*] Getting function URL...
for /f "delims=" %%i in ('gcloud functions describe %FUNCTION_NAME% --region=%REGION% --gen2 --format="value(serviceConfig.uri)"') do set FUNCTION_URL=%%i
echo.
echo Function URL: %FUNCTION_URL%
echo.

echo Next Steps:
echo 1. Test the function with: test_cloud_function.bat
echo 2. Set up Pub/Sub trigger (see instructions below)
echo 3. Monitor logs: gcloud functions logs read %FUNCTION_NAME% --region=%REGION% --limit=50
echo.
echo Bismillah. Deployment complete!
echo.

pause
