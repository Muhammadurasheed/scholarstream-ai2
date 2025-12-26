@echo off
REM Redeploy Cloud Function with fixed HTTP handler

echo.
echo ============================================================
echo Redeploying Cloud Function with HTTP Handler Fix
echo ============================================================
echo.

set PROJECT_ID=scholarstream-i4i
set REGION=us-central1
set FUNCTION_NAME=stream-processor
set ENTRY_POINT=process_kafka_message

set CONFLUENT_BOOTSTRAP=pkc-619z3.us-east1.gcp.confluent.cloud:9092
set CONFLUENT_API_KEY=LIDMJJ7VE5BHVPMZ
set CONFLUENT_API_SECRET=cflt2UhtDD1Dm2tl/nbsoeGrJ1EADPkuaeVwo0V6TzpIWxeNWv866ko1fTRcCYnQ

echo [*] Deploying Cloud Function...
echo    This may take 2-3 minutes...
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
    --memory=1024MB ^
    --timeout=540s

echo.
echo [SUCCESS] Deployment complete!
echo.
echo Next: Run test_cloud_function.bat to test
echo.

pause
