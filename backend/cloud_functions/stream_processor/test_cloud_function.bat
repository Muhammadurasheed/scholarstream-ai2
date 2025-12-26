@echo off
REM Test Cloud Function by sending HTTP request
REM This simulates what Pub/Sub would send

echo.
echo ============================================================
echo Testing Cloud Function
echo ============================================================
echo.

set REGION=us-central1
set FUNCTION_NAME=stream-processor
set PROJECT_ID=scholarstream-i4i

echo [*] Getting function URL...
for /f "delims=" %%i in ('gcloud functions describe %FUNCTION_NAME% --region=%REGION% --gen2 --format="value(serviceConfig.uri)"') do set FUNCTION_URL=%%i

echo Function URL: %FUNCTION_URL%
echo.

echo [*] Sending test message...
echo.

REM Create test payload (simulating Pub/Sub message format)
echo { > test_payload.json
echo   "message": { >> test_payload.json
echo     "data": "eyJzb3VyY2UiOiJ0ZXN0IiwicmF3X2RhdGEiOnsibmFtZSI6IlRlc3QgU2Nob2xhcnNoaXAiLCJvcmdhbml6YXRpb24iOiJUZXN0IE9yZyIsImFtb3VudCI6NTAwMCwidHlwZSI6InNjaG9sYXJzaGlwIiwiZGVzY3JpcHRpb24iOiJBIHRlc3Qgc2Nob2xhcnNoaXAifSwic2NyYXBlZF9hdCI6IjIwMjUtMTItMDZUMTY6MDA6MDBaIn0=" >> test_payload.json
echo   } >> test_payload.json
echo } >> test_payload.json

curl -X POST %FUNCTION_URL% ^
  -H "Content-Type: application/json" ^
  -d @test_payload.json

echo.
echo.
echo [*] Check function logs for results:
echo gcloud functions logs read %FUNCTION_NAME% --region=%REGION% --limit=20
echo.

del test_payload.json

pause
