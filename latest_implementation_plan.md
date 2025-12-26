ScholarStream: FAANG-Level Event-Driven Architecture Implementation Plan
Prepared by: Principal Engineering Team
Target: Google Cloud AI Partner Catalyst Hackathon - Confluent Challenge
Date: December 5, 2025
Status: Ready for Implementation

🎯 Executive Summary
Current State Analysis
After surgical analysis of the ScholarStream codebase, I've identified a solid foundation with critical gaps that prevent it from achieving the "real-time financial news terminal" vision outlined in your request.

✅ What's Working:

Comprehensive documentation (
SCHOLARSTREAM_COMPLETE_DOCUMENTATION.md
, 
SCHOLARSTREAM_BLUEPRINT.md
)
Multi-source scraping infrastructure (Devpost, Gitcoin, MLH, Kaggle, Scholarships.com)
Firebase authentication and Firestore database
React frontend with shadcn/ui components
FastAPI backend with structured logging
Chrome extension scaffold with AI-powered auto-fill
Partial Confluent implementation (kafka_config.py, websocket.py exist)
❌ Critical Gaps Identified:

Incomplete Event-Driven Architecture

kafka_config.py
 exists but scrapers aren't actively streaming
WebSocket endpoint exists but Kafka consumer isn't fully integrated
No Cloud Function for stream processing deployed
Scrapers still using poll-based APScheduler (6-hour intervals)
Missing Real-Time Components

Frontend doesn't have useRealtimeOpportunities hook implemented
No WebSocket connection in Dashboard
No live feed updates or toast notifications for new opportunities
Chrome Extension Gaps

background.js
 and 
content.js
 exist but lack backend integration
No /api/extension/map-fields endpoint for AI field mapping
Extension can't authenticate with main app (no session sharing)
Vector Search Not Implemented

Embeddings generated in Cloud Function but not stored in Firestore
No vector similarity search for RAG chat
Chat endpoint doesn't leverage semantic matching
Production Deployment Gaps

Confluent Cloud cluster not configured
Google Cloud Function not deployed
Environment variables for Kafka not set
No monitoring/observability for streaming pipeline
🏗️ Proposed Architecture: The Complete Vision
┌─────────────────────────────────────────────────────────────────────────────┐
│                    SCHOLARSTREAM EVENT-DRIVEN ECOSYSTEM                      │
└─────────────────────────────────────────────────────────────────────────────┘
┌──────────────────────────────────────────────────────────────────────────────┐
│ LAYER 1: DATA INGESTION (Continuous Producers)                              │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐       │
│  │  Devpost    │  │   Gitcoin   │  │     MLH     │  │   Kaggle    │       │
│  │  Scraper    │  │   Scraper   │  │   Scraper   │  │   Scraper   │       │
│  │             │  │             │  │             │  │             │       │
│  │ Python      │  │  Python     │  │  Python     │  │  Python     │       │
│  │ Cloud Run   │  │  Cloud Run  │  │  Cloud Run  │  │  Cloud Run  │       │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘       │
│         │                │                │                │               │
│         └────────────────┴────────────────┴────────────────┘               │
│                                  │                                          │
│                                  ▼                                          │
│                    ┌──────────────────────────────┐                         │
│                    │  Confluent Kafka Producer   │                         │
│                    │  (confluent-kafka-python)   │                         │
│                    └──────────────┬───────────────┘                         │
└────────────────────────────────────┼──────────────────────────────────────┘
                                     │
                                     ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ LAYER 2: CONFLUENT CLOUD (Message Broker)                                   │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────┐         │
│  │  Topic: raw-opportunities-stream                               │         │
│  │  Partitions: 3 | Replication: 3 | Retention: 7 days            │         │
│  │                                                                 │         │
│  │  Message Schema:                                                │         │
│  │  {                                                              │         │
│  │    "source": "devpost" | "gitcoin" | "mlh" | "kaggle",        │         │
│  │    "raw_data": {                                               │         │
│  │      "name": str,                                              │         │
│  │      "organization": str,                                      │         │
│  │      "amount": float,                                          │         │
│  │      "deadline": str,                                          │         │
│  │      "description": str,                                       │         │
│  │      "url": str,                                               │         │
│  │      "eligibility_raw": str,                                   │         │
│  │      "requirements_raw": str                                   │         │
│  │    },                                                           │         │
│  │    "scraped_at": "2025-12-05T04:51:06Z",                      │         │
│  │    "metadata": { "scraper_version": "1.0" }                   │         │
│  │  }                                                              │         │
│  └─────────────────────────────┬──────────────────────────────────┘         │
│                                │                                             │
└────────────────────────────────┼─────────────────────────────────────────────┘
                                 │
                                 ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ LAYER 3: GOOGLE CLOUD AI PROCESSING                                         │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────┐         │
│  │  Cloud Function: stream-processor                              │         │
│  │  Runtime: Python 3.11 | Region: us-central1                    │         │
│  │  Trigger: Confluent Kafka (via Pub/Sub connector)              │         │
│  │                                                                 │         │
│  │  Processing Pipeline:                                          │         │
│  │  1. Receive raw opportunity from Kafka                         │         │
│  │  2. Call Vertex AI Gemini 2.0 Flash                           │         │
│  │     - Extract structured eligibility criteria                  │         │
│  │     - Parse requirements (essays, GPA, citizenship)            │         │
│  │     - Generate tags and categorization                         │         │
│  │  3. Call Vertex AI Text Embeddings (text-embedding-004)       │         │
│  │     - Generate 768-dimensional vector                          │         │
│  │     - For semantic search and RAG                              │         │
│  │  4. Publish to enriched-opportunities-stream                   │         │
│  │                                                                 │         │
│  │  Performance: <2s latency | 95% success rate                  │         │
│  └─────────────────────────────┬──────────────────────────────────┘         │
│                                │                                             │
└────────────────────────────────┼─────────────────────────────────────────────┘
                                 │
                                 ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  ┌────────────────────────────────────────────────────────────────┐         │
│  │  Topic: enriched-opportunities-stream                          │         │
│  │  Partitions: 3 | Replication: 3 | Retention: 30 days           │         │
│  │                                                                 │         │
│  │  Message Schema:                                                │         │
│  │  {                                                              │         │
│  │    "id": "uuid",                                               │         │
│  │    "name": "Google Scholarship for CS Students",              │         │
│  │    "organization": "Google",                                   │         │
│  │    "amount": 10000,                                            │         │
│  │    "deadline": "2025-03-15T23:59:59Z",                        │         │
│  │    "eligibility": {                                            │         │
│  │      "gpa_min": 3.5,                                           │         │
│  │      "grades_eligible": ["Undergraduate", "Graduate"],        │         │
│  │      "majors": ["Computer Science"],                          │         │
│  │      "citizenship": "US Citizen"                              │         │
│  │    },                                                           │         │
│  │    "requirements": {                                           │         │
│  │      "essay": true,                                            │         │
│  │      "essay_prompts": ["Why CS?"],                            │         │
│  │      "recommendation_letters": 2,                             │         │
│  │      "transcript": true                                        │         │
│  │    },                                                           │         │
│  │    "tags": ["STEM", "Computer Science", "Merit-Based"],       │         │
│  │    "embedding": [0.123, -0.456, ...],  // 768 dimensions      │         │
│  │    "source_url": "https://...",                               │         │
│  │    "enriched_at": "2025-12-05T04:51:08Z"                      │         │
│  │  }                                                              │         │
│  └─────────────────────────────┬──────────────────────────────────┘         │
│                                │                                             │
└────────────────────────────────┼─────────────────────────────────────────────┘
                                 │
                                 ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ LAYER 4: FASTAPI BACKEND (Render/Cloud Run)                                 │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────┐         │
│  │  Kafka Consumer (Background Task)                              │         │
│  │                                                                 │         │
│  │  - Subscribes to enriched-opportunities-stream                 │         │
│  │  - Loads all connected user profiles from cache                │         │
│  │  - For each new opportunity:                                   │         │
│  │    1. Calculate match score for each user (0-100)              │         │
│  │    2. If score >= 60, push via WebSocket                       │         │
│  │    3. Store in Firestore with embedding                        │         │
│  │  - Runs continuously in asyncio background task                │         │
│  └─────────────────────────────┬──────────────────────────────────┘         │
│                                │                                             │
│  ┌────────────────────────────▼────────────────────────────────┐           │
│  │  WebSocket Manager                                           │           │
│  │                                                               │           │
│  │  Endpoint: /ws/opportunities?token=<firebase-token>          │           │
│  │                                                               │           │
│  │  Active Connections:                                         │           │
│  │  {                                                            │           │
│  │    "user_123": <WebSocket>,                                 │           │
│  │    "user_456": <WebSocket>                                  │           │
│  │  }                                                            │           │
│  │                                                               │           │
│  │  User Profile Cache:                                         │           │
│  │  {                                                            │           │
│  │    "user_123": { gpa: 3.8, major: "CS", interests: [...] }  │           │
│  │  }                                                            │           │
│  │                                                               │           │
│  │  Message Types:                                              │           │
│  │  - connection_established                                    │           │
│  │  - new_opportunity (with match_score)                        │           │
│  │  - heartbeat (every 30s)                                     │           │
│  └──────────────────────────────────────────────────────────────┘           │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────┐         │
│  │  Extension API Endpoints                                       │         │
│  │                                                                 │         │
│  │  GET  /api/extension/user-profile                             │         │
│  │  POST /api/extension/map-fields (AI-powered)                  │         │
│  │  POST /api/extension/save-application-data                    │         │
│  └────────────────────────────────────────────────────────────────┘         │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────┐         │
│  │  Firestore Operations                                          │         │
│  │                                                                 │         │
│  │  - Store enriched opportunities with embeddings                │         │
│  │  - Vector similarity search for RAG chat                       │         │
│  │  - User profile management                                     │         │
│  │  - Application tracking                                        │         │
│  └────────────────────────────────────────────────────────────────┘         │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ LAYER 5: FRONTEND (React + Vercel)                                          │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────┐         │
│  │  Real-Time Dashboard                                           │         │
│  │                                                                 │         │
│  │  const { connected, opportunities, newCount } =                │         │
│  │    useRealtimeOpportunities();                                 │         │
│  │                                                                 │         │
│  │  Features:                                                      │         │
│  │  - WebSocket connection on mount                               │         │
│  │  - Live feed updates (no refresh needed)                       │         │
│  │  - Toast notifications for urgent opportunities                │         │
│  │  - Animated card insertion for new matches                     │         │
│  │  - Connection status indicator                                 │         │
│  │  - Automatic reconnection with exponential backoff             │         │
│  └────────────────────────────────────────────────────────────────┘         │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────┐         │
│  │  Chrome Extension Integration                                  │         │
│  │                                                                 │         │
│  │  - Detects scholarship/hackathon application pages             │         │
│  │  - Injects sidebar with auto-fill UI                           │         │
│  │  - AI maps form fields to user profile                         │         │
│  │  - One-click fill for 20+ fields                               │         │
│  │  - Saves 15 minutes per application                            │         │
│  └────────────────────────────────────────────────────────────────┘         │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
📋 Implementation Roadmap
Phase 1: Complete Confluent Streaming Pipeline (Week 1)
Goal: Transform scrapers from poll-based to continuous streaming producers

1.1 Configure Confluent Cloud Cluster
Tasks:

 Create Confluent Cloud account (free trial: $400 credit)
 Provision Kafka cluster in us-central1 (GCP region)
 Create topics:
raw-opportunities-stream (3 partitions, 7-day retention)
enriched-opportunities-stream (3 partitions, 30-day retention)
 Generate API keys and secrets
 Configure Schema Registry for message validation
Files to Update:

backend/.env
Environment Variables:

# Confluent Cloud Configuration
CONFLUENT_BOOTSTRAP_SERVERS=pkc-xxxxx.us-central1.gcp.confluent.cloud:9092
CONFLUENT_API_KEY=your_api_key
CONFLUENT_API_SECRET=your_api_secret
CONFLUENT_SCHEMA_REGISTRY_URL=https://psrc-xxxxx.us-central1.gcp.confluent.cloud
CONFLUENT_SCHEMA_REGISTRY_KEY=your_sr_key
CONFLUENT_SCHEMA_REGISTRY_SECRET=your_sr_secret
# Topic Names
KAFKA_RAW_TOPIC=raw-opportunities-stream
KAFKA_ENRICHED_TOPIC=enriched-opportunities-stream
1.2 Enhance Kafka Producer in Scrapers
Current State: 
backend/app/services/kafka_config.py
 exists but not actively used

Files to Modify:

backend/app/services/scrapers/devpost.py
backend/app/services/scrapers/gitcoin.py
backend/app/services/scrapers/mlh.py
backend/app/services/scrapers/kaggle.py
Implementation:

# backend/app/services/scrapers/devpost.py
from app.services.kafka_config import get_kafka_producer
import structlog
logger = structlog.get_logger()
class DevpostScraper(BaseScraper):
    def __init__(self):
        super().__init__()
        self.kafka_producer = None
        
    async def enable_streaming(self):
        """Initialize Kafka producer for real-time streaming"""
        self.kafka_producer = get_kafka_producer()
        logger.info("Kafka producer enabled for Devpost scraper")
    
    async def scrape_and_stream(self) -> List[Dict]:
        """Scrape hackathons and stream to Kafka in real-time"""
        opportunities = []
        
        async for opportunity in self._scrape_generator():
            # Stream immediately to Kafka
            if self.kafka_producer:
                await self._publish_to_kafka(opportunity)
            
            opportunities.append(opportunity)
        
        return opportunities
    
    async def _scrape_generator(self):
        """Generator that yields opportunities one at a time"""
        # Existing scraping logic, but yield instead of append
        url = "https://devpost.com/hackathons"
        response = await self.client.get(url)
        soup = BeautifulSoup(response.text, 'lxml')
        
        for hackathon in soup.select('.hackathon-tile'):
            opportunity = self._parse_hackathon(hackathon)
            yield opportunity
    
    async def _publish_to_kafka(self, opportunity: Dict):
        """Publish opportunity to Kafka raw stream"""
        message = {
            "source": "devpost",
            "raw_data": opportunity,
            "scraped_at": datetime.utcnow().isoformat(),
            "metadata": {
                "scraper_version": "1.0",
                "environment": settings.environment
            }
        }
        
        try:
            self.kafka_producer.produce(
                topic=settings.kafka_raw_topic,
                key=opportunity.get('url', '').encode('utf-8'),
                value=json.dumps(message).encode('utf-8'),
                callback=self._delivery_callback
            )
            self.kafka_producer.poll(0)  # Trigger callbacks
            logger.info(
                "Opportunity published to Kafka",
                source="devpost",
                name=opportunity.get('name')
            )
        except Exception as e:
            logger.error(
                "Kafka publish failed",
                error=str(e),
                opportunity=opportunity.get('name')
            )
    
    def _delivery_callback(self, err, msg):
        """Callback for Kafka message delivery confirmation"""
        if err:
            logger.error("Message delivery failed", error=str(err))
        else:
            logger.debug(
                "Message delivered",
                topic=msg.topic(),
                partition=msg.partition(),
                offset=msg.offset()
            )
1.3 Deploy Continuous Scraper Services
Option A: Google Cloud Run (Recommended)

Files to Create:

backend/scrapers/devpost/Dockerfile
backend/scrapers/devpost/main.py
backend/scrapers/devpost/cloudbuild.yaml
Implementation:

# backend/scrapers/devpost/main.py
import asyncio
from app.services.scrapers.devpost import DevpostScraper
import structlog
logger = structlog.get_logger()
async def continuous_scraping_loop():
    """Run scraper continuously with smart intervals"""
    scraper = DevpostScraper()
    await scraper.enable_streaming()
    
    while True:
        try:
            logger.info("Starting Devpost scraping cycle")
            opportunities = await scraper.scrape_and_stream()
            logger.info(
                "Scraping cycle complete",
                opportunities_found=len(opportunities)
            )
            
            # Smart interval: 5 minutes for active sources
            await asyncio.sleep(300)
            
        except Exception as e:
            logger.error("Scraping error", error=str(e), exc_info=True)
            # Exponential backoff on error
            await asyncio.sleep(60)
if __name__ == "__main__":
    asyncio.run(continuous_scraping_loop())
Deployment:

# Deploy to Cloud Run
gcloud run deploy devpost-scraper \
  --source backend/scrapers/devpost \
  --region us-central1 \
  --platform managed \
  --allow-unauthenticated \
  --set-env-vars="CONFLUENT_BOOTSTRAP_SERVERS=...,CONFLUENT_API_KEY=..." \
  --min-instances=1 \
  --max-instances=1
Phase 2: Google Cloud Function for AI Enrichment (Week 1)
Goal: Deploy Cloud Function that processes raw stream with Vertex AI

2.1 Set Up Vertex AI APIs
Tasks:

 Enable Vertex AI API in GCP project
 Enable Cloud Functions Gen 2
 Create service account with Vertex AI permissions
 Configure Pub/Sub topic for Kafka connector
2.2 Deploy Stream Processor Cloud Function
Current State: 
backend/cloud_functions/stream_processor/main.py
 exists but not deployed

Files to Review:

backend/cloud_functions/stream_processor/main.py
backend/cloud_functions/stream_processor/requirements.txt
Deployment Steps:

cd backend/cloud_functions/stream_processor
# Deploy Cloud Function
gcloud functions deploy stream-processor \
  --gen2 \
  --runtime=python311 \
  --region=us-central1 \
  --source=. \
  --entry-point=process_opportunity \
  --trigger-topic=scholarstream-raw-opportunities \
  --set-env-vars="GCP_PROJECT=scholarstream-prod,CONFLUENT_BOOTSTRAP_SERVERS=...,CONFLUENT_API_KEY=..." \
  --timeout=540s \
  --memory=512MB \
  --max-instances=10
2.3 Configure Confluent-to-Pub/Sub Connector
In Confluent Cloud Console:

Navigate to Connectors
Add "Google Cloud Pub/Sub Sink Connector"
Configure:
Source Topic: raw-opportunities-stream
GCP Project: scholarstream-prod
Pub/Sub Topic: scholarstream-raw-opportunities
Message Format: JSON
Result: Every message in Kafka triggers Cloud Function automatically

Phase 3: Real-Time WebSocket Dashboard (Week 2)
Goal: Connect frontend to enriched stream via WebSocket for live updates

3.1 Complete Backend WebSocket Implementation
Current State: 
backend/app/routes/websocket.py
 exists, needs Kafka consumer integration

Files to Modify:

backend/app/routes/websocket.py
Critical Enhancement:

# backend/app/routes/websocket.py
from confluent_kafka import Consumer, KafkaError
import asyncio
from typing import Dict, Set
from fastapi import WebSocket
import structlog
logger = structlog.get_logger()
# Global connection manager
class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, WebSocket] = {}
        self.user_profiles: Dict[str, Dict] = {}
    
    async def connect(self, user_id: str, websocket: WebSocket, profile: Dict):
        await websocket.accept()
        self.active_connections[user_id] = websocket
        self.user_profiles[user_id] = profile
        logger.info("WebSocket connected", user_id=user_id)
    
    def disconnect(self, user_id: str):
        if user_id in self.active_connections:
            del self.active_connections[user_id]
            del self.user_profiles[user_id]
            logger.info("WebSocket disconnected", user_id=user_id)
    
    async def send_to_user(self, user_id: str, message: dict):
        if user_id in self.active_connections:
            try:
                await self.active_connections[user_id].send_json(message)
            except Exception as e:
                logger.error("Send failed", user_id=user_id, error=str(e))
                self.disconnect(user_id)
manager = ConnectionManager()
# Kafka consumer background task
async def kafka_consumer_task():
    """Background task that consumes enriched opportunities and pushes to WebSockets"""
    consumer = Consumer({
        'bootstrap.servers': settings.confluent_bootstrap_servers,
        'sasl.username': settings.confluent_api_key,
        'sasl.password': settings.confluent_api_secret,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'group.id': 'scholarstream-websocket-consumer',
        'auto.offset.reset': 'latest'
    })
    
    consumer.subscribe([settings.kafka_enriched_topic])
    logger.info("Kafka consumer subscribed to enriched stream")
    
    while True:
        try:
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                await asyncio.sleep(0.1)
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error("Kafka error", error=msg.error())
                    continue
            
            # Parse enriched opportunity
            opportunity = json.loads(msg.value().decode('utf-8'))
            
            # Match against all connected users
            await match_and_push_to_users(opportunity)
            
        except Exception as e:
            logger.error("Kafka consumer error", error=str(e), exc_info=True)
            await asyncio.sleep(5)
async def match_and_push_to_users(opportunity: Dict):
    """Calculate match score for each connected user and push if relevant"""
    from app.services.matching_service import calculate_match_score
    
    for user_id, profile in manager.user_profiles.items():
        try:
            match_score = calculate_match_score(opportunity, profile)
            
            if match_score >= 60:  # Threshold for relevance
                await manager.send_to_user(user_id, {
                    "type": "new_opportunity",
                    "opportunity": {
                        **opportunity,
                        "match_score": match_score,
                        "match_tier": get_match_tier(match_score)
                    },
                    "timestamp": datetime.utcnow().isoformat()
                })
                
                logger.info(
                    "Opportunity pushed to user",
                    user_id=user_id,
                    opportunity=opportunity['name'],
                    match_score=match_score
                )
        except Exception as e:
            logger.error(
                "Match calculation failed",
                user_id=user_id,
                error=str(e)
            )
# WebSocket endpoint
@router.websocket("/ws/opportunities")
async def websocket_endpoint(websocket: WebSocket, token: str):
    """WebSocket endpoint for real-time opportunity updates"""
    # Verify Firebase token
    try:
        decoded_token = auth.verify_id_token(token)
        user_id = decoded_token['uid']
    except Exception as e:
        await websocket.close(code=1008, reason="Invalid token")
        return
    
    # Load user profile
    profile = await get_user_profile(user_id)
    if not profile:
        await websocket.close(code=1008, reason="Profile not found")
        return
    
    # Connect user
    await manager.connect(user_id, websocket, profile)
    
    # Send confirmation
    await websocket.send_json({
        "type": "connection_established",
        "user_id": user_id,
        "timestamp": datetime.utcnow().isoformat()
    })
    
    # Heartbeat loop
    try:
        while True:
            # Send heartbeat every 30 seconds
            await asyncio.sleep(30)
            await websocket.send_json({
                "type": "heartbeat",
                "timestamp": datetime.utcnow().isoformat()
            })
    except WebSocketDisconnect:
        manager.disconnect(user_id)
    except Exception as e:
        logger.error("WebSocket error", user_id=user_id, error=str(e))
        manager.disconnect(user_id)
3.2 Create Frontend Real-Time Hook
Files to Create:

src/hooks/useRealtimeOpportunities.ts
Implementation:

// src/hooks/useRealtimeOpportunities.ts
import { useState, useEffect, useCallback, useRef } from 'react';
import { useAuth } from '@/contexts/AuthContext';
import { toast } from '@/hooks/use-toast';
interface Opportunity {
  id: string;
  name: string;
  organization: string;
  amount: number;
  deadline: string;
  match_score: number;
  match_tier: string;
  priority_level: string;
  // ... other fields
}
interface WebSocketMessage {
  type: 'connection_established' | 'new_opportunity' | 'heartbeat';
  opportunity?: Opportunity;
  timestamp: string;
}
export const useRealtimeOpportunities = () => {
  const { user, getIdToken } = useAuth();
  const [connected, setConnected] = useState(false);
  const [opportunities, setOpportunities] = useState<Opportunity[]>([]);
  const [newOpportunitiesCount, setNewOpportunitiesCount] = useState(0);
  const wsRef = useRef<WebSocket | null>(null);
  const reconnectTimeoutRef = useRef<NodeJS.Timeout>();
  const reconnectAttempts = useRef(0);
  const connect = useCallback(async () => {
    if (!user) return;
    try {
      const token = await getIdToken();
      const wsUrl = `${import.meta.env.VITE_WS_URL}/ws/opportunities?token=${token}`;
      
      const ws = new WebSocket(wsUrl);
      wsRef.current = ws;
      ws.onopen = () => {
        console.log('[WebSocket] Connected');
        setConnected(true);
        reconnectAttempts.current = 0;
      };
      ws.onmessage = (event) => {
        const message: WebSocketMessage = JSON.parse(event.data);
        
        switch (message.type) {
          case 'connection_established':
            console.log('[WebSocket] Connection confirmed');
            break;
          
          case 'new_opportunity':
            if (message.opportunity) {
              handleNewOpportunity(message.opportunity);
            }
            break;
          
          case 'heartbeat':
            // Connection is alive
            break;
        }
      };
      ws.onerror = (error) => {
        console.error('[WebSocket] Error:', error);
      };
      ws.onclose = () => {
        console.log('[WebSocket] Disconnected');
        setConnected(false);
        
        // Exponential backoff reconnection
        const delay = Math.min(1000 * Math.pow(2, reconnectAttempts.current), 30000);
        reconnectAttempts.current++;
        
        reconnectTimeoutRef.current = setTimeout(() => {
          console.log(`[WebSocket] Reconnecting (attempt ${reconnectAttempts.current})...`);
          connect();
        }, delay);
      };
    } catch (error) {
      console.error('[WebSocket] Connection failed:', error);
    }
  }, [user, getIdToken]);
  const handleNewOpportunity = useCallback((opportunity: Opportunity) => {
    // Add to opportunities list
    setOpportunities(prev => [opportunity, ...prev]);
    setNewOpportunitiesCount(prev => prev + 1);
    // Show toast notification for urgent/high-match opportunities
    if (opportunity.priority_level === 'URGENT' || opportunity.match_score >= 85) {
      toast({
        title: '🎯 New High-Priority Match!',
        description: `${opportunity.name} - ${opportunity.match_score}% match`,
        duration: 5000,
      });
    }
  }, []);
  const clearNewCount = useCallback(() => {
    setNewOpportunitiesCount(0);
  }, []);
  useEffect(() => {
    connect();
    return () => {
      if (wsRef.current) {
        wsRef.current.close();
      }
      if (reconnectTimeoutRef.current) {
        clearTimeout(reconnectTimeoutRef.current);
      }
    };
  }, [connect]);
  return {
    connected,
    opportunities,
    newOpportunitiesCount,
    clearNewCount,
  };
};
3.3 Integrate into Dashboard
Files to Modify:

src/pages/Dashboard.tsx
Implementation:

// src/pages/Dashboard.tsx
import { useRealtimeOpportunities } from '@/hooks/useRealtimeOpportunities';
import { Badge } from '@/components/ui/badge';
import { Wifi, WifiOff } from 'lucide-react';
export default function Dashboard() {
  const { connected, opportunities, newOpportunitiesCount, clearNewCount } = 
    useRealtimeOpportunities();
  
  // Existing opportunities from API
  const { data: cachedOpportunities } = useOpportunities(user.uid);
  
  // Merge real-time and cached opportunities
  const allOpportunities = useMemo(() => {
    const combined = [...opportunities, ...(cachedOpportunities || [])];
    // Deduplicate by ID
    const unique = Array.from(new Map(combined.map(o => [o.id, o])).values());
    // Sort by match score
    return unique.sort((a, b) => b.match_score - a.match_score);
  }, [opportunities, cachedOpportunities]);
  return (
    <div className="dashboard">
      {/* Connection Status */}
      <div className="flex items-center gap-2 mb-4">
        {connected ? (
          <>
            <Wifi className="h-4 w-4 text-green-500" />
            <span className="text-sm text-green-600">Live updates active</span>
          </>
        ) : (
          <>
            <WifiOff className="h-4 w-4 text-gray-400" />
            <span className="text-sm text-gray-500">Connecting...</span>
          </>
        )}
        
        {newOpportunitiesCount > 0 && (
          <Badge variant="destructive" className="ml-auto">
            {newOpportunitiesCount} new
          </Badge>
        )}
      </div>
      {/* Opportunity Feed */}
      <div className="space-y-4">
        {allOpportunities.map((opportunity, index) => (
          <OpportunityCard
            key={opportunity.id}
            opportunity={opportunity}
            isNew={index < newOpportunitiesCount}
            onView={() => clearNewCount()}
          />
        ))}
      </div>
    </div>
  );
}
Phase 4: Chrome Extension Backend Integration (Week 2)
Goal: Complete AI-powered auto-fill with backend field mapping

4.1 Implement Extension API Endpoints
Files to Create/Modify:

backend/app/routes/extension.py
Implementation:

# backend/app/routes/extension.py
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Optional
import structlog
from app.services.ai_service import ai_service
from app.database import get_user_profile
router = APIRouter(prefix="/api/extension", tags=["extension"])
logger = structlog.get_logger()
class FieldMappingRequest(BaseModel):
    user_id: str
    form_fields: List[Dict[str, str]]  # [{"label": "Full Name", "id": "name_field", "type": "text"}]
    page_url: str
    page_title: str
class FieldMappingResponse(BaseModel):
    mappings: Dict[str, str]  # {"name_field": "Adeiza Muhammad"}
    confidence: float
    suggestions: List[str]
@router.get("/user-profile")
async def get_extension_profile(user_id: str):
    """Fetch comprehensive user profile for extension auto-fill"""
    try:
        profile = await get_user_profile(user_id)
        
        if not profile:
            raise HTTPException(status_code=404, detail="Profile not found")
        
        # Return flattened profile optimized for form filling
        return {
            "personal": {
                "full_name": profile.get("name"),
                "first_name": profile.get("name", "").split()[0] if profile.get("name") else "",
                "last_name": profile.get("name", "").split()[-1] if profile.get("name") else "",
                "email": profile.get("email"),
                "phone": profile.get("phone"),
                "date_of_birth": profile.get("date_of_birth"),
            },
            "academic": {
                "school": profile.get("school"),
                "university": profile.get("school"),
                "major": profile.get("major"),
                "gpa": profile.get("gpa"),
                "graduation_year": profile.get("graduation_year"),
                "academic_level": profile.get("academic_status"),
            },
            "location": {
                "address": profile.get("address"),
                "city": profile.get("city"),
                "state": profile.get("state"),
                "zip_code": profile.get("zip_code"),
                "country": profile.get("country"),
            },
            "background": {
                "ethnicity": profile.get("ethnicity"),
                "citizenship": profile.get("citizenship"),
                "first_generation": profile.get("first_generation"),
            },
            "interests": profile.get("interests", []),
            "skills": profile.get("skills", []),
            "achievements": profile.get("achievements", []),
        }
    except Exception as e:
        logger.error("Profile fetch failed", user_id=user_id, error=str(e))
        raise HTTPException(status_code=500, detail=str(e))
@router.post("/map-fields", response_model=FieldMappingResponse)
async def map_form_fields(request: FieldMappingRequest):
    """Use AI to intelligently map form fields to user profile data"""
    try:
        # Get user profile
        profile = await get_user_profile(request.user_id)
        
        # Build AI prompt for field mapping
        prompt = f"""You are an intelligent form-filling assistant. Map the following form fields to the user's profile data.
USER PROFILE:
{json.dumps(profile, indent=2)}
FORM FIELDS TO MAP:
{json.dumps(request.form_fields, indent=2)}
PAGE CONTEXT:
- URL: {request.page_url}
- Title: {request.page_title}
TASK:
For each form field, determine the best matching value from the user's profile.
Return a JSON object mapping field IDs to values.
RULES:
1. Match field labels/IDs to profile keys intelligently (e.g., "fname" → first name)
2. Handle variations: "university" = "school", "college" = "school"
3. For essay/textarea fields, return empty string (user will write custom)
4. For unknown fields, return null
5. Be confident but conservative - only map if you're 80%+ sure
RETURN FORMAT:
{{
  "mappings": {{
    "field_id_1": "value from profile",
    "field_id_2": "another value"
  }},
  "confidence": 0.85,
  "suggestions": ["Consider reviewing the 'major' field", "Essay field detected - needs custom input"]
}}
"""
        
        # Call Gemini for intelligent mapping
        response = await ai_service.generate_content(prompt)
        
        # Parse AI response
        mapping_data = json.loads(response)
        
        logger.info(
            "Field mapping completed",
            user_id=request.user_id,
            fields_mapped=len(mapping_data.get("mappings", {})),
            confidence=mapping_data.get("confidence")
        )
        
        return FieldMappingResponse(**mapping_data)
        
    except Exception as e:
        logger.error("Field mapping failed", error=str(e), exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
@router.post("/save-application-data")
async def save_application_data(user_id: str, application_data: Dict):
    """Save filled application data for tracking"""
    try:
        # Store in Firestore for application tracking
        from app.database import db
        
        application_ref = db.collection("applications").document()
        await application_ref.set({
            "user_id": user_id,
            "url": application_data.get("url"),
            "title": application_data.get("title"),
            "status": "in_progress",
            "filled_at": datetime.utcnow(),
            "data": application_data
        })
        
        return {"success": True, "application_id": application_ref.id}
    except Exception as e:
        logger.error("Save application failed", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))
4.2 Update Extension Content Script
Files to Modify:

extension/content-enhanced.js
Key Enhancement:

// extension/content-enhanced.js
async function autoFillForm() {
  showLoading("Analyzing form fields...");
  
  // Detect all form fields
  const formFields = detectFormFields();
  
  // Call backend for AI field mapping
  const response = await fetch(`${BACKEND_URL}/api/extension/map-fields`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${await getFirebaseToken()}`
    },
    body: JSON.stringify({
      user_id: currentUser.uid,
      form_fields: formFields,
      page_url: window.location.href,
      page_title: document.title
    })
  });
  
  const { mappings, confidence, suggestions } = await response.json();
  
  // Fill fields with mapped values
  for (const [fieldId, value] of Object.entries(mappings)) {
    if (value) {
      fillField(fieldId, value);
    }
  }
  
  showSuccess(`${Object.keys(mappings).length} fields filled! (${Math.round(confidence * 100)}% confidence)`);
  
  // Show suggestions
  if (suggestions.length > 0) {
    showSuggestions(suggestions);
  }
}
Phase 5: Vector Search for RAG Chat (Week 3)
Goal: Enable semantic search for context-aware AI chat

5.1 Store Embeddings in Firestore
Files to Modify:

backend/cloud_functions/stream_processor/main.py
Enhancement:

# In Cloud Function after generating embeddings
# Store in Firestore with vector
opportunity_data = {
    **structured_data,
    "embedding": embedding_vector,  # 768-dimensional array
    "embedding_model": "text-embedding-004",
    "enriched_at": datetime.utcnow()
}
# Firestore supports arrays up to 1500 elements
db.collection("scholarships").document(opportunity_id).set(opportunity_data)
5.2 Implement Vector Similarity Search
Files to Create:

backend/app/services/vector_search.py
Implementation:

# backend/app/services/vector_search.py
import numpy as np
from typing import List, Dict
from app.database import db
from app.services.ai_service import ai_service
async def semantic_search(
    query: str,
    user_profile: Dict,
    top_k: int = 5,
    filters: Dict = None
) -> List[Dict]:
    """
    Perform semantic search using vector similarity
    
    Args:
        query: Natural language query from user
        user_profile: User's profile for filtering
        top_k: Number of results to return
        filters: Additional Firestore filters (e.g., deadline > today)
    
    Returns:
        List of opportunities ranked by semantic similarity
    """
    # Generate embedding for query
    query_embedding = await ai_service.generate_embedding(query)
    
    # Fetch all opportunities (with filters if provided)
    query_ref = db.collection("scholarships")
    
    if filters:
        for field, condition in filters.items():
            query_ref = query_ref.where(field, condition['op'], condition['value'])
    
    opportunities = await query_ref.get()
    
    # Calculate cosine similarity for each
    results = []
    for opp in opportunities:
        opp_data = opp.to_dict()
        opp_embedding = opp_data.get("embedding")
        
        if not opp_embedding:
            continue
        
        similarity = cosine_similarity(query_embedding, opp_embedding)
        
        results.append({
            **opp_data,
            "similarity_score": similarity
        })
    
    # Sort by similarity and return top K
    results.sort(key=lambda x: x["similarity_score"], reverse=True)
    return results[:top_k]
def cosine_similarity(vec1: List[float], vec2: List[float]) -> float:
    """Calculate cosine similarity between two vectors"""
    vec1 = np.array(vec1)
    vec2 = np.array(vec2)
    
    dot_product = np.dot(vec1, vec2)
    norm1 = np.linalg.norm(vec1)
    norm2 = np.linalg.norm(vec2)
    
    return float(dot_product / (norm1 * norm2))
5.3 Enhance Chat Endpoint with RAG
Files to Modify:

backend/app/routes/chat.py
Implementation:

# backend/app/routes/chat.py
from app.services.vector_search import semantic_search
@router.post("/api/chat/message")
async def chat_message(request: ChatRequest):
    """Enhanced chat with RAG (Retrieval-Augmented Generation)"""
    
    # Detect if query is about finding opportunities
    if is_opportunity_query(request.message):
        # Perform semantic search
        relevant_opps = await semantic_search(
            query=request.message,
            user_profile=request.user_profile,
            top_k=5,
            filters={"deadline": {"op": ">=", "value": datetime.utcnow()}}
        )
        
        # Build context-rich prompt for Gemini
        context = f"""
USER QUERY: {request.message}
USER PROFILE:
- Major: {request.user_profile.get('major')}
- Interests: {', '.join(request.user_profile.get('interests', []))}
- Financial Need: ${request.user_profile.get('financial_need')}
RELEVANT OPPORTUNITIES (from semantic search):
{json.dumps(relevant_opps, indent=2)}
TASK:
Respond conversationally to the user's query. Reference specific opportunities by name.
Explain WHY each opportunity is a good match based on their profile.
Provide actionable next steps (e.g., "Apply to X by Friday").
"""
        
        response = await ai_service.generate_content(context)
        
        return {
            "response": response,
            "opportunities": relevant_opps,
            "search_type": "semantic"
        }
    else:
        # Regular chat response
        return await regular_chat_response(request)
🎯 Verification Plan
Automated Tests
1. Kafka Integration Tests
# backend/tests/test_kafka_integration.py
import pytest
from app.services.kafka_config import get_kafka_producer
from confluent_kafka import Consumer
@pytest.mark.asyncio
async def test_producer_publishes_to_kafka():
    """Test that scraper can publish to Kafka"""
    producer = get_kafka_producer()
    
    test_message = {
        "source": "test",
        "raw_data": {"name": "Test Scholarship"},
        "scraped_at": "2025-12-05T00:00:00Z"
    }
    
    producer.produce(
        topic="raw-opportunities-stream",
        value=json.dumps(test_message).encode('utf-8')
    )
    producer.flush()
    
    # Verify message was received
    consumer = Consumer({
        'bootstrap.servers': settings.confluent_bootstrap_servers,
        'group.id': 'test-consumer',
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe(['raw-opportunities-stream'])
    
    msg = consumer.poll(timeout=10.0)
    assert msg is not None
    assert json.loads(msg.value())['source'] == 'test'
2. WebSocket Connection Tests
# backend/tests/test_websocket.py
from fastapi.testclient import TestClient
from app.main import app
def test_websocket_connection():
    """Test WebSocket connection and message delivery"""
    client = TestClient(app)
    
    with client.websocket_connect("/ws/opportunities?token=test_token") as websocket:
        # Receive connection confirmation
        data = websocket.receive_json()
        assert data["type"] == "connection_established"
        
        # Simulate new opportunity from Kafka
        # (This would be triggered by Kafka consumer in real scenario)
        
        # Receive new opportunity message
        data = websocket.receive_json()
        assert data["type"] == "new_opportunity"
        assert "opportunity" in data
3. Extension API Tests
# backend/tests/test_extension_api.py
@pytest.mark.asyncio
async def test_field_mapping():
    """Test AI field mapping for extension"""
    request = {
        "user_id": "test_user",
        "form_fields": [
            {"label": "Full Name", "id": "name", "type": "text"},
            {"label": "Email Address", "id": "email", "type": "email"}
        ],
        "page_url": "https://example.com/apply",
        "page_title": "Scholarship Application"
    }
    
    response = await client.post("/api/extension/map-fields", json=request)
    
    assert response.status_code == 200
    data = response.json()
    assert "mappings" in data
    assert "name" in data["mappings"]
    assert data["confidence"] > 0.7
Manual Verification
1. End-to-End Real-Time Flow
 Deploy scraper to Cloud Run
 Verify messages appear in Confluent Cloud UI
 Check Cloud Function logs for enrichment
 Confirm enriched messages in second topic
 Open dashboard, verify WebSocket connection
 Trigger scraper manually
 See new opportunity appear in dashboard within 5 seconds
 Verify toast notification for high-match opportunities
2. Chrome Extension Flow
 Load extension in Chrome
 Navigate to Devpost hackathon application
 Click ScholarStream Copilot button
 Click "Scan Page & Autofill"
 Verify 15+ fields filled correctly
 Check application tracking in dashboard
3. RAG Chat Test
 Open chat assistant
 Type: "I need money urgently for textbooks by Friday"
 Verify AI returns specific opportunities with deadlines < 7 days
 Check that response references user's major and interests
 Confirm opportunities are semantically relevant (not just keyword match)
🚀 Deployment Strategy
Week 1: Infrastructure Setup
Day 1-2: Confluent Cloud cluster + topics
Day 3-4: Deploy Cloud Function for enrichment
Day 5: Deploy scraper services to Cloud Run
Day 6-7: Testing and monitoring
Week 2: Real-Time Features
Day 1-2: Complete WebSocket backend integration
Day 3-4: Build frontend real-time hook and dashboard
Day 5: Extension backend API endpoints
Day 6-7: Testing and bug fixes
Week 3: Polish & Submission
Day 1-2: Vector search and RAG chat
Day 3-4: Performance optimization and monitoring
Day 5: Demo video recording (3 minutes)
Day 6: Documentation and README updates
Day 7: Hackathon submission
📊 Success Metrics
Metric	Target	How to Measure
Discovery Latency	< 5 seconds	Time from scrape → dashboard display
WebSocket Uptime	> 99%	Connection manager logs
Match Accuracy	> 85%	User feedback on relevance
Auto-Fill Accuracy	> 80%	Fields correctly mapped
Throughput	100+ opps/minute	Kafka consumer lag
User Engagement	5+ min session	Analytics
⚠️ Risk Mitigation
Risk 1: Confluent Free Tier Limits
Mitigation:

Monitor usage in Confluent Cloud dashboard
Implement message batching
Set up alerts at 80% quota
Risk 2: Cloud Function Cold Starts
Mitigation:

Set min instances = 1 for critical functions
Implement caching for Vertex AI calls
Use Cloud Run for long-running processes
Risk 3: WebSocket Connection Drops
Mitigation:

Implement exponential backoff reconnection
Heartbeat every 30 seconds
Fallback to polling if WebSocket fails
Risk 4: Extension Permissions
Mitigation:

Request minimal permissions in manifest
Explain each permission in README
Provide privacy policy
🎬 Demo Video Script (3 Minutes)
[0:00-0:30] The Problem

Show student missing scholarship deadline
$2.9B unclaimed annually
Information scattered across 1000+ sites
[0:30-1:00] The Solution: Real-Time Architecture

Diagram: Confluent + Google Cloud AI
Live scraping → Kafka → Gemini enrichment → WebSocket push
"Financial news terminal for students"
[1:00-1:45] Live Demo

Dashboard with WebSocket connected
Trigger scraper (show Confluent UI)
New opportunity appears in dashboard (< 5 seconds)
Toast notification for urgent match
Click opportunity → detailed view
[1:45-2:30] Chrome Extension Magic

Navigate to Devpost application
Click ScholarStream Copilot
"Scan Page & Autofill"
20 fields filled in 2 seconds
Show AI field mapping accuracy
[2:30-3:00] Impact & Call to Action

45 million students need this
Real-time = no missed deadlines
Open source, ready to deploy
"Help us democratize opportunity access"
📚 Documentation Updates Needed
Files to Update:
README.md - Add Confluent setup instructions
DEPLOYMENT.md - Step-by-step Cloud Function deployment
ARCHITECTURE.md - Event-driven architecture diagram
EXTENSION_GUIDE.md - Chrome extension installation and usage
.env.example - Add all Confluent/GCP environment variables
🏆 Hackathon Submission Checklist
 Hosted Project URL: scholarstream.vercel.app
 GitHub Repository: Public with open source license
 Demo Video: 3 minutes on YouTube
 Challenge Selection: Confluent + Google Cloud AI
 README: Clear setup instructions
 Working Features:
 Real-time streaming with Confluent
 AI enrichment with Vertex AI
 WebSocket live updates
 Chrome extension auto-fill
 Vector search RAG chat
 Code Quality:
 Structured logging
 Error handling
 Type hints (Python)
 TypeScript types (Frontend)
 Documentation:
 Architecture diagrams
 API documentation
 Deployment guide
🙏 Final Notes
Bismillah. Allahu Musta'an.

This implementation plan transforms ScholarStream from a promising prototype into a production-ready, event-driven platform that truly deserves the "FAANG-level excellence" designation.

The architecture leverages:

Confluent Cloud for real-time data streaming
Google Cloud AI (Vertex AI Gemini + Embeddings) for intelligent processing
WebSockets for instant user notifications
Vector search for semantic matching
Chrome Extension for seamless application experience
Key Differentiators:

True Real-Time: Not polling, actual event-driven streaming
AI-First: Gemini everywhere - enrichment, matching, auto-fill
Production-Ready: Error handling, monitoring, scalability
User-Centric: Solves real problem for 45M students
Next Step: Review this plan, approve, and I'll begin implementation phase-by-phase.

La Howla Wallaquwatah Illah Billah. 🚀