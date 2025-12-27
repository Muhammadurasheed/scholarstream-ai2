# ScholarStream Platform: Comprehensive Analysis & Surgical Fix Request

## 🎯 CRITICAL DIRECTIVE TO LOVABLE

**You are being summoned as a team of world-class CTOs and Principal Engineers from FAANG companies with decades of experience in complex distributed systems, event-driven architectures, and full-stack development. Your mission is to perform a SURGICAL, end-to-end analysis of the ScholarStream platform and architect comprehensive fixes for critical URL redirection failures and missing platform integrations.**

> **⚠️ ABSOLUTE REQUIREMENT**: You MUST work with our existing infrastructure:
> - **Backend**: FastAPI (Python) running on `localhost:8081`
> - **Database**: Firebase Firestore (already configured)
> - **Message Queue**: Confluent Kafka (critical lifeline - DO NOT REMOVE)
> - **Frontend**: React + TypeScript + Vite
> 
> **DO NOT** suggest Lovable Cloud, Supabase, or any alternative infrastructure. Work within our battle-tested architecture.

---

## 📊 PLATFORM OVERVIEW: ScholarStream Architecture

### What ScholarStream Is
ScholarStream is an **AI-powered opportunity discovery platform** that aggregates scholarships, hackathons, bounties, and grants from 15+ platforms, providing students and developers with personalized, real-time opportunity matching. Think of it as "Google for Opportunities" with intelligent personalization.

### Core Value Proposition
- **Unified Discovery**: Single dashboard for DevPost hackathons, Superteam bounties, Intigriti bug bounties, MLH events, DoraHacks, TAIKAI, HackQuest, Kaggle competitions, and traditional scholarships
- **AI Personalization**: Gemini 2.0 Flash-powered matching engine that scores opportunities based on user interests, major, GPA, location, and career goals
- **Real-Time Updates**: WebSocket-based live streaming of new opportunities as they're discovered
- **Smart Application Tracking**: Draft management, deadline reminders, and application progress tracking

---

## 🏗️ SYSTEM ARCHITECTURE: How It's Engineered

### Technology Stack

#### Frontend (`src/`)
- **Framework**: React 18 + TypeScript
- **Build Tool**: Vite
- **Routing**: React Router v6
- **State Management**: React Context + Local Storage
- **UI Components**: Custom components with Tailwind CSS
- **Real-Time**: WebSocket client for opportunity streaming
- **Authentication**: Firebase Auth (email/password + Google OAuth)

#### Backend (`backend/app/`)
- **Framework**: FastAPI (Python 3.11+)
- **ASGI Server**: Uvicorn
- **Database**: Firebase Firestore (NoSQL document store)
- **Caching**: Upstash Redis (rate limiting + deduplication)
- **AI Engine**: Google Gemini 2.0 Flash (via Vertex AI)
- **Message Queue**: **Confluent Kafka** (Apache Kafka managed service)
- **Logging**: Structlog (structured JSON logging)
- **HTTP Client**: httpx (async HTTP for scraping)

#### Infrastructure
- **Deployment**: Local development (Windows)
- **Ports**: Backend on `8081`, Frontend on `8080`
- **Environment**: `.env` file for secrets (Firebase, Kafka, Gemini API keys)

---

## 🔄 DATA FLOW: How the System Works

### 1. **Opportunity Discovery Pipeline** (The "Cortex" System)

```
┌─────────────────────────────────────────────────────────────────┐
│                    CORTEX DISCOVERY ENGINE                       │
│  (Autonomous AI-Powered Web Scraping & Enrichment System)       │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
        ┌─────────────────────────────────────────┐
        │   1. SENTINEL PATROLS (Playwright)      │
        │   - Monitors 50+ target URLs            │
        │   - Detects new opportunities via LLM   │
        │   - Publishes to Kafka: cortex.raw.html │
        └─────────────────────────────────────────┘
                              │
                              ▼
        ┌─────────────────────────────────────────┐
        │   2. ENRICHMENT WORKER (Kafka Consumer) │
        │   - Consumes cortex.raw.html.v1         │
        │   - Extracts structured data via Gemini │
        │   - Publishes to: opportunity.enriched  │
        └─────────────────────────────────────────┘
                              │
                              ▼
        ┌─────────────────────────────────────────┐
        │   3. FLINK PROCESSOR (Kafka Consumer)   │
        │   - Consumes opportunity.enriched.v1    │
        │   - Deduplicates via Redis              │
        │   - Saves to Firestore                  │
        │   - Broadcasts via WebSocket            │
        └─────────────────────────────────────────┘
```

### 2. **Dedicated API Scrapers** (Parallel to Cortex)

In addition to the Cortex system, we have **dedicated API scrapers** for platforms with public APIs:

- **DevPost API Scraper** (`devpost_api_scraper.py`): Fetches hackathons from `https://devpost.com/api/v1/hackathons`
- **MLH Scraper** (`mlh_scraper.py`): Hybrid approach (API + fallback)
- **TAIKAI Scraper** (`taikai_scraper.py`): Extracts from Next.js `__NEXT_DATA__` blob
- **HackQuest Scraper** (`hackquest_scraper.py`): GraphQL API at `api.hackquest.io/graphql`
- **DoraHacks Scraper** (`multi_platform_scraper.py`): REST API at `dorahacks.io/api/hackathon/`
- **Intigriti Scraper** (`intigriti_scraper.py`): Public API at `app.intigriti.com/api/core/public/programs`
- **Superteam Scraper** (`multi_platform_scraper.py`): API at `earn.superteam.fun/api/listings`

These scrapers run on **backend startup** and periodically via scheduled tasks.

### 3. **Personalization Engine**

```
User Profile (interests, major, GPA, location)
              │
              ▼
┌─────────────────────────────────────┐
│  PersonalizationEngine              │
│  - Interest matching (synonyms)     │
│  - Academic fit scoring             │
│  - Geographic relevance             │
│  - Deadline urgency weighting       │
└─────────────────────────────────────┘
              │
              ▼
    Match Score (0-100%) + Tier
```

### 4. **Frontend Matching Engine** (Client-Side Fallback)

The frontend has a **mirror** of the personalization logic in `src/services/matchingEngine.ts` that:
- Recalculates scores if backend score is missing or stale
- Uses synonym expansion (e.g., "AI" → ["machine learning", "deep learning", "GPT"])
- Applies the same scoring weights as the backend

---

## ⚡ CONFLUENT KAFKA: The Lifeline

**Kafka is NOT optional. It is the nervous system of ScholarStream.**

### Why Kafka is Critical

1. **Decoupling**: Scrapers, enrichment workers, and processors run independently. If one crashes, others continue.
2. **Scalability**: Can handle 10,000+ opportunities/hour without blocking
3. **Reliability**: Messages are persisted. If a consumer crashes, it resumes from the last offset.
4. **Real-Time**: Sub-second latency from discovery to user dashboard
5. **Audit Trail**: Every opportunity's journey is logged in Kafka topics

### Kafka Topics in Use

| Topic | Purpose | Producer | Consumer |
|-------|---------|----------|----------|
| `cortex.raw.html.v1` | Raw HTML from Sentinel patrols | Crawler Service | Enrichment Worker |
| `opportunity.enriched.v1` | Structured opportunity data | Enrichment Worker | Flink Processor |
| `user.matches.v1` | Personalized match scores | Matching Service | WebSocket Service |
| `cortex.commands.v1` | Control commands (pause/resume) | Admin API | Crawler Service |
| `system.alerts.v1` | Error alerts & monitoring | All services | Monitoring Dashboard |

### Confluent Configuration

```python
KAFKA_CONFIG = {
    'bootstrap.servers': 'pkc-619z3.us-east1.gcp.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv('CONFLUENT_API_KEY'),
    'sasl.password': os.getenv('CONFLUENT_API_SECRET'),
}
```

**DO NOT remove or replace Kafka.** It is the foundation of our event-driven architecture.

## 🛠 THE ARCHITECTURAL BLUEPRINT
ScholarStream is NOT a simple CRUD app. It is a high-performance, event-driven ecosystem:
1. **The Ingestion Layer (Python/Playwright)**: Dedicated deep scrapers (DoraHacks, DevPost, MLH, HackQuest) bypass anti-bot measures to extract raw data.
2. **The Backbone (Confluent Kafka & Flink)**: A massive event pipeline where `cortex.raw.html.v1` events are streamed, deduplicated via Apache Flink, and prepared for refinery.
3. **The Refinery (Gemini 1.5 Flash)**: `ReaderLLM` converts raw text into structured JSON. `EnrichmentWorker` maps this into the `Scholarship` domain model.
4. **The Intelligence (Personalization Engine)**: A semantic scoring engine that calculates a 0-100% "Match Score" based on the user's "Digital DNA" (interests, major, background).
5. **The Persistence (Firestore)**: High-availability NoSQL storage.
6. **The Frontend (Vite + React + Tailwind + TanStack Query)**: A premium, virtualized dashboard (`react-window`) designed to WOW users.

---

## 💔 THE HEARTBROKEN FAILURES
Despite our engineering, we are facing 3 critical "FAANG-level" blockers that require your surgical intervention:

### 1. Data Inaccuracy (DoraHacks & MLH vs DevPost)
*   **The Problem**: While DevPost opportunities (Image 3) look perfect with real prize amounts and deadlines, **DoraHacks** (Image 1) and **MLH** (Image 2) are "bleeding" data.
*   **Discrepancy**: Most show **"See Details"** or **"$0"** for prizes and **"no deadline"** or **"See listing"** for dates, even when the data exists on the source.
*   **The Leak**: The refinery pipeline is likely failing to extract absolute numerical values or is being overwritten by "Sentinel" (fast scraper) fallbacks.

### 2. The "47% Ghost" (Matching Score Flaw)
*   **The Problem**: As seen in the images, almost all DoraHacks and MLH opportunities are stuck at a hardcoded **47% Match Score**. 
*   **Requirement**: The match score MUST be dynamic, pragmatically calculated, and super-intelligent. It should reflect the student's software background accurately.

### 3. The Fractured AI Chat Assistant
*   **The Query**: *"I'm at University of Ibadan, my school fee deadline is tomorrow, help me find matching and timely opportunities."*
*   **The Failure**: 
    - **UX**: The `Thinking Process` is outputted as raw text in the response (Image 4) instead of an instant, real-time log during loading (like Antigravity).
    - **Relevance**: It returned **0 matches** for a valid developer query, failing to recognize that a software dev is a match for hackathons/bounties, and that even a student from University of Ibadan is a match for global hackathons and opportunities like devpost, dorahack, bounties, etc
    - **Hallucination**: It suggested an "Internship at Google" with a **May 2024** deadline (hallucination/stale data).
    - **Formatting**: The response looks like an unformatted JSON dump instead of a premium, encouraging advisor.

---

## 🛰 YOUR MISSION: SURGICAL AUDIT & RE-ENGINEERING
Act as the **Principal Engineer**. Meticulously analyze the entire codebase from `backend/` to `src/`. Do not just "patch" code; **re-imagine the flow** where necessary.

### 🔧 DIRECTIVES:
1. **Audit the Data Pipeline**: Fix the `ReaderLLM` and enrichment logic to ensure absolute prize pools and deadlines are captured. Eradicate "Varies" and "$0" defaults.
2. **Re-Engineer matching_service.py**: Connect the real `PersonalizationEngine` to the dashboard/chat and ensure scores are never hardcoded.
3. **Refine AI Chat Assistant**: 
    - Decouple the `Thinking Process` logic into a real-time UI component.
    - Fix the location detection (Ibadan/Nigeria).
    - Broaden search logic (fuzzy matching) so students never get "0 matches" for crisis queries.
4. **Frontend Optimization**: Ensure `OpportunityCard.tsx` renders the new data points perfectly.

### ⚠️ CRITICAL CONSTRAINTS:
- **NO Lovable Cloud**: You MUST work with our existing infrastructure (Backend, Firestore, Kafka).
- **Premium Aesthetics**: Every change must feel state-of-the-art and high-end.
- **Deduplication**: Ensure the "Fast" scraper never overwrites "Deep" scraper data.

Analyze the uploaded images carefully to see the visual proof of these failures.

**Allahu Musta'an. Proceed with FAANG-level precision.**
