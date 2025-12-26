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

---

## 💔 CRITICAL ISSUES: The Heartbreak

### Issue #1: DevPost URL Redirection Failures (404 Pages)

**Symptom**: When users click "Apply Now" on **ANY** DevPost hackathon, they are redirected to a 404 page.

**Example**:
- Opportunity: "AI Partner Catalyst Hackathon"
- Stored URL: `https://devpost.com/hackathons/ai-partner-catalyst/`
- **Result**: 404 "Sorry, that page does not exist."

**Screenshot Reference**: See `uploaded_image_0_1766737580740.png` (DevPost 404 page)

**Root Cause Hypothesis**:
1. **URL Normalization Bug**: The frontend (`src/utils/scholarshipUtils.ts`) has a `normalizeApplyUrl()` function that may be incorrectly transforming URLs
2. **Backend Storage Issue**: The scraper (`devpost_api_scraper.py`) may be storing incorrect URL formats
3. **Subdomain vs. Path Confusion**: DevPost uses subdomains for hackathons (e.g., `gemini-3-hackathon.devpost.com`) but also has path-based URLs (`devpost.com/hackathons/gemini-3-hackathon/`). One format may be breaking.

**Current Code Locations**:
- Frontend normalization: `src/utils/scholarshipUtils.ts` (lines 200-232)
- Backend scraper: `backend/app/services/scrapers/hackathons/devpost_api_scraper.py` (lines 73-167)
- Apply button handler: `src/components/dashboard/OpportunityCard.tsx` (line 165)

---

### Issue #2: Superteam Earn URL Redirection Failures (Nothing Found)

**Symptom**: Clicking "Apply Now" on Superteam bounties leads to a "Nothing Found" page with a cat image.

**Example**:
- Opportunity: "Donut AI Fanart Contest"
- Stored URL: `https://earn.superteam.fun/listings/donut-ai-fanart-contest`
- **Result**: "Nothing Found" error page

**Screenshot Reference**: See `uploaded_image_1_1766737580740.png` (Superteam "Nothing Found" page)

**Root Cause Hypothesis**:
1. **Stale Data**: The bounty may have expired, but we're still showing it
2. **Incorrect Slug**: The API may return a different slug format than what the URL expects
3. **URL Construction Bug**: The scraper (`multi_platform_scraper.py`) builds URLs as `https://earn.superteam.fun/listings/{slug}`, but the actual URL might be `/bounties/{slug}` or `/opportunities/{slug}`

**Current Code Locations**:
- Superteam scraper: `backend/app/services/scrapers/bounties/multi_platform_scraper.py` (lines 316-385)
- URL construction: Line 325 (`url = f"https://earn.superteam.fun/listings/{slug}"`)

---

### Issue #3: Intigriti URL Redirection Failures (Forbidden)

**Symptom**: Clicking "Apply Now" on Intigriti bug bounties leads to a "Forbidden" page.

**Example**:
- Opportunity: "Uphold Bug Bounty"
- Stored URL: `https://app.intigriti.com/programs/Uphold/Uphold/detail`
- **Result**: 403 Forbidden "You actually don't have access to this page."

**Screenshot Reference**: See `uploaded_image_2_1766737580740.png` (Intigriti Forbidden page)

**Root Cause Hypothesis**:
1. **Authentication Required**: The URL may require login, but we're sending users to a protected page
2. **Incorrect URL Format**: The scraper may be constructing URLs incorrectly
3. **Public vs. Private Programs**: Some programs may not be publicly accessible

**Current Code Locations**:
- Intigriti scraper: `backend/app/services/scrapers/bounties/intigriti_scraper.py` (lines 140-180)
- URL construction: Line ~160 (`url = f"https://www.intigriti.com/programs/{handle}/{programHandle}"`)

---

### Issue #4: Missing Platforms (No Data Ingestion)

**Symptom**: The following platforms are **NOT** showing any opportunities in the dashboard, despite having scrapers:

1. **DoraHacks**: Scraper exists but returns 0 results
2. **HackQuest**: Scraper gets 403 Forbidden from API
3. **TAIKAI**: Scraper crashes with "Page is navigating" error
4. **Kaggle**: No scraper implemented
5. **MLH**: Scraper exists but may not be running

**Backend Logs Show**:
```
2025-12-25 11:02:07 [warning] HackQuest API failed [app.services.scrapers.hackathons.hackquest_scraper] body=<!doctype html><meta charset="utf-8"><meta name=viewport content="width=device-width, initial-scale=1"><title>403</title>403 Forbidden status=403

2025-12-25 11:04:00 [warning] No opportunities extracted from target [app.services.enrichment_worker] duration=0.99s url=https://dorahacks.io/hackathon

2025-12-25 11:12:36 [error] Drone crash [app.services.crawler_service] error=Page.content: Unable to retrieve content because the page is navigating and changing the content. url=https://taikai.network/hackathons
```

**Root Cause Hypothesis**:
1. **Anti-Bot Protection**: Platforms are blocking our scrapers (403 errors)
2. **Incorrect API Endpoints**: We may be hitting deprecated or wrong endpoints
3. **Missing Headers**: Scrapers may need browser-like headers to bypass Cloudflare
4. **SPA Rendering Issues**: TAIKAI is a Single Page App that requires JavaScript execution

---

## 🎯 YOUR MISSION: Surgical Analysis & Implementation Plan

### Phase 1: Forensic Analysis (What You Must Do)

1. **Trace URL Flow End-to-End**:
   - Start from the scraper that fetches the opportunity
   - Follow the URL through Kafka → Firestore → Frontend
   - Identify every transformation, normalization, or mutation
   - Document the **exact** URL at each stage

2. **Analyze URL Normalization Logic**:
   - Review `src/utils/scholarshipUtils.ts` → `normalizeApplyUrl()`
   - Check if it's breaking valid URLs
   - Determine if it's needed at all

3. **Inspect Scraper URL Construction**:
   - For each platform (DevPost, Superteam, Intigriti), verify:
     - What URL format does the API return?
     - What URL format does the scraper store?
     - What URL format does the platform actually expect?

4. **Debug Missing Platforms**:
   - For DoraHacks, HackQuest, TAIKAI:
     - Test the API endpoints manually (curl/Postman)
     - Check if headers are missing (User-Agent, Referer, etc.)
     - Verify the response structure matches our parser

5. **Review Data Persistence**:
   - Check Firestore schema for `source_url` field
   - Verify no database-level corruption or encoding issues

---

### Phase 2: Comprehensive Implementation Plan (What You Must Deliver)

Create a **detailed, step-by-step implementation plan** that includes:

#### 1. **URL Redirection Fixes**

For each broken platform (DevPost, Superteam, Intigriti):

- **Root Cause**: Exact reason for the 404/403/Nothing Found error
- **Proposed Fix**: Specific code changes with file paths and line numbers
- **Validation**: How to test the fix (manual test cases)
- **Rollback Plan**: If the fix breaks something else

#### 2. **Missing Platform Integration**

For each missing platform (DoraHacks, HackQuest, TAIKAI, Kaggle, MLH):

- **API Research**: Document the correct API endpoint, headers, and response format
- **Scraper Implementation**: Pseudocode or actual code for the scraper
- **Error Handling**: How to handle rate limits, 403s, timeouts
- **Data Transformation**: How to map API response to our `Scholarship` model

#### 3. **Architectural Improvements** (If Needed)

If you identify systemic issues, propose:

- **URL Storage Strategy**: Should we store raw URLs and normalize only on display?
- **Scraper Hardening**: Should we add retry logic, exponential backoff, or proxy rotation?
- **Data Validation**: Should we validate URLs before saving to Firestore?
- **Monitoring**: Should we add alerts for scraper failures?

**Feel free to re-imagine the architecture**, but:
- Keep Kafka (non-negotiable)
- Keep Firebase Firestore (non-negotiable)
- Keep FastAPI backend (non-negotiable)
- Keep React frontend (non-negotiable)

---

## 📋 DELIVERABLES EXPECTED

1. **Forensic Analysis Report**:
   - URL flow diagram (scraper → Kafka → Firestore → frontend)
   - Root cause analysis for each broken platform
   - Code snippets showing the exact bug

2. **Implementation Plan** (Markdown format):
   - Organized by platform
   - Prioritized by impact (DevPost first, then Superteam, then Intigriti)
   - Includes code diffs, file paths, and testing instructions

3. **Code Fixes** (If Possible):
   - Updated scraper files
   - Updated frontend normalization logic
   - Database migration scripts (if schema changes needed)

4. **Testing Strategy**:
   - Manual test cases for each platform
   - Automated tests (if applicable)
   - Rollback procedure

---

## 🚨 CONSTRAINTS & REQUIREMENTS

### What You MUST Do
✅ Work within our existing tech stack (FastAPI, React, Kafka, Firestore)  
✅ Preserve Kafka-based event-driven architecture  
✅ Ensure backward compatibility (don't break existing opportunities)  
✅ Provide detailed, actionable implementation steps  
✅ Include code examples and file paths  

### What You MUST NOT Do
❌ Suggest replacing Kafka with anything else  
❌ Suggest replacing Firestore with Supabase/PostgreSQL  
❌ Suggest using Lovable Cloud or any hosted service  
❌ Provide vague, high-level advice without code  
❌ Ignore the screenshots provided (they are critical evidence)  

---

## 🔍 REFERENCE MATERIALS

### Key Files to Analyze

**Frontend**:
- `src/utils/scholarshipUtils.ts` (URL normalization)
- `src/components/dashboard/OpportunityCard.tsx` (Apply button)
- `src/services/matchingEngine.ts` (Scoring logic)

**Backend Scrapers**:
- `backend/app/services/scrapers/hackathons/devpost_api_scraper.py`
- `backend/app/services/scrapers/hackathons/mlh_scraper.py`
- `backend/app/services/scrapers/hackathons/taikai_scraper.py`
- `backend/app/services/scrapers/hackathons/hackquest_scraper.py`
- `backend/app/services/scrapers/bounties/multi_platform_scraper.py`
- `backend/app/services/scrapers/bounties/intigriti_scraper.py`

**Kafka Integration**:
- `backend/app/services/kafka_service.py`
- `backend/app/services/flink_processor.py`
- `backend/app/services/enrichment_worker.py`

**Database**:
- `backend/app/database.py` (Firestore client)
- `backend/app/models.py` (Scholarship schema)

---

## 💬 FINAL WORDS

We've been battling these URL redirection issues for weeks. Users are frustrated. Our platform's credibility is at stake. We need a **FAANG-level, surgical fix** that addresses the root cause, not just symptoms.

You have full access to the codebase. You have the screenshots showing the exact errors. You have the backend logs showing the failures.

**Step in as the world-class engineering team we need. Analyze. Diagnose. Fix. Document.**

Allahu Musta'an (God is sufficient for us).

---

## 📎 APPENDIX: Screenshot References

When analyzing the issues, refer to these screenshots I will provide:

1. **DevPost 404 Error**: `uploaded_image_0_1766737580740.png`
   - Shows "Sorry, that page does not exist" on DevPost
   - URL in browser: `devpost.com/hackathons/ai-partner-catalyst/`

2. **Superteam "Nothing Found"**: `uploaded_image_1_1766737580740.png`
   - Shows cat image with "Nothing Found" message
   - URL in browser: `earn.superteam.fun/listings/donut-ai-fanart-contest`

3. **Intigriti Forbidden**: `uploaded_image_2_1766737580740.png`
   - Shows "Forbidden - You actually don't have access to this page"
   - URL in browser: `app.intigriti.com/programs/Uphold/Uphold/detail`

These are not hypothetical errors. These are **real, production issues** affecting our users right now.
