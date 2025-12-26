# 🚀 ScholarStream: Mission-Critical Engineering Brief

> **TO:** Antigravity Engineering Team (Assume FAANG Principal+ Authority)
> **FROM:** ScholarStream Founder
> **PRIORITY:** 🔴 P0 - Critical
> **DATE:** December 2025

---

## Bismillah - Allahu Musta'an

This document represents the most comprehensive technical brief for ScholarStream. You are being summoned to operate as a **coalition of CTOs and Principal Engineers from Google, Meta, Netflix, Stripe, and OpenAI** - bringing world-class engineering rigor to transform this platform into a flagship, production-grade product.

---

## 📋 EXECUTIVE SUMMARY

**ScholarStream** is an AI-powered, real-time opportunity discovery and application platform for students. It aggregates scholarships, hackathons, bounties, and competitions from across the internet, enriches them with AI, and delivers hyper-personalized matches to users in real-time.

### The Vision
> *"Never let a student miss a life-changing opportunity because they didn't know it existed."*

### Current Reality
The platform has sophisticated architecture on paper but critical execution gaps that prevent it from delivering on its promise. **This is your mission: Make it work. Make it exceptional.**

---

## 🏗️ SYSTEM ARCHITECTURE DEEP DIVE

### The Event-Driven Pipeline (How It SHOULD Work)

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                    SCHOLARSTREAM EVENT-DRIVEN ARCHITECTURE                        │
└──────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────┐     ┌─────────────────────┐     ┌─────────────────────────────┐
│  HUNTER DRONES  │     │   CONFLUENT KAFKA   │     │       CORTEX BRAIN          │
│  (Playwright)   │     │   (Event Stream)    │     │   (AI Enrichment Layer)     │
├─────────────────┤     ├─────────────────────┤     ├─────────────────────────────┤
│                 │     │                     │     │                             │
│ • DevPost       │     │ cortex.raw.html.v1  │     │ Reader LLM (Gemini)         │
│ • DoraHacks     │────▶│   (Raw HTML)        │────▶│   - Multi-extraction        │
│ • TAIKAI        │     │                     │     │   - Schema normalization    │
│ • HackQuest     │     │ opportunity.        │     │                             │
│ • Gitcoin       │     │   enriched.v1       │◀────│ Refinery Service            │
│ • Immunefi      │     │   (Verified Data)   │     │   - Deduplication           │
│ • Kaggle        │     │                     │     │   - Geo-tagging             │
│ • MLH           │     │ user.identity.v1    │     │   - Type-tagging            │
│                 │     │ user.matches.v1     │     │   - Expiration filtering    │
└─────────────────┘     └──────────┬──────────┘     └─────────────────────────────┘
                                   │
                                   ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│                           FASTAPI BACKEND (Render)                                │
├──────────────────────────────────────────────────────────────────────────────────┤
│                                                                                   │
│  ┌─────────────────────┐   ┌─────────────────────┐   ┌────────────────────────┐  │
│  │  Kafka Consumer     │   │  Personalization    │   │   WebSocket Manager    │  │
│  │  (Background)       │──▶│  Engine             │──▶│   /ws/opportunities    │  │
│  │                     │   │                     │   │                        │  │
│  │  • Subscribes to    │   │  • 4-Factor Scoring │   │  • Active connections  │  │
│  │    enriched stream  │   │  • Interest Match   │   │  • Real-time push      │  │
│  │                     │   │  • Semantic Scoring │   │  • Firebase auth       │  │
│  └─────────────────────┘   └─────────────────────┘   └───────────┬────────────┘  │
│                                                                   │              │
│  ┌─────────────────────┐   ┌─────────────────────┐               │              │
│  │  AI Chat Service    │   │  Copilot Service    │               │              │
│  │  (chat_service.py)  │   │  V2 Platform-Aware  │               │              │
│  │                     │   │                     │               │              │
│  │  • Emergency Mode   │   │  • Tri-Fold KB      │               │              │
│  │  • Transparent UX   │   │  • Platform Detect  │               │              │
│  │  • Real Search      │   │  • Auto-fill        │               │              │
│  └─────────────────────┘   └─────────────────────┘               │              │
│                                                                   │              │
└──────────────────────────────────────────────────────────────────┼──────────────┘
                                                                    │
                                                                    ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│                        REACT FRONTEND (Vercel) + CHROME EXTENSION                 │
├──────────────────────────────────────────────────────────────────────────────────┤
│                                                                                   │
│  ┌─────────────────────┐   ┌─────────────────────┐   ┌────────────────────────┐  │
│  │  Dashboard          │   │  AI Chat Assistant  │   │  ScholarStream Copilot │  │
│  │                     │   │                     │   │  (Browser Extension)   │  │
│  │  • Real-time feed   │   │  • On-demand search │   │                        │  │
│  │  • Match scores     │   │  • Transparent UX   │   │  • Form auto-fill      │  │
│  │  • Priority alerts  │   │  • Emergency mode   │   │  • Essay assistant     │  │
│  └─────────────────────┘   └─────────────────────┘   └────────────────────────┘  │
│                                                                                   │
└──────────────────────────────────────────────────────────────────────────────────┘
```

---

## 🔧 KEY TECHNOLOGY COMPONENTS

### 1. Playwright-Powered Hunter Drones
**File:** `backend/app/services/crawler_service.py`

The `UniversalCrawlerService` uses Playwright with advanced stealth techniques:
- **User Agent Rotation** - 5 different browser fingerprints
- **Viewport Randomization** - 4 screen resolutions
- **Anti-Detection Scripts** - Removes `navigator.webdriver`, spoofs plugins
- **Human Simulation** - Mouse movement, scrolling, delays
- **Resource Blocking** - Blocks images, fonts, tracking scripts for speed
- **Deep Scrolling** - Triggers lazy-loaded content (essential for DevPost/DoraHacks)

**Flow:**
1. Drones receive target URLs
2. Create stealth browser contexts
3. Navigate with networkidle/domcontentloaded fallbacks
4. Scroll to load infinite scroll content
5. Extract HTML and publish to Kafka topic `cortex.raw.html.v1`

### 2. Confluent Kafka (The Lifeline)
**File:** `backend/app/services/kafka_config.py`

Six core topics power the system:

| Topic | Purpose |
|-------|---------|
| `user.identity.v1` | User profile updates (compacted) |
| `cortex.commands.v1` | Crawler commands and scheduling |
| `cortex.raw.html.v1` | Raw HTML from drones (high throughput) |
| `opportunity.enriched.v1` | AI-processed verified opportunities |
| `system.alerts.v1` | System health and monitoring |
| `user.matches.v1` | Personalized user notifications |

**Producer Config Highlights:**
- SASL_SSL authentication to Confluent Cloud
- Snappy compression for bandwidth efficiency
- 5 retries with exponential backoff
- 60s socket timeout for resilience

### 3. Cortex (The Brain)
**Location:** `backend/app/services/cortex/`

Three components:
1. **Navigator** (`navigator.py`) - URL discovery and sitemap parsing
2. **Reader LLM** (`reader_llm.py`) - Gemini-powered HTML parsing, extracts multiple opportunities per page
3. **Refinery** (`refinery.py`) - Validates, enriches, and publishes verified opportunities

**Refinery Pipeline:**
```
Raw HTML → Reader LLM (Multi-Extract) → Expiration Gate → Geo-Tagging → Type-Tagging → Kafka/Firestore
```

### 4. Flink-Like Processor
**File:** `backend/app/services/flink_processor.py`

The `CortexFlinkProcessor` provides:
- **Content-Based Deduplication** - SHA256 hash of URL + Title + Organization
- **Firestore Persistence** - Cross-restart deduplication by loading existing IDs
- **Sliding Window** - 1-hour window for processing queue stats
- **No Eviction** - Seen IDs never expire (100k IDs ≈ 5MB RAM)

### 5. Personalization Engine
**File:** `backend/app/services/personalization_engine.py`

**4-Factor Scoring (0-100):**
| Factor | Weight | Description |
|--------|--------|-------------|
| Interest Match | 40% | User interests vs opportunity tags (with synonym expansion) |
| Passion Alignment | 30% | User background vs opportunity description |
| Demographic Match | 20% | GPA, major, location, citizenship checks |
| Academic Fit | 10% | Grade level eligibility |

**Key Features:**
- 17+ interest categories with keyword synonyms
- No artificial floor - true scores from 10-100
- Semantic scoring via Gemini (optional)

### 6. AI Chat Service
**File:** `backend/app/services/chat_service.py`

- **Emergency Mode Detection** - Triggers empathy-first responses
- **Search Intent Detection** - Automatically searches database
- **Transparent UX** - Shows thinking process, search stats, filtering details
- **Real Database Search** - Filters by type, urgency, location, eligibility

### 7. Copilot Service (Platform-Aware)
**File:** `backend/app/services/copilot_service.py`

**Platform Personas:**
- DevPost Hackathon Coach
- DoraHacks BUIDL Expert
- MLH Hackathon Mentor
- Gitcoin Grants Advisor
- HackerOne Bug Bounty Specialist
- Immunefi Web3 Security Researcher
- Kaggle Competition Expert

**Tri-Fold Knowledge Base:**
1. User Profile (skills, interests, background)
2. Uploaded Documents (resume, project docs)
3. Real-Time Page Context (current URL content)

---

## 🚨 CRITICAL ISSUES TO ADDRESS

### ISSUE #1: Static Dashboard - Scraping Not Updating

**Symptoms:**
- Dashboard looks identical after 30 minutes, hours, or days
- New users see the same opportunities as existing users
- No evidence of real-time opportunity discovery

**Suspected Root Causes:**

1. **Crawler Scheduler Not Running**
   - File: `backend/app/services/crawler_scheduler.py`
   - The scheduled crawl jobs may not be starting or completing
   - No cron/APScheduler triggering the drones

2. **Kafka Not Connected/Publishing**
   - Confluent credentials may be missing/invalid
   - Topic creation may have failed
   - Messages may be stuck in producer buffer

3. **Cortex Refinery Not Consuming**
   - Consumer may not be subscribed or processing
   - Reader LLM may be failing silently
   - Expiration gate may be filtering everything

4. **Firestore Writes Failing**
   - Heartbeat fallback (`_persist_fallback`) may be erroring
   - Firebase initialization issues

5. **Possible Cached Seed Data**
   - File: `backend/app/services/seeds.py`
   - The app may be displaying static seed data instead of live data
   - Check `db.get_all_scholarships()` source

**Investigation Steps:**
```bash
# Check Kafka connectivity
python backend/test_kafka_connection.py

# Check crawler is running
# Look for logs: "Deploying Hunter Drone Squad", "Drone transmitted payload"

# Check Cortex refinery logs
# Look for: "Refinery V2: Processing Raw Event", "Extracted N opportunities"

# Check Firestore for recent entries
# Query: db.collection('scholarships').order_by('created_at', 'DESCENDING').limit(10)
```

**Required Fixes:**
- Implement robust scheduler with APScheduler/Celery
- Add startup health checks for Kafka connectivity
- Add monitoring dashboard for pipeline status
- Implement retry logic for failed crawl jobs
- Ensure Cortex consumer is always running

---

### ISSUE #2: Platform Coverage is Abysmal

**Current State:**
- Devpost: ✅ Supported
- DoraHacks: ❌ NOT WORKING
- TAIKAI: ❌ NOT CRAWLING
- HackQuest: ❌ NOT CRAWLING
- Gitcoin: ❓ Limited
- Immunefi: ❌ NOT CRAWLING
- Superteam Earn: ❌ NOT CRAWLING
- Kaggle: ❓ Unknown

**Bounties specifically:**
- Users see only ~3 bounties
- Major bounty platforms completely missing
- Bug bounty platforms (HackerOne, Immunefi) not integrated

**Required Action:**

1. **Add New Source Scrapers:**
   ```
   DoraHacks:       https://dorahacks.io/hackathon
   TAIKAI:          https://taikai.network/hackathons
   HackQuest:       https://www.hackquest.io/
   Superteam Earn:  https://earn.superteam.fun/
   Immunefi:        https://immunefi.com/explore/
   HackerOne:       https://hackerone.com/bug-bounty-programs
   Bugcrowd:        https://bugcrowd.com/programs
   ETHGlobal:       https://ethglobal.com/events

   ```

2. **Implement Source-Specific Parsers:**
   Each platform has unique HTML structure. The Reader LLM needs platform-specific extraction prompts.

3. **Bounty-Specific Schema:**
   ```python
   class BountySchema:
       platform: str  # immunefi, hackerone, gitcoin
       reward_range: Tuple[int, int]  # min, max
       asset_type: str  # smart_contract, web_app, api
       severity_levels: List[str]
       in_scope: List[str]
       launch_date: datetime
   ```

---

### ISSUE #3: Personalization is Broken

**Symptoms:**
- Different users with different profiles see identical opportunities
- Match scores don't reflect actual relevance
- No observable personalization in dashboard ranking

**Suspected Root Causes:**

1. **Personalization Engine Not Being Called**
   - Check: `backend/app/routes/scholarships.py`
   - The `personalization_engine.calculate_personalized_score()` may not be invoked

2. **User Profile Not Loaded**
   - Profile data may be empty or missing at scoring time
   - Firebase user document may not have onboarding data

3. **Scoring Returns Neutral Values**
   - Empty interests/background defaults to 50% score
   - All opportunities get similar neutral scores

**Investigation:**
```python
# Add logging to personalization_engine.py
logger.info("Personalization Input", 
    user_id=user_id,
    interests=user_profile.interests,
    background=user_profile.background,
    opportunity=opportunity.name[:30]
)
```

**Required Fixes:**
- Ensure personalization is called for EVERY opportunity fetch
- Validate user profile is fully loaded from Firestore
- Add unit tests for personalization engine
- Implement A/B testing framework to validate personalization

---

### ISSUE #4: AI Chat Returns Hardcoded/Static Results

**Symptoms:**
- Chat always returns same 2 opportunities regardless of prompt
- No visible "thinking process" UX
- Results don't match user query

**Suspected Root Causes:**

1. **Search Not Executing**
   - `_detect_search_intent()` may be returning False
   - `_search_opportunities_with_stats()` may be failing

2. **Database Query Returning Cached Results**
   - `db.get_all_scholarships()` may be reading from cache
   - No actual Firestore query happening

3. **Gemini API Failing Silently**
   - API key issues, rate limits, or quota exceeded
   - Error caught and generic response returned

**Required Fixes:**
- Add extensive logging to chat flow
- Implement streaming "thinking" UX (like Claude's thinking blocks)
- Show sites being explored in real-time
- Add retry logic for Gemini API calls
- Cache invalidation strategy

**Target UX:**
```
🔍 Analyzing your request...
📋 Searching for: hackathons + blockchain + urgent
🌐 Scanning: DevPost, DoraHacks, TAIKAI...
📊 Found 47 opportunities, filtering...
✅ 5 excellent matches found!

[Opportunity Cards with Match Reasons]
```

---

### ISSUE #5: Copilot Not Using Three Knowledge Bases

**Expected Behavior:**
- Uses User Profile from onboarding
- Uses Uploaded Documents (resume, cover letters)
- Uses Current Page Context (URL, visible content)

**Current Issues:**
- Platform detection may not be working
- Uploaded documents may not be retrievable
- Page content may be truncated/missing

**Investigation:**
- Check `copilot_service._detect_platform()` with various URLs
- Verify document upload/retrieval flow
- Test with different platforms (DevPost vs DoraHacks)

---

## 🎯 PRIORITY ACTION ITEMS

### P0 (Do First - This Week)
1. **Diagnose Crawler Pipeline** - Figure out WHY opportunities aren't updating
2. **Verify Kafka Connectivity** - Ensure producer/consumer are working
3. **Fix Personalization Flow** - Ensure scores are actually personalized
4. **Implement Robust Logging** - You can't fix what you can't see

### P1 (Critical - Next 2 Weeks)
5. **Add New Platforms** - DoraHacks, TAIKAI, HackQuest, Superteam
6. **Add Bounty Sources** - Immunefi, HackerOne, Gitcoin bounties
7. **Fix Chat UX** - Streaming thinking, real-time feedback
8. **Implement Scheduler** - APScheduler with 6-hour crawl intervals

### P2 (Important - Month)
9. **WebSocket Real-Time Push** - New opportunities appear live
10. **Copilot Enhancement** - Full tri-fold knowledge base
11. **Monitoring Dashboard** - Pipeline health, crawl stats
12. **Rate Limiting** - Prevent platform blocks

---

## 📁 KEY FILES REFERENCE

### Backend (Python)
| File | Purpose |
|------|---------|
| `backend/app/services/crawler_service.py` | Playwright Hunter Drones |
| `backend/app/services/kafka_config.py` | Confluent Kafka configuration |
| `backend/app/services/flink_processor.py` | Deduplication engine |
| `backend/app/services/cortex/refinery.py` | AI enrichment pipeline |
| `backend/app/services/cortex/reader_llm.py` | Gemini HTML parser |
| `backend/app/services/chat_service.py` | AI chat assistant |
| `backend/app/services/copilot_service.py` | Platform-aware copilot |
| `backend/app/services/personalization_engine.py` | Match scoring |
| `backend/app/services/crawler_scheduler.py` | Scheduled crawl jobs |
| `backend/app/routes/websocket.py` | Real-time WebSocket |
| `backend/app/routes/scholarships.py` | Opportunity API endpoints |

### Frontend (React/TypeScript)
| File | Purpose |
|------|---------|
| `src/pages/Dashboard.tsx` | Main dashboard view |
| `src/hooks/useRealtimeOpportunities.ts` | WebSocket hook |
| `src/components/ScholarshipCard.tsx` | Opportunity cards |

### Extension
| File | Purpose |
|------|---------|
| `extension/content.js` | Form detection & auto-fill |
| `extension/manifest.json` | Extension configuration |

---

## 💡 ENGINEERING STANDARDS TO UPHOLD

1. **Observability First** - Every critical path must have structured logging (structlog is already used)
2. **Graceful Degradation** - Heartbeat fallback pattern (Kafka fails → direct DB write)
3. **Idempotency** - Content-based deduplication prevents duplicates
4. **Resilience** - Exponential backoff, retry logic, timeout handling
5. **Security** - SASL_SSL for Kafka, Firebase Auth for endpoints

---

## 🙏 CLOSING

This platform has immense potential to change lives. $2.9 billion in scholarships go unclaimed every year because students don't know they exist. ScholarStream can fix that.

But right now, the plumbing is broken. The vision is clear. The architecture is sound. **We need execution.**

I'm counting on you to bring FAANG-level engineering excellence to make this platform the gold standard in opportunity discovery.

**Bismillah. Let's build something extraordinary.**

---

## APPENDIX: Quick Start Commands

```bash
# Start backend
cd backend
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000

# Start frontend
cd ..
npm install
npm run dev

# Test Kafka
python backend/test_kafka_connection.py

# Run crawler manually
python -c "from app.services.crawler_service import crawler_service; import asyncio; asyncio.run(crawler_service.crawl_and_stream(['https://devpost.com/hackathons']))"
```

---

*Document Version: 1.0*
*Last Updated: December 2025*
*Author: Muhammad Rasheed (Founder)*
