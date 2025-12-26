# ScholarStream - Complete Project Documentation

**Version:** 1.0.0  
**Last Updated:** 04 December 2025  
**Purpose:** AI-Powered Scholarship Discovery & Application Platform

---

## 🎯 Project Mission & Vision

### Mission
ScholarStream democratizes access to educational funding by using AI to discover, match, and streamline scholarship applications for students worldwide. We address the $2.9 billion in unclaimed scholarships annually.

### Vision
Become the world's most intelligent opportunity discovery platform, helping students find and secure funding through:
- **AI-Powered Matching**: Gemini AI analyzes user profiles against 1,500+ opportunities
- **Multi-Source Aggregation**: Scholarships, hackathons, bounties, competitions
- **Intelligent Application Assistance**: AI copilot for essays and form filling
- **Real-Time Tracking**: Deadline monitoring and application progress

---

## 🏗️ Architecture Overview

### Tech Stack

**Frontend:**
- React 18.3.1 + TypeScript
- Vite (build tool)
- TailwindCSS + shadcn/ui components
- React Query (state management & caching)
- React Router v6 (routing)
- Firebase Authentication

**Backend:**
- FastAPI 0.115.5 (Python 3.11)
- Uvicorn/Gunicorn (ASGI server)
- Google Gemini 2.5 Flash (AI)
- Firebase Admin SDK
- Upstash Redis (caching)
- APScheduler (background jobs)

**Database:**
- Firebase Firestore (NoSQL)

**Scraping Infrastructure:**
- BeautifulSoup4 + lxml
- Selenium (dynamic content)
- httpx (async HTTP)
- fake-useragent (rotation)

**Deployment:**
- Frontend: Vercel
- Backend: Render
- Database: Firebase Cloud

---

## 📁 Project Structure

```
scholarstream/
├── src/                          # Frontend React application
│   ├── components/               # Reusable UI components (99 files)
│   │   ├── dashboard/           # Dashboard-specific components
│   │   ├── onboarding/          # Multi-step onboarding flow
│   │   └── ui/                  # shadcn/ui base components
│   ├── pages/                   # Route pages (14 pages)
│   │   ├── Landing.tsx          # Public landing page
│   │   ├── SignUp.tsx           # User registration
│   │   ├── Login.tsx            # Authentication
│   │   ├── Onboarding.tsx       # 9-step profile builder
│   │   ├── Dashboard.tsx        # Main opportunity feed
│   │   ├── OpportunityDetail.tsx # Detailed scholarship view
│   │   ├── Apply.tsx            # Application workflow
│   │   ├── Applications.tsx     # Application tracker
│   │   └── Profile.tsx          # User profile management
│   ├── hooks/                   # Custom React hooks
│   │   ├── useScholarships.ts   # Main data fetching hook
│   │   └── use-toast.ts         # Toast notifications
│   ├── services/                # API integration
│   │   ├── api.ts               # Backend API client
│   │   └── matchingEngine.ts    # Client-side filtering
│   ├── contexts/                # React Context providers
│   │   └── AuthContext.tsx      # Firebase auth state
│   ├── lib/                     # Utilities
│   │   ├── firebase.ts          # Firebase initialization
│   │   └── queryClient.ts       # React Query config
│   └── types/                   # TypeScript definitions
│
├── backend/                     # FastAPI Python backend
│   ├── app/
│   │   ├── main.py             # FastAPI app entry point
│   │   ├── config.py           # Environment configuration
│   │   ├── database.py         # Firestore operations (484 lines)
│   │   ├── models.py           # Pydantic data models (310 lines)
│   │   ├── routes/             # API endpoints
│   │   │   ├── scholarships.py # Scholarship discovery & matching
│   │   │   ├── applications.py # Application management
│   │   │   └── chat.py         # AI chat assistant
│   │   └── services/           # Business logic
│   │       ├── ai_service.py          # Gemini AI integration
│   │       ├── matching_service.py    # Personalization engine
│   │       ├── chat_service.py        # Conversational AI
│   │       ├── scraper_service.py     # Scraping orchestrator
│   │       ├── background_jobs.py     # Scheduled tasks
│   │       └── scrapers/              # Platform-specific scrapers
│   │           ├── scholarships_com.py
│   │           ├── devpost.py
│   │           ├── mlh.py
│   │           ├── kaggle.py
│   │           └── gitcoin.py
│   ├── requirements.txt        # Python dependencies
│   └── run.py                  # Development server
│
├── extension/                   # Browser extension (future)
│   └── manifest.json           # Chrome extension config
│
├── public/                      # Static assets
├── .env                        # Environment variables
├── package.json                # Node dependencies
├── vite.config.ts              # Vite configuration
├── tailwind.config.ts          # Tailwind CSS config
└── SCHOLARSTREAM_BLUEPRINT.md  # Detailed implementation plan
```

---

## 🔄 Data Flow & System Architecture

### 1. User Onboarding Flow

```
Landing Page → Sign Up → Login → Onboarding (9 steps) → Discovery → Dashboard
```

**Onboarding Steps:**
1. Name & basic info
2. Motivation (Urgent funding, Planning ahead, etc.)
3. Academic status (High school, Undergraduate, etc.)
4. Profile details (GPA, major, graduation year)
5. Interests & skills
6. Background (First-gen, Minority, LGBTQ+, etc.)
7. Time availability
8. Location (Country, State, City)
9. Completion & celebration

**Data Collected:**
- Demographics (name, academic status, school, GPA, major)
- Financial need
- Interests & skills
- Background identifiers
- Time commitment preferences
- Geographic location

### 2. Scholarship Discovery Pipeline

```
User Profile → Backend API → Scraper Service → Multiple Sources
                                    ↓
                            Raw Scholarships
                                    ↓
                            Gemini AI Enrichment
                                    ↓
                            Matching Engine
                                    ↓
                            Personalized Feed → Frontend
```

**Discovery Process:**
1. **Trigger**: User completes onboarding or manually refreshes
2. **Scraping**: Backend scrapes 60-140+ opportunities from:
   - Scholarships.com
   - Devpost (hackathons)
   - MLH (Major League Hacking)
   - Kaggle (data science competitions)
   - Gitcoin (crypto bounties)
3. **AI Enrichment**: Gemini AI parses raw data into structured format:
   - Eligibility criteria (GPA, major, citizenship, etc.)
   - Requirements (essays, letters, transcripts)
   - Tags & categorization
4. **Matching**: Personalization engine calculates match scores (0-100)
5. **Caching**: Results stored in Firestore + Upstash Redis
6. **Delivery**: Frontend receives ranked, personalized opportunities

### 3. AI Matching Algorithm

**Match Score Calculation (0-100 points):**

```python
# Weighted scoring system
weights = {
    'eligibility': 30,    # Must meet basic requirements
    'interests': 25,      # Alignment with user interests
    'urgency': 20,        # Time-sensitive needs
    'value': 15,          # Financial impact
    'effort': 10          # Time to complete vs availability
}
```

**Eligibility Scoring (30 points):**
- GPA match: Hard requirement (disqualify if below minimum)
- Grade level: Must match (High School, Undergrad, Grad)
- Major alignment: Penalty if mismatch
- Location: State/nationwide eligibility
- Citizenship: Hard requirement

**Interest Alignment (25 points):**
- Jaccard similarity between user interests and opportunity tags
- Bonus for exact major match

**Urgency Match (20 points):**
- User needs urgent funding → prioritize deadlines <7 days
- User planning ahead → prioritize deadlines >60 days

**Value Score (15 points):**
- Ratio of scholarship amount to user's financial need
- Higher scores for opportunities covering significant need

**Effort Feasibility (10 points):**
- Estimated application time vs user availability
- Essay count, recommendation letters, transcript requirements

**Match Tiers:**
- Excellent: 80-100 (top priority)
- Good: 60-79 (recommended)
- Fair: 40-59 (consider)
- Poor: 0-39 (filtered out)

**Priority Levels:**
- URGENT: Deadline <7 days
- HIGH: Match score ≥85 or high value
- MEDIUM: Match score 60-84
- LOW: Match score <60

### 4. State Management & Caching

**React Query Strategy:**
```typescript
// 5-minute stale time, 30-minute cache time
useQuery({
  queryKey: ['scholarships', userId],
  queryFn: () => apiService.getMatchedScholarships(userId),
  staleTime: 5 * 60 * 1000,
  cacheTime: 30 * 60 * 1000,
  refetchOnWindowFocus: false,
  placeholderData: (previousData) => previousData  // Show stale data while fetching
})
```

**Backend Caching:**
- Upstash Redis: AI enrichment results (24-hour TTL)
- Firestore: User matches, application drafts
- In-memory fallback: When Redis unavailable

---

## 🗄️ Database Schema (Firebase Firestore)

### Collections

**1. `users` Collection**
```javascript
{
  uid: "firebase_auth_uid",
  email: "user@example.com",
  profile: {
    name: "John Doe",
    academic_status: "Undergraduate",
    school: "MIT",
    year: "Junior",
    gpa: 3.8,
    major: "Computer Science",
    graduation_year: "2025",
    background: ["First-generation", "Low-income"],
    financial_need: 25000,
    interests: ["AI", "Machine Learning", "Robotics"],
    country: "United States",
    state: "MA",
    city: "Cambridge"
  },
  saved_scholarships: ["scholarship_id_1", "scholarship_id_2"],
  onboarding_completed: true,
  created_at: Timestamp,
  updated_at: Timestamp
}
```

**2. `scholarships` Collection**
```javascript
{
  id: "unique_scholarship_id",
  name: "Google Scholarship for CS Students",
  organization: "Google",
  logo_url: "https://...",
  amount: 10000,
  amount_display: "$10,000",
  deadline: "2025-03-15T23:59:59Z",
  deadline_type: "fixed",  // or "rolling"
  
  eligibility: {
    gpa_min: 3.5,
    grades_eligible: ["Undergraduate", "Graduate"],
    majors: ["Computer Science", "Software Engineering"],
    gender: null,
    citizenship: "US Citizen",
    backgrounds: ["Minority", "First-generation"],
    states: null  // nationwide
  },
  
  requirements: {
    essay: true,
    essay_prompts: ["Why do you want to study CS?"],
    recommendation_letters: 2,
    transcript: true,
    resume: true,
    other: ["Portfolio of projects"]
  },
  
  match_score: 92.5,
  match_tier: "Excellent",
  priority_level: "HIGH",
  
  tags: ["STEM", "Computer Science", "Merit-Based", "Diversity"],
  description: "Full scholarship description...",
  competition_level: "High",
  estimated_time: "4-6 hours",
  expected_value: 10000,
  
  source_url: "https://...",
  source_type: "scraped",
  discovered_at: "2024-12-01T10:00:00Z",
  last_verified: "2024-12-04T15:30:00Z"
}
```

**3. `user_matches` Collection**
```javascript
{
  user_id: "firebase_uid",
  scholarship_ids: ["id1", "id2", "id3", ...],
  updated_at: Timestamp
}
```

**4. `applications` Collection**
```javascript
{
  application_id: "unique_app_id",
  user_id: "firebase_uid",
  scholarship_id: "scholarship_id",
  scholarship_name: "Google Scholarship",
  scholarship_amount: 10000,
  
  status: "draft",  // draft, submitted, under_review, finalist, awarded, declined, expired
  current_step: 3,
  progress_percentage: 45.0,
  
  personal_info: {
    full_name: "John Doe",
    email: "john@example.com",
    phone: "+1234567890",
    school_name: "MIT",
    gpa: 3.8,
    major: "Computer Science",
    // ... more fields
  },
  
  documents: [
    {
      document_type: "transcript",
      file_name: "transcript.pdf",
      file_url: "https://cloudinary.com/...",
      cloudinary_public_id: "...",
      uploaded_at: Timestamp
    }
  ],
  
  essays: [
    {
      prompt: "Why do you want to study CS?",
      content: "Essay content...",
      word_count: 450,
      last_edited: Timestamp
    }
  ],
  
  recommenders: [
    {
      name: "Prof. Smith",
      email: "smith@mit.edu",
      relationship: "Professor",
      status: "submitted",
      requested_at: Timestamp,
      submitted_at: Timestamp
    }
  ],
  
  created_at: Timestamp,
  updated_at: Timestamp,
  last_saved: Timestamp,
  
  // If submitted:
  confirmation_number: "AS-2024-A1B2C3",
  submitted_at: Timestamp,
  decision_date: null,
  award_amount: null
}
```

**5. `discovery_jobs` Collection**
```javascript
{
  job_id: "unique_job_id",
  user_id: "firebase_uid",
  status: "processing",  // idle, processing, completed, failed
  progress: 65,  // 0-100
  scholarships_found: 87,
  started_at: Timestamp,
  updated_at: Timestamp
}
```

**6. `chat_history` Collection**
```javascript
// Subcollection structure: chat_history/{user_id}/messages/{message_id}
{
  role: "user",  // or "assistant"
  content: "What scholarships match my profile?",
  timestamp: Timestamp
}
```

---

## 🤖 AI Integration (Google Gemini)

### AI Service Architecture

**Model:** `gemini-2.5-flash`  
**Rate Limit:** Configurable (default: 1000 calls/hour)  
**Caching:** Upstash Redis (24-hour TTL) + in-memory fallback

### Use Cases

**1. Scholarship Enrichment**
- **Input**: Raw scraped data (name, description, eligibility text)
- **Output**: Structured JSON with:
  - Parsed eligibility criteria
  - Application requirements
  - Match score calculation
  - Tags and categorization
  - Competition level assessment
  - Time estimation

**2. Chat Assistant**
- Context-aware conversations
- Scholarship recommendations
- Application guidance
- Essay brainstorming
- Deadline reminders

**3. Essay Assistance** (Future)
- Draft generation
- Grammar checking
- Tone improvement
- Word count optimization

### AI Prompt Engineering

**Enrichment Prompt Structure:**
```
You are an expert scholarship analyst. Analyze this scholarship and provide structured data.

SCHOLARSHIP DATA:
[Raw scholarship information]

USER PROFILE:
[User's academic and demographic data]

TASK:
Provide JSON response with eligibility, requirements, tags, match_score, etc.

Calculate match_score based on:
- GPA match (0-25 points)
- Major/field alignment (0-20 points)
- Background eligibility (0-25 points)
- Interest alignment (0-15 points)
- Financial need match (0-15 points)
```

**Response Parsing:**
- Removes markdown code blocks
- Validates JSON structure
- Fallback to default values on error
- Logs failures for monitoring

---

## 🔌 API Endpoints

### Base URL
- **Development**: `http://localhost:8000`
- **Production**: `https://scholarstream-backend.onrender.com`

### Authentication
- Firebase ID tokens in `Authorization: Bearer <token>` header

### Endpoints

**Scholarships:**
```
POST   /api/scholarships/discover
  Body: { user_id, profile }
  Response: { status, immediate_results, job_id, estimated_completion }

GET    /api/scholarships/matched/{user_id}
  Response: { scholarships[], total_value, last_updated }

GET    /api/scholarships/progress/{job_id}
  Response: { status, progress, new_scholarships, total_found }

POST   /api/scholarships/save
  Body: { user_id, scholarship_id }

POST   /api/scholarships/unsave
  Body: { user_id, scholarship_id }
```

**Applications:**
```
POST   /api/applications/start
  Body: { user_id, scholarship_id }
  Response: { application_id }

POST   /api/applications/save-draft
  Body: { user_id, scholarship_id, current_step, progress_percentage, ... }

GET    /api/applications/{user_id}
  Response: { applications[] }

POST   /api/applications/submit
  Body: { user_id, scholarship_id, personal_info, documents, essays, ... }
  Response: { confirmation_number }
```

**Chat:**
```
POST   /api/chat/message
  Body: { user_id, message, conversation_history }
  Response: { response, suggestions }

GET    /api/chat/history/{user_id}
  Response: { messages[] }

DELETE /api/chat/history/{user_id}
```

---

## 🕷️ Scraping Infrastructure

### Scraper Architecture

**Orchestrator:** `scraper_service.py`
- Manages multiple platform scrapers
- Implements retry logic with exponential backoff
- Rotates user agents
- Handles rate limiting

**Platform Scrapers:**

1. **Scholarships.com** (`scholarships_com.py`)
   - Method: BeautifulSoup + httpx
   - Frequency: Daily
   - Yield: 30-50 scholarships

2. **Devpost** (`devpost.py`)
   - Method: Selenium (dynamic content)
   - Frequency: Every 6 hours
   - Yield: 20-40 hackathons

3. **MLH** (`mlh.py`)
   - Method: BeautifulSoup
   - Frequency: Weekly
   - Yield: 15-25 hackathons

4. **Kaggle** (`kaggle.py`)
   - Method: httpx + JSON API
   - Frequency: Daily
   - Yield: 10-20 competitions

5. **Gitcoin** (`gitcoin.py`)
   - Method: httpx + GraphQL
   - Frequency: Daily
   - Yield: 5-15 bounties

### Scraping Strategy

**Data Extraction:**
```python
{
  "name": str,
  "organization": str,
  "amount": float,
  "deadline": str (ISO format),
  "description": str,
  "source_url": str,
  "eligibility_raw": str,  # Unstructured text
  "requirements_raw": str  # Unstructured text
}
```

**Post-Processing:**
1. Deduplication (by URL)
2. AI enrichment (Gemini)
3. Validation
4. Firestore storage

**Error Handling:**
- Retry failed requests (3 attempts)
- Fallback to cached data
- Log errors for monitoring
- Continue with partial results

---

## 🎨 Frontend Architecture

### Component Hierarchy

**Layout Components:**
- `App.tsx`: Root component with providers
- `ProtectedRoute`: Auth guard for private routes
- `ErrorBoundary`: Global error handling
- `LoadingScreen`: Suspense fallback

**Page Components:**
- `Landing`: Marketing page with features
- `SignUp/Login`: Authentication flows
- `Onboarding`: 9-step wizard
- `Dashboard`: Main opportunity feed
- `OpportunityDetail`: Detailed scholarship view
- `Apply`: Multi-step application form
- `Applications`: Application tracker
- `Profile`: User settings

**Shared Components:**
- `FloatingChatAssistant`: AI chat widget
- `ScholarshipCard`: Opportunity display
- `FilterBar`: Search and filter controls
- `StatsCards`: Dashboard metrics

### State Management

**React Query:**
- Server state caching
- Automatic refetching
- Optimistic updates
- Background synchronization

**Context API:**
- `AuthContext`: Firebase auth state
- User session management
- Protected route logic

**Local State:**
- Form inputs (React Hook Form)
- UI toggles
- Transient discovery progress

### Routing

```typescript
/                      → Landing (public)
/signup                → SignUp (public)
/login                 → Login (public)
/onboarding            → Onboarding (protected, no onboarding required)
/dashboard             → Dashboard (protected)
/opportunity/:id       → OpportunityDetail (protected)
/saved                 → SavedOpportunities (protected)
/apply/:scholarshipId  → Apply (protected)
/applications          → Applications (protected)
/application-tracker   → ApplicationTracker (protected)
/profile               → Profile (protected)
```

---

## 🚀 Deployment & Infrastructure

### Frontend (Vercel)

**Build Configuration:**
```json
{
  "buildCommand": "npm run build",
  "outputDirectory": "dist",
  "framework": "vite"
}
```

**Environment Variables:**
```
VITE_API_BASE_URL=https://scholarstream-backend.onrender.com
VITE_FIREBASE_API_KEY=...
VITE_FIREBASE_AUTH_DOMAIN=...
VITE_FIREBASE_PROJECT_ID=...
VITE_FIREBASE_STORAGE_BUCKET=...
VITE_FIREBASE_MESSAGING_SENDER_ID=...
VITE_FIREBASE_APP_ID=...
```

### Backend (Render)

**Service Configuration:**
```yaml
services:
  - type: web
    name: scholarstream-backend
    env: python
    buildCommand: pip install -r requirements.txt
    startCommand: python run.py
    envVars:
      - key: HOST
        value: 0.0.0.0
      - key: PORT
        value: 10000
      - key: ENVIRONMENT
        value: production
```

**Environment Variables:**
```
FIREBASE_PROJECT_ID=...
FIREBASE_PRIVATE_KEY=...
FIREBASE_CLIENT_EMAIL=...
GEMINI_API_KEY=...
UPSTASH_REDIS_REST_URL=...
UPSTASH_REDIS_REST_TOKEN=...
CORS_ORIGINS=https://scholarstream.vercel.app,http://localhost:5173
```

### Database (Firebase)

**Firestore Indexes:**
- `applications`: (user_id, status, updated_at)
- `scholarships`: (match_score DESC, deadline ASC)

**Security Rules:**
```javascript
rules_version = '2';
service cloud.firestore {
  match /databases/{database}/documents {
    match /users/{userId} {
      allow read, write: if request.auth != null && request.auth.uid == userId;
    }
    match /scholarships/{scholarshipId} {
      allow read: if request.auth != null;
    }
    match /applications/{applicationId} {
      allow read, write: if request.auth != null && 
        resource.data.user_id == request.auth.uid;
    }
  }
}
```

---

## 🔐 Security & Authentication

### Firebase Authentication

**Supported Methods:**
- Email/Password
- Google OAuth
- (Future: GitHub, Microsoft)

**Auth Flow:**
```
1. User signs up/logs in
2. Firebase returns ID token
3. Frontend stores token in memory
4. Backend validates token on each request
5. Token refresh handled automatically
```

**Protected Routes:**
- All routes except `/`, `/signup`, `/login`, `/forgot-password`
- Redirect to `/login` if unauthenticated
- Redirect to `/onboarding` if profile incomplete

### API Security

**Rate Limiting:**
- 100 requests/minute per IP (SlowAPI)
- Gemini AI: 1000 calls/hour

**CORS:**
- Whitelist: Vercel domain + localhost
- Credentials: Allowed
- Methods: GET, POST, PUT, DELETE

**Data Validation:**
- Pydantic models for all requests
- Input sanitization
- SQL injection prevention (NoSQL)

---

## 📊 Performance Optimizations

### Frontend

**Code Splitting:**
- Lazy loading all pages
- Dynamic imports for heavy components
- Route-based chunking

**Caching:**
- React Query: 5-minute stale time
- Service Worker: Static assets (future)
- CDN: Vercel Edge Network

**Bundle Size:**
- Tree shaking
- Minification
- Compression (gzip/brotli)

### Backend

**Database:**
- Batch reads for user matches
- Firestore indexes for fast queries
- Connection pooling

**Caching:**
- Upstash Redis: AI enrichment (24h TTL)
- In-memory: Rate limiting, temporary data

**Background Jobs:**
- APScheduler: Daily scraping at 2 AM UTC
- Async processing: Discovery jobs

---

## 🧪 Testing Strategy

**Frontend:**
- Unit tests: React Testing Library (future)
- E2E tests: Playwright (future)

**Backend:**
- Unit tests: pytest
- Integration tests: Test database
- API tests: httpx test client

**Manual Testing:**
- Onboarding flow
- Discovery process
- Application submission
- Chat interactions

---

## 📈 Future Enhancements

### Phase 1 (Current)
- ✅ Multi-source scraping
- ✅ AI matching
- ✅ Chat assistant
- ✅ Application tracking

### Phase 2 (Planned)
- 🔲 Browser extension (auto-fill forms)
- 🔲 Email notifications
- 🔲 Mobile app (React Native)
- 🔲 Advanced analytics

### Phase 3 (Future)
- 🔲 Scholarship provider portal
- 🔲 Peer review system
- 🔲 Success stories & testimonials
- 🔲 Premium features

---

## 🐛 Known Issues & Limitations

1. **Discovery Speed**: Initial discovery takes 15-30 seconds
   - **Mitigation**: Show immediate cached results, poll for updates

2. **Scraping Reliability**: Websites change structure
   - **Mitigation**: Fallback scrapers, error logging, manual verification

3. **AI Rate Limits**: Gemini API has hourly limits
   - **Mitigation**: Redis caching, batch processing, rate limiting

4. **Free Tier Constraints**:
   - Render: Spins down after 15 min inactivity (30-60s cold start)
   - Vercel: 100GB bandwidth/month
   - **Mitigation**: Upstash Redis keeps backend warm, optimize assets

---

## 📞 Support & Maintenance

**Monitoring:**
- Structlog: Structured logging
- Error tracking: Console logs (future: Sentry)
- Health checks: `/health` endpoint

**Backup:**
- Firestore: Automatic daily backups
- Code: GitHub repository

**Updates:**
- Dependencies: Monthly security updates
- Scrapers: Monitor for website changes
- AI model: Upgrade to newer Gemini versions

---

## 🙏 Acknowledgments

- **Google Gemini**: AI-powered matching
- **Firebase**: Authentication & database
- **Upstash**: Serverless Redis
- **shadcn/ui**: Beautiful components
- **Vercel & Render**: Free hosting

---

**Built with ❤️ for students seeking educational opportunities**

**Bismillah ir-Rahman ir-Rahim** 🤲
