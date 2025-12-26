# 🎓 ScholarStream - AI-Powered Scholarship Discovery Platform

**Find unclaimed scholarships, track deadlines, and apply faster with AI assistance.**

ScholarStream uses advanced web scraping and AI matching to discover scholarships, hackathons, bounties, and competitions tailored to your profile. With real-time deadline tracking and an AI copilot to guide applications, students can access $2.9 billion in unclaimed opportunities.

---

## ✨ Features

- 🔍 **Smart Discovery**: Scrapes 60-140+ opportunities from Devpost, MLH, Kaggle, Gitcoin, and more
- 🤖 **AI Matching**: Gemini AI ranks opportunities by relevance to your profile
- 💬 **AI Chat Assistant**: Get personalized recommendations and application help
- 📊 **Dashboard**: Track applications, deadlines, and potential earnings
- 📝 **Guided Applications**: Step-by-step workflows with AI essay assistance
- 🔖 **Bookmarking**: Save and organize opportunities
- 📱 **Responsive Design**: Optimized for desktop, tablet, and mobile

---

## 🚀 Tech Stack

### Frontend
- **Framework**: React 18 + TypeScript + Vite
- **UI**: Tailwind CSS + shadcn/ui components
- **State**: React Query + Context API
- **Auth**: Firebase Authentication
- **Routing**: React Router v6

### Backend
- **Framework**: FastAPI (Python 3.11)
- **AI**: Google Gemini 2.5 Flash
- **Database**: Firebase Firestore
- **Scraping**: BeautifulSoup4 + Selenium + httpx
- **Caching**: Upstash Redis
- **Storage**: Cloudinary
- **Scheduling**: APScheduler

---

## 📦 Local Development

### Prerequisites
- Node.js 18+ and npm
- Python 3.11+
- Firebase project
- Gemini API key

### Frontend Setup

```bash
# Clone the repository
git clone <YOUR_GIT_URL>
cd scholarstream-v2

# Install dependencies
npm install

# Create .env file
cp .env.example .env

# Add your environment variables
VITE_API_BASE_URL=http://localhost:8000
VITE_FIREBASE_API_KEY=your_firebase_api_key
VITE_FIREBASE_AUTH_DOMAIN=your_project.firebaseapp.com
VITE_FIREBASE_PROJECT_ID=your_project_id
VITE_FIREBASE_STORAGE_BUCKET=your_project.appspot.com
VITE_FIREBASE_MESSAGING_SENDER_ID=your_sender_id
VITE_FIREBASE_APP_ID=your_app_id

# Start development server
npm run dev
```

### Backend Setup

```bash
cd backend

# Create virtual environment
python -m venv venv

# Activate virtual environment
# Windows:
venv\Scripts\activate
# macOS/Linux:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Create .env file
cp .env.example .env

# Add your environment variables (see backend/.env.example)

# Start backend server
python run.py
```

Backend will run at `http://localhost:8000`  
API docs available at `http://localhost:8000/docs`

---

## 🌐 Deployment

### Frontend - Vercel (Free Tier)

1. **Push to GitHub**
   ```bash
   git add .
   git commit -m "Ready for deployment"
   git push origin main
   ```

2. **Deploy to Vercel**
   - Go to [vercel.com](https://vercel.com)
   - Click "Import Project"
   - Select your GitHub repository
   - Framework preset: **Vite**
   - Root directory: **.**
   - Build command: `npm run build`
   - Output directory: `dist`

3. **Add Environment Variables** in Vercel dashboard:
   - `VITE_API_BASE_URL` = Your Render backend URL
   - `VITE_FIREBASE_API_KEY`
   - `VITE_FIREBASE_AUTH_DOMAIN`
   - `VITE_FIREBASE_PROJECT_ID`
   - `VITE_FIREBASE_STORAGE_BUCKET`
   - `VITE_FIREBASE_MESSAGING_SENDER_ID`
   - `VITE_FIREBASE_APP_ID`

4. **Deploy** - Vercel will automatically build and deploy

### Backend - Render (Free Tier)

1. **Create Render Account** at [render.com](https://render.com)

2. **Create New Web Service**
   - Connect your GitHub repository
   - Name: `scholarstream-backend`
   - Region: Select closest to your users
   - Branch: `main`
   - Root Directory: `backend`
   - Runtime: **Python 3**
   - Build Command: `pip install -r requirements.txt`
   - Start Command: `python run.py`

3. **Add Environment Variables** in Render dashboard:
   ```
   HOST=0.0.0.0
   PORT=10000
   DEBUG=False
   ENVIRONMENT=production
   
   # Firebase credentials
   FIREBASE_PROJECT_ID=your_project_id
   FIREBASE_PRIVATE_KEY_ID=your_key_id
   FIREBASE_PRIVATE_KEY="-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n"
   FIREBASE_CLIENT_EMAIL=firebase-adminsdk-xxxxx@your-project.iam.gserviceaccount.com
   FIREBASE_CLIENT_ID=your_client_id
   FIREBASE_AUTH_URI=https://accounts.google.com/o/oauth2/auth
   FIREBASE_TOKEN_URI=https://oauth2.googleapis.com/token
   FIREBASE_AUTH_PROVIDER_X509_CERT_URL=https://www.googleapis.com/oauth2/v1/certs
   FIREBASE_CLIENT_X509_CERT_URL=your_cert_url
   
   # Gemini AI
   GEMINI_API_KEY=your_gemini_api_key
   GEMINI_MODEL=gemini-2.5-flash
   
   # Redis (optional but recommended)
   UPSTASH_REDIS_REST_URL=your_redis_url
   UPSTASH_REDIS_REST_TOKEN=your_redis_token
   
   # CORS - Add your Vercel domain
   CORS_ORIGINS=https://your-app.vercel.app,http://localhost:5173
   
   # Cloudinary (optional - for file uploads)
   CLOUDINARY_CLOUD_NAME=your_cloud_name
   CLOUDINARY_API_KEY=your_api_key
   CLOUDINARY_API_SECRET=your_api_secret
   ```

4. **Deploy** - Render will build and deploy automatically

5. **Copy your Render URL** (e.g., `https://scholarstream-backend.onrender.com`)

6. **Update Vercel Environment Variables**
   - Go back to Vercel dashboard
   - Update `VITE_API_BASE_URL` to your Render URL
   - Redeploy frontend

---

## 🔧 Configuration

### Free Tier Optimizations

**Render Free Tier Limitations:**
- Spins down after 15 minutes of inactivity
- 750 hours/month free (enough for 1 service)
- Cold start takes 30-60 seconds

**Optimization Tips:**
1. Use Upstash Redis for caching (reduces API calls)
2. Implement background jobs for scraping (off-peak hours)
3. Set up a cron job to ping your backend every 14 minutes (keeps it warm)

**Vercel Free Tier:**
- 100GB bandwidth/month
- Unlimited deployments
- Automatic HTTPS
- Edge network CDN

---

## 📝 Environment Variables

### Frontend (.env)
```bash
VITE_API_BASE_URL=http://localhost:8000
VITE_FIREBASE_API_KEY=
VITE_FIREBASE_AUTH_DOMAIN=
VITE_FIREBASE_PROJECT_ID=
VITE_FIREBASE_STORAGE_BUCKET=
VITE_FIREBASE_MESSAGING_SENDER_ID=
VITE_FIREBASE_APP_ID=
```

### Backend (backend/.env)
See `backend/.env.example` for complete list

---

## 🧪 Testing

```bash
# Frontend tests
npm run test

# Backend tests
cd backend
pytest

# API health check
curl https://your-backend.onrender.com/health
```

---

## 📚 Documentation

- [Backend Setup Guide](./BACKEND_SETUP_GUIDE.md)
- [Deployment Guide](./DEPLOY_GUIDE.md)
- [Firebase Setup](./FIREBASE_SETUP.md)
- [Testing Guide](./TESTING_GUIDE.md)

---

## 🤝 Contributing

Contributions are welcome! Please:
1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---

## 📄 License

This project is built with [Lovable](https://lovable.dev) and is open for educational purposes.

---

## 🙏 Acknowledgments

- **Gemini AI** for intelligent matching
- **Firebase** for authentication and database
- **Upstash** for serverless Redis
- **Devpost, MLH, Kaggle, Gitcoin** for opportunity data

---

## 📧 Contact

For questions or support, open an issue on GitHub.

---

**Built with ❤️ for students seeking financial opportunities**
