# **Finance News Intelligence System — Full Stack**

An automated system that fetches real-time financial news, filters + deduplicates it, structures articles with Gemini LLM, stores enriched content in MongoDB with TTL cleanup, and exposes REST APIs used by a React + Vite frontend.

Runs continuously every few minutes, fully autonomous.

---

# Overview

### **Backend Pipeline**

* Fetch news from LiveMint, Economic Times, The Hindu
* Apply filtering rules (negative keywords + market impact exceptions)
* Embed + dedupe using SentenceTransformer
* Structure via Gemini (summary, sentiment, category, tickers, tags, impact analysis)
* Load into MongoDB with a 36-hour TTL
* Serve APIs + frontend build via FastAPI
* Continuous scheduler runs every 2 minutes

### **Frontend**

* React + Vite + Tailwind
* Displays latest structured financial news
* Supports searching, filtering, and viewing article details
* Served by FastAPI under `/app`

---

# **Project Structure**

```
.
├── backend/
│   ├── main.py                     # FastAPI server + continuous pipeline scheduler
│   ├── news_fetcher.py             # Scraper for 3 news sources
│   ├── filter.py                   # Title filtering + embeddings + dedupe
│   ├── structurer.py               # Gemini LLM structuring logic
│   ├── db_loader.py                # MongoDB loader with TTL
│   │
│   ├── seen_hashes.json            # Tracks URLs already processed
│   ├── source_state.json           # Tracks per-source last fetch timestamps
│   │
│   ├── staging_raw.json            # Intermediate output: raw fetched news
│   ├── staging_filtered.json       # After title filtering
│   ├── staging_unique.json         # After dedupe
│   ├── news_structured.json        # Final structured output from LLM
│   │
│   └── README.md (optional)
│
├── frontend/
│   ├── public/
│   ├── src/
│   ├── dist/                       # Production build (after npm run build)
│   ├── package.json
│   └── vite.config.js
│
└── README.md                       # (this file)
```

---

# **Backend Setup**

### **1. Create virtual environment**

```
cd backend
python -m venv venv
venv/Scripts/activate   # Windows
# or
source venv/bin/activate   # macOS/Linux
```

### **2. Install dependencies**

```
pip install -r requirements.txt
```

### **3. Environment Variables**

Create a `.env` inside **backend/**:

```
GEMINI_API_KEY=your_key
MONGO_URI=mongodb+srv://user:pass@cluster.mongodb.net/?retryWrites=true&w=majority
MONGO_DB=newsdb
MONGO_COLLECTION=news_structured

INTERVAL_MIN=2

# Frontend build directory (FastAPI serves this)
FRONTEND_DIR=../frontend/dist
```

### **4. Run backend server**

```
cd backend
python main.py
```

Backend starts on:

```
http://localhost:8000
```

Frontend served at:

```
http://localhost:8000/app
```

---

# **Backend Pipeline Explanation**

### **1️⃣ news_fetcher.py**

* Fetches news from 3 websites
* Extracts cleaned article body
* Avoids duplicates using `seen_hashes.json`
* Writes raw results to `staging_raw.json`

### **2️⃣ filter.py**

* Title-based filtering
* Market impact exceptions
* Embedding model → dedupe logic
* Saves:

  * `staging_filtered.json`
  * `staging_unique.json`

### **3️⃣ structurer.py**

* Sends articles to Gemini using strict JSON prompt
* Extracts clean JSON even if LLM outputs text/mixed output
* Validates with JSON Schema
* Saves to `news_structured.json`
* Errors saved in `news_structurer_errors.json`

### **4️⃣ db_loader.py**

* Cleans structured article
* Adds:

  * `_id`
  * `article_id`
  * `stored_at`
  * `expires_at` (+36 hours)
* Upserts documents into MongoDB
* Ensures TTL index

### **5️⃣ main.py**

FastAPI server + background scheduler.

Endpoints:

| Endpoint              | Description                   |
| --------------------- | ----------------------------- |
| `/status`             | Pipeline status               |
| `/run`                | Force a manual pipeline cycle |
| `/articles`           | Latest processed articles     |
| `/articles/search?q=` | Search by title/summary       |
| `/news/json/raw`      | Raw fetched JSON              |
| `/news/json/unique`   | Unique deduped JSON           |
| `/healthz`            | Health check                  |
| `/app`                | Frontend                      |

Scheduler auto-runs every **2 minutes** (configurable by `INTERVAL_MIN`).

---

# **Frontend Setup (React + Vite)**

### **Install Node modules**

```
cd frontend
npm install
```

### **Development mode**

```
npm run dev
```

Opens:

```
http://localhost:5173
```

### **Build for production**

```
npm run build
```

Output goes to:

```
frontend/dist/
```

FastAPI serves this automatically when:

```
FRONTEND_DIR=../frontend/dist
```

### **Frontend tech stack**

* React 18
* Vite 5
* Tailwind CSS
* Axios
* Served through FastAPI

---

# **API Endpoints Used by Frontend**

### Fetch latest articles

```
GET /articles?limit=50
```

### Search articles

```
GET /articles/search?q=reliance
```

### Example fields shown in UI

* Title
* Summary
* Sentiment badge
* Category
* Tags
* "Read Source" link
* Published timestamp

---

# Deployment

### Local deployment

```
npm install --prefix frontend
npm run build --prefix frontend
pip install -r backend/requirements.txt
python backend/main.py
```

### Works on:

* EC2 / Lightsail
* Render
* Railway
* Docker
* Local Windows / Linux

---

# Troubleshooting

### CSS/JS files return 404 in production

Fix Vite base path:

```js
base: "/app/"
```

Rebuild:

```
npm run build
```
