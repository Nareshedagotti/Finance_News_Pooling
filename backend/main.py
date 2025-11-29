#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import asyncio
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from pymongo import DESCENDING
import uvicorn

# ---------- App config ----------
DATA_DIR = os.getenv("DATA_DIR", ".")
RAW_JSON = os.path.join(DATA_DIR, "staging_raw.json")
UNIQUE_JSON = os.path.join(DATA_DIR, "staging_unique.json")
STRUCTURED_JSON = os.path.join(DATA_DIR, "news_structured.json")

INTERVAL_MIN = float(os.getenv("INTERVAL_MIN", "2"))  # schedule (minutes)
FRONTEND_ORIGIN = os.getenv("FRONTEND_ORIGIN", "http://localhost:5173")
FRONTEND_DIR = os.getenv("FRONTEND_DIR")  # e.g., "web/dist" after `npm run build`

os.makedirs(DATA_DIR, exist_ok=True)

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("news-pipeline")

# ---------- Your modules ----------
import news_fetcher
import filter as filter_mod        # avoid built-in 'filter'
import structurer
import db_loader

# TLS-aware Mongo connector used for the read APIs
from tls_client import connect_mongo

# ---------- Pipeline status ----------
class PipelineStatus(BaseModel):
    last_run: Optional[str] = None
    last_result_count: Optional[int] = None
    phase: Optional[str] = None
    ok: bool = True
    error: Optional[str] = None
    running: bool = False
    runs_total: int = 0

STATUS = PipelineStatus()
_run_lock = asyncio.Lock()  # prevent overlapping runs

# Reuse one fetcher across cycles (to avoid re-fetching dupes)
FETCHER = news_fetcher.NewsFetcher()

# ---------- IO helpers ----------
def _save_json(path: str, data: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def _load_json(path: str) -> Any:
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

# ---------- One pipeline run ----------
async def run_pipeline_once(save_intermediate: bool = True) -> int:
    async with _run_lock:
        try:
            STATUS.running = True
            STATUS.ok = True
            STATUS.error = None

            # 1) fetch
            STATUS.phase = "fetch"
            logger.info("Fetching news…")
            raw_items: List[Dict[str, Any]] = FETCHER.fetch_all()
            logger.info("Fetched %d items.", len(raw_items))
            if save_intermediate:
                _save_json(RAW_JSON, raw_items)

            # persist fetcher state so dupes don't reappear
            try:
                FETCHER._save_seen()
                FETCHER._save_source_state()
            except Exception as e:
                logger.warning("Could not persist fetcher state: %s", e)

            # 2) filter + embed + dedupe
            STATUS.phase = "filter"
            logger.info("Filtering + deduping…")
            unique_items = filter_mod.clean_and_dedupe(raw_items)
            logger.info("Unique items after dedupe: %d", len(unique_items))
            if save_intermediate:
                _save_json(UNIQUE_JSON, unique_items)

            # 3) structure (LLM)
            STATUS.phase = "structure"
            logger.info("Structuring via LLM…")
            structured = structurer.structure(unique_items)
            logger.info("Structured items: %d", len(structured))
            if save_intermediate:
                _save_json(STRUCTURED_JSON, structured)

            # 4) save to DB (upsert)
            STATUS.phase = "db"
            logger.info("Saving to MongoDB…")
            saved = db_loader.save(structured)
            logger.info("Upserted %d items.", saved)

            STATUS.last_run = datetime.utcnow().isoformat() + "Z"
            STATUS.last_result_count = int(saved)
            STATUS.phase = None
            STATUS.runs_total += 1
            return int(saved)

        except Exception as e:
            logger.exception("Pipeline failed")
            STATUS.ok = False
            STATUS.error = str(e)
            STATUS.phase = None
            return 0
        finally:
            STATUS.running = False

# ---------- Background scheduler ----------
async def _scheduler_task():
    interval_sec = max(30.0, INTERVAL_MIN * 60.0)  # safety floor
    logger.info("Continuous scheduler started: every %.1f sec", interval_sec)
    while True:
        await run_pipeline_once(save_intermediate=True)
        await asyncio.sleep(interval_sec)

# ---------- FastAPI app ----------
app = FastAPI(title="Finance News Pipeline", version="1.1.0")

# CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=[FRONTEND_ORIGIN, "http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Optionally serve your built frontend (e.g., web/dist)
# Build your React app first:  cd web && npm run build  → sets FRONTEND_DIR=web/dist
if FRONTEND_DIR and os.path.isdir(FRONTEND_DIR):
    # Serve the built UI at /app to avoid intercepting /articles
    app.mount("/app", StaticFiles(directory=FRONTEND_DIR, html=True), name="frontend")
    logger.info("Serving static frontend from: %s at /app", FRONTEND_DIR)
else:
    logger.info("FRONTEND_DIR not set or missing; API-only mode.")

@app.on_event("startup")
async def on_startup():
    asyncio.create_task(_scheduler_task())
    logger.info("Startup complete — background pipeline running continuously.")

# ---------- Control/Status ----------
class RunResponse(BaseModel):
    saved: int
    status: PipelineStatus

@app.get("/status", response_model=PipelineStatus)
async def get_status():
    return STATUS

@app.post("/run", response_model=RunResponse)
async def run_now():
    saved = await run_pipeline_once(save_intermediate=True)
    return RunResponse(saved=saved, status=STATUS)

# ---------- Quick JSON taps (debug) ----------
@app.get("/news/json/raw")
def get_raw_json():
    return _load_json(RAW_JSON) or []

@app.get("/news/json/unique")
def get_unique_json():
    return _load_json(UNIQUE_JSON) or []

@app.get("/news/json/structured")
def get_structured_json():
    return _load_json(STRUCTURED_JSON) or []

from typing import Optional
from pymongo import DESCENDING
from pymongo.errors import PyMongoError, ServerSelectionTimeoutError, AutoReconnect
from tls_client import connect_mongo

# Single, module-level client holder
_client: Optional["MongoClient"] = None  # type: ignore[name-defined]

def _mongo_collection():
    """
    Returns a live collection handle. If the cached client is dead after
    a network change, it reconnects once and returns a fresh handle.
    """
    global _client
    mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    mongo_db = os.getenv("MONGO_DB", "newsdb")
    mongo_col = os.getenv("MONGO_COLLECTION", "news_structured")

    # Lazily create client
    if _client is None:
        _client = connect_mongo(mongo_uri)

    # Ensure it's alive; if not, reconnect once
    try:
        _client.admin.command("ping")
    except (ServerSelectionTimeoutError, AutoReconnect, PyMongoError, Exception):
        # try a fresh client
        try:
            _client.close()
        except Exception:
            pass
        _client = connect_mongo(mongo_uri)
        # raise if still bad so caller sees a 500 instead of silent failure
        _client.admin.command("ping")

    return _client[mongo_db][mongo_col]



@app.get("/articles")
def list_articles(limit: int = 50, skip: int = 0):
    if limit < 1 or limit > 200:
        raise HTTPException(status_code=400, detail="limit must be 1..200")
    coll = _mongo_collection()
    cur = (
        coll.find({}, projection={"_id": 0})
            .sort([("published_at", DESCENDING), ("stored_at", DESCENDING)])
            .skip(skip).limit(limit)
    )
    return list(cur)

@app.get("/articles/search")
def search_articles(q: Optional[str] = None, limit: int = 50, skip: int = 0):
    coll = _mongo_collection()
    filt: Dict[str, Any] = {}
    if q:
        filt = {"$or": [
            {"title": {"$regex": q, "$options": "i"}},
            {"summary": {"$regex": q, "$options": "i"}},
            {"tags": {"$regex": q, "$options": "i"}}
        ]}
    cur = (
        coll.find(filt, projection={"_id": 0})
            .sort([("published_at", DESCENDING), ("stored_at", DESCENDING)])
            .skip(skip).limit(limit)
    )
    return list(cur)

@app.get("/healthz")
def healthz():
    return {"ok": True, "time": datetime.utcnow().isoformat() + "Z"}

if __name__ == "__main__":
    uvicorn.run("main:app",
                host=os.getenv("API_HOST", "0.0.0.0"),
                port=int(os.getenv("PORT", "8000")),
                reload=False)
