#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
db_loader.py
Load structured articles into MongoDB with a 36-hour TTL.

- Standalone: reads news_structured.json and upserts.
- As module: call save(items: List[dict]) from main.py.

ENV (.env supported):
  MONGO_URI=mongodb://localhost:27017
  MONGO_DB=newsdb
  MONGO_COLLECTION=news_structured
  USE_TIMESERIES=0
  MONGO_ALLOW_INVALID_CERTS=0   # set 1 only for debugging corporate SSL interception
"""

from __future__ import annotations
import os, json, uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

# .env
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# CA bundle (Atlas)
try:
    import certifi
    TLS_CA_FILE: Optional[str] = certifi.where()
except Exception:
    TLS_CA_FILE = None

from pymongo import MongoClient, ASCENDING
from pymongo.collection import Collection
from pymongo.errors import PyMongoError, CollectionInvalid

INPUT_FILE = "news_structured.json"


# ---------------- TLS-aware connector ----------------
def _connect_client(uri: str) -> MongoClient:
    allow_invalid = os.getenv("MONGO_ALLOW_INVALID_CERTS", "0") == "1"
    kwargs = dict(
        serverSelectionTimeoutMS=30000,
        connectTimeoutMS=30000,
        retryWrites=True,
    )
    is_tls = uri.startswith("mongodb+srv://") or "tls=true" in uri.lower() or "ssl=true" in uri.lower()
    if is_tls:
        if allow_invalid:
            kwargs["tlsAllowInvalidCertificates"] = True
        elif TLS_CA_FILE:
            kwargs["tlsCAFile"] = TLS_CA_FILE
    return MongoClient(uri, **kwargs)


# ---------------- helpers ----------------
def _load_items(path: str) -> List[Dict[str, Any]]:
    if not os.path.exists(path):
        print(f"Input not found: {path}")
        return []
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, dict) and "items" in data:
        data = data["items"]
    if not isinstance(data, list):
        raise ValueError("Input JSON must be a list or dict with 'items'.")
    return [d for d in data if isinstance(d, dict)]

def _ensure_collection(db, name: str) -> Collection:
    if name in db.list_collection_names():
        return db[name]
    use_ts = os.getenv("USE_TIMESERIES", "0") == "1"
    if use_ts:
        try:
            db.create_collection(name, timeseries={"timeField": "stored_at", "granularity": "minutes"})
            print(f"✓ Created time-series collection '{name}' (timeField='stored_at').")
            return db[name]
        except (CollectionInvalid, TypeError, PyMongoError) as e:
            print(f"Time-series create failed ({e}); falling back to normal collection.")
    try:
        db.create_collection(name)
        print(f"✓ Created collection '{name}'.")
    except CollectionInvalid:
        pass
    return db[name]

def _drop_hash_index(coll: Collection) -> None:
    try:
        coll.drop_index("hash_1")
        print("✓ Dropped hash_1 index.")
    except Exception as e:
        print(f"Note: hash_1 index not found or already dropped: {e}")

def _ensure_ttl_index(coll: Collection) -> None:
    try:
        coll.create_index([("expires_at", ASCENDING)], name="expires_at_ttl", expireAfterSeconds=0)
        print("✓ TTL index configured.")
    except Exception as e:
        print(f"TTL index warning: {e}")

def _make_doc(raw: Dict[str, Any]) -> Dict[str, Any]:
    doc = dict(raw)
    # remove fields not needed
    doc.pop("entities", None)
    # _id
    _id = doc.get("id")
    if not isinstance(_id, str) or not _id.strip():
        _id = str(uuid.uuid4())
    doc["_id"] = _id
    # satisfy unique article_id if exists
    doc["article_id"] = doc["_id"]
    # TTL timestamps
    stored_at = datetime.utcnow()
    doc["stored_at"] = stored_at
    doc["expires_at"] = stored_at + timedelta(hours=36)
    return doc

def _upsert_one(coll: Collection, doc: Dict[str, Any]) -> str:
    to_set = dict(doc)
    to_set.pop("_id", None)  # never set _id
    res = coll.update_one({"_id": doc["_id"]}, {"$set": to_set}, upsert=True)
    if res.matched_count == 0 and res.upserted_id is not None:
        return "inserted"
    return "updated"


import time
from pymongo.errors import PyMongoError, ServerSelectionTimeoutError

def save(items):
    if not items:
        print("db_loader.save: no items")
        return 0

    uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    fb_uri = os.getenv("MONGO_FALLBACK_URI", "")  # optional: local fallback
    dbname = os.getenv("MONGO_DB", "newsdb")
    colname = os.getenv("MONGO_COLLECTION", "news_structured")

    def _try_connect(u):
        client = _connect_client(u)
        client.admin.command("ping")
        return client

    client = None
    attempts = 0
    max_attempts = 4
    backoff = 2.0

    while attempts < max_attempts and client is None:
        try:
            attempts += 1
            client = _try_connect(uri)
            print("✓ Connected to MongoDB (primary)")
        except Exception as e:
            print(f"db_loader.save: connect attempt {attempts}/{max_attempts} failed: {e}")
            if attempts < max_attempts:
                time.sleep(backoff)
                backoff *= 2

    # Optional fallback
    if client is None and fb_uri:
        try:
            client = _try_connect(fb_uri)
            print("✓ Connected to MongoDB (fallback)")
        except Exception as e:
            print(f"db_loader.save: fallback connect failed: {e}")

    if client is None:
        print("db_loader.save: MongoDB unavailable; skipping upsert this cycle.")
        return 0

    try:
        db = client[dbname]
        coll = _ensure_collection(db, colname)
        _drop_hash_index(coll)
        _ensure_ttl_index(coll)

        ins = upd = fail = 0
        for raw in items:
            try:
                doc = _make_doc(raw)
                res = _upsert_one(coll, doc)
                if res == "inserted": ins += 1
                else: upd += 1
            except Exception as e:
                fail += 1
                print(f"Error upserting document: {e}")

        print(f"db_loader.save summary: inserted={ins} updated={upd} failed={fail}")
        return ins + upd
    finally:
        try:
            client.close()
        except Exception:
            pass



# --------------- CLI mode ---------------
def main() -> None:
    uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    dbname = os.getenv("MONGO_DB", "newsdb")
    colname = os.getenv("MONGO_COLLECTION", "news_structured")

    items = _load_items(INPUT_FILE)
    if not items:
        print("No items to load.")
        return

    print(f"Connecting to MongoDB: {uri}")
    print(f"Database: {dbname} | Collection: {colname}")
    print(f"Found {len(items)} articles.\n")

    try:
        client = _connect_client(uri)
        client.admin.command("ping")
        print("✓ Connected to MongoDB")
    except Exception as e:
        print(f"MongoDB connection failed: {e}")
        return

    db = client[dbname]
    coll = _ensure_collection(db, colname)
    _drop_hash_index(coll)
    _ensure_ttl_index(coll)

    ins = upd = fail = 0
    for i, raw in enumerate(items, 1):
        try:
            doc = _make_doc(raw)
            res = _upsert_one(coll, doc)
            if res == "inserted":
                ins += 1
            else:
                upd += 1
            if i % 25 == 0 or i == len(items):
                print(f"Processed {i}/{len(items)} | inserted={ins} updated={upd} failed={fail}")
        except PyMongoError as e:
            fail += 1
            print(f"[{i}/{len(items)}] Upsert failed for _id={raw.get('id')} → {e}")
        except Exception as e:
            fail += 1
            print(f"[{i}/{len(items)}] Error preparing/upserting doc → {e}")

    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    print(f"Total: {len(items)} | Inserted: {ins} | Updated: {upd} | Failed: {fail}")
    print("TTL configured: docs auto-delete ~36h after 'stored_at'.")
    client.close()


if __name__ == "__main__":
    main()
