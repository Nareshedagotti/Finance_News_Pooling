#!/usr/bin/env python3
# Step-2: Title filter (negative with impact exceptions) → preprocess → embeddings → dedupe

import os, json, re, unicodedata
from datetime import datetime
from typing import List, Dict, Tuple
import numpy as np
from sentence_transformers import SentenceTransformer

# ------------ CONFIG ------------
INPUT_JSON              = "staging_raw.json"
OUTPUT_FILTERED_JSON    = "staging_filtered.json"
OUTPUT_FILTERED_DROPPED = "staging_filtered_dropped.json"
OUTPUT_UNIQUE_JSON      = "staging_unique.json"
OUTPUT_DUPES_JSON       = "staging_duplicates.json"

MODEL_NAME              = "sentence-transformers/all-MiniLM-L6-v2"
SIM_THRESHOLD           = 0.70
BATCH_SIZE              = 32
MAX_BODY_CHARS          = 3000
MIN_TITLE_LEN           = 8
# ---------------------------------

# A) NEGATIVE (general-news) TITLE keywords → drop, unless impact exception triggers
NEG_TITLE_KEYWORDS = {
    # entertainment / celebrity
    "movie","film","web series","trailer","box office","actor","actress",
    "bollywood","tollywood","kollywood","entertainment","celebrity","music","album",
    # sports
    "cricket","ipl","odi","t20","test match","football","fifa","tennis","badminton",
    "world cup","asia cup","olympics","scorecard","match preview","match report",
    # lifestyle / culture
    "recipe","travel","tourism","festival","fashion","beauty","lifestyle","diet",
    # generic how-to/viral
    "how to","tips and tricks","what is","explained","viral",
    # pure politics (will be overridden if impact signals found)
    "election","elections","minister","cabinet","parliament","assembly","campaign","rally",
    "politics","political","chief minister","prime minister","pm","cm","mla","mp"
}

# B) IMPACT EXCEPTIONS — if the title has any of these signals, we KEEP even if negative matched
# These indicate stock/market/company/finance relevance directly from the TITLE
IMPACT_KEYWORDS = {
    # markets/indices
    "stock","stocks","share","shares","market","markets","equity","equities",
    "nifty","sensex","bank nifty","nse","bse","index","indices","intraday",
    # corporate actions & events
    "ipo","fpo","buyback","dividend","split","bonus","rights issue","delisting",
    "merger","acquisition","stake","pledge","de-pledge","board meeting",
    # earnings/results/guidance
    "earnings","results","q1","q2","q3","q4","fy","profit","loss","revenue","ebitda",
    "margin","guidance","order book",
    # policy/regulatory/macro affecting markets
    "rbi","sebi","gst","customs duty","import duty","export duty","tariff","fta",
    "repo rate","inflation","gdp","iip","cci","crr","slr",
    # brokerages/targets/ratings
    "brokerage","target price","price target","rating","upgrade","downgrade",
    "overweight","underweight","buy","sell","accumulate","hold","neutral",
    # financing/capex/contracts
    "capex","debt","bond","ncd","maturity","refinance","order win","contract","mou",
    "approval","licence","license","patent",
    # product/launch with market angle (often price-moving)
    "launch","unveil","roll out","shipments","pre-orders","bookings"
}

# REGEX patterns that also indicate impact (keep)
IMPACT_PATTERNS = [
    r"\b[A-Z]{2,12}\.(NS|BO)\b",      # tickers like RELIANCE.NS
    r"\b(NSE|BSE|Nifty|Sensex)\b",    # exchanges & indices
    r"\b(up|down|rises?|falls?|surges?|slumps?|spikes?|tumbles?)\b",  # movement verbs
    r"\b\d+(\.\d+)?\s?%\b",           # percentage changes
    r"(₹|rs\.?|inr)\s?\d+(\.\d+)?\s?(crore|cr|lakh|mn|bn)?",  # price/amounts
    r"\b(52[- ]week|all[- ]time)\s?(high|low)\b",
    r"\b(ipo|fpo|qib|qip|of s|ofs)\b",  # capital market events
]

SKIP_PHRASES_BODY = [
    "also read","read more","subscribe","advertisement","follow us",
    "sign up","login","unlock","premium","download the app",
    "Add as a Reliable and Trusted News Source",
]

def lower(s: str) -> str: return (s or "").lower()

def title_has_impact(title: str) -> bool:
    t = lower(title)
    if any(k in t for k in IMPACT_KEYWORDS):
        return True
    for pat in IMPACT_PATTERNS:
        if re.search(pat, title, flags=re.I):
            return True
    return False

def title_should_keep(title: str) -> Tuple[bool, str]:
    """
    Negative-first:
      - If title has any NEG keyword:
          - keep if impact signals present (exception)
          - else drop
      - Else (no NEG matched): keep
    """
    t = lower(title)
    neg_hit = next((neg for neg in NEG_TITLE_KEYWORDS if neg in t), None)
    if neg_hit:
        if title_has_impact(title):
            return True, f"impact_exception({neg_hit})"
        return False, f"negative_keyword({neg_hit})"
    # no negative matched → keep
    return True, "no_negative_match"

def clean_text(text: str) -> str:
    if not text: return ""
    text = unicodedata.normalize("NFKC", text)
    text = re.sub(r"\s+", " ", text).strip()
    for s in SKIP_PHRASES_BODY:
        text = re.sub(re.escape(s), " ", text, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", text).strip()

def build_embed_text(item: Dict) -> str:
    title = clean_text(item.get("title", ""))
    body  = clean_text((item.get("body") or "")[:MAX_BODY_CHARS])
    return f"{title}. {body}" if body else title

def parse_dt(iso_or_none: str, fallback: str) -> datetime:
    for c in (iso_or_none, fallback):
        if not c: continue
        try:
            if ("+" in c) or c.endswith("Z"): return datetime.fromisoformat(c.replace("Z","+00:00"))
            return datetime.fromisoformat(c)
        except Exception: continue
    return datetime.utcnow()

def embed_batch(model: SentenceTransformer, texts: List[str]) -> np.ndarray:
    return np.array(model.encode(texts, show_progress_bar=False, batch_size=BATCH_SIZE, normalize_embeddings=True))

def greedy_dedupe(items: List[Dict], embs: np.ndarray, thr: float):
    """
    Stable, Pythonic ordering to avoid NumPy->list indexing errors.
    """
    pub_fetch_pairs = []
    for it in items:
        it["_dt_pub"] = parse_dt(it.get("published_at"), it.get("fetched_at"))
        it["_dt_fetch"] = parse_dt(it.get("fetched_at"), it.get("fetched_at"))
        pub_fetch_pairs.append((it["_dt_pub"], it["_dt_fetch"]))

    idxs = sorted(range(len(items)), key=lambda k: (pub_fetch_pairs[k][0], pub_fetch_pairs[k][1]))
    items_s = [items[k] for k in idxs]
    E       = embs[idxs, :]  # NumPy array indexing on NumPy array is fine

    kept, keptE, dupes = [], [], []
    for it, e in zip(items_s, E):
        if not keptE:
            kept.append(it); keptE.append(e); continue
        sims = np.vstack(keptE) @ e  # normalized → dot = cosine
        j = int(np.argmax(sims)); mx = float(np.max(sims))
        if mx >= thr:
            dupes.append({
                "id": it.get("id"), "title": it.get("title"), "url": it.get("url"),
                "source": it.get("source"), "published_at": it.get("published_at"),
                "duplicate_of": kept[j].get("id"), "duplicate_of_title": kept[j].get("title"),
                "cosine_similarity": round(mx, 4)
            })
        else:
            kept.append(it); keptE.append(e)

    for it in kept:
        it.pop("_dt_pub", None); it.pop("_dt_fetch", None)
    return kept, dupes

def load_items(path: str) -> List[Dict]:
    if not os.path.exists(path):
        print(f"Input not found: {path}"); return []
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, dict) and "items" in data: data = data["items"]
    return [d for d in data if isinstance(d, dict)]

def save_json(path: str, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False, default=str)
    print(f"Saved {path}")

def main():
    raw = load_items(INPUT_JSON)
    if not raw:
        print("No input items; nothing to do."); return

    # A) Title filter: NEG → drop unless impact exception; otherwise keep
    kept_filter, dropped = [], []
    for it in raw:
        title = (it.get("title") or "").strip()
        url   = (it.get("url")   or "").strip()
        if len(title) < MIN_TITLE_LEN or not url:
            dropped.append({**it, "_drop_reason": "missing_title_or_url"}); continue
        ok, reason = title_should_keep(title)
        if ok: kept_filter.append(it)
        else:  dropped.append({**it, "_drop_reason": reason})
    print(f"Filter (negative with impact exceptions): input={len(raw)} | kept={len(kept_filter)} | dropped={len(dropped)}")
    save_json(OUTPUT_FILTERED_JSON, kept_filter)
    save_json(OUTPUT_FILTERED_DROPPED, dropped)
    if not kept_filter:
        print("Nothing relevant after title filter."); return

    # B) Preprocess + texts
    texts = [build_embed_text(it) for it in kept_filter]

    # C) Embeddings
    print(f"Loading model: {MODEL_NAME}")
    model = SentenceTransformer(MODEL_NAME)
    print(f"Encoding {len(texts)} items (batch={BATCH_SIZE}) …")
    embs = embed_batch(model, texts)

    # D) Dedupe
    kept, dupes = greedy_dedupe(kept_filter, embs, SIM_THRESHOLD)
    print(f"Deduped: kept={len(kept)} | duplicates={len(dupes)} (threshold={SIM_THRESHOLD})")

    # E) Save
    save_json(OUTPUT_UNIQUE_JSON, kept)
    save_json(OUTPUT_DUPES_JSON, dupes)

# --------- Adapter for pipeline integration (callable by main.py) ---------
# Caches the embedding model across calls to avoid reloading on each run.
_MODEL = None
def _get_model():
    global _MODEL
    if _MODEL is None:
        print(f"Loading model: {MODEL_NAME}")
        _MODEL = SentenceTransformer(MODEL_NAME)
    return _MODEL

def clean_and_dedupe(items: List[Dict]) -> List[Dict]:
    """
    Pipeline entrypoint.
    - Applies title filter with impact exceptions
    - Builds embeddings
    - Greedy similarity-based dedupe
    - Writes the same staging files this module already uses (optional but helpful)
    Returns the unique, relevant items (List[Dict]).
    """
    if not items:
        print("clean_and_dedupe: no input items")
        # still write empties for observability
        save_json(OUTPUT_FILTERED_JSON, [])
        save_json(OUTPUT_FILTERED_DROPPED, [])
        save_json(OUTPUT_UNIQUE_JSON, [])
        save_json(OUTPUT_DUPES_JSON, [])
        return []

    # A) Title filter
    kept_filter, dropped = [], []
    for it in items:
        title = (it.get("title") or "").strip()
        url   = (it.get("url")   or "").strip()
        if len(title) < MIN_TITLE_LEN or not url:
            dropped.append({**it, "_drop_reason": "missing_title_or_url"}); continue
        ok, reason = title_should_keep(title)
        if ok: kept_filter.append(it)
        else:  dropped.append({**it, "_drop_reason": reason})

    print(f"[clean_and_dedupe] Filter: input={len(items)} | kept={len(kept_filter)} | dropped={len(dropped)}")
    save_json(OUTPUT_FILTERED_JSON, kept_filter)
    save_json(OUTPUT_FILTERED_DROPPED, dropped)

    if not kept_filter:
        save_json(OUTPUT_UNIQUE_JSON, [])
        save_json(OUTPUT_DUPES_JSON, [])
        return []

    # B) Texts + embeddings (cached model)
    texts = [build_embed_text(it) for it in kept_filter]
    model = _get_model()
    print(f"[clean_and_dedupe] Encoding {len(texts)} items (batch={BATCH_SIZE}) …")
    embs = embed_batch(model, texts)

    # C) Dedupe
    kept, dupes = greedy_dedupe(kept_filter, embs, SIM_THRESHOLD)
    print(f"[clean_and_dedupe] Deduped: kept={len(kept)} | duplicates={len(dupes)} (thr={SIM_THRESHOLD})")

    # D) Save observability files (same names you already use)
    save_json(OUTPUT_UNIQUE_JSON, kept)
    save_json(OUTPUT_DUPES_JSON, dupes)

    return kept

if __name__ == "__main__":
    main()
