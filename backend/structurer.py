#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Step-3 (structuring only, no DB):
- Read items from 'staging_unique.json'
- Call Gemini to produce STRICT JSON with:
    id, title, summary, sentiment{label,score}, ui_recommendation, impact_analysis,
    category, tickers[], entities[], tags[], published_at, source, original_url, body_excerpt
- Validate & normalize
- Save to:
    - news_structured.json
    - news_structurer_errors.json (failures + reasons)
    - llm_bad_output_*.txt (raw bad payloads for debugging)

Env / .env:
  GEMINI_API_KEY
"""

from __future__ import annotations
import os
import json
import time
import uuid
import re
from typing import Any, Dict, List, Optional
from datetime import datetime

# Optional: load .env
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

from jsonschema import Draft202012Validator

# Gemini client
from google import genai

INPUT_FILE = "staging_unique.json"
OUT_STRUCT = "news_structured.json"
OUT_ERRORS = "news_structurer_errors.json"

# -------- Strict schema we expect from LLM --------
STRUCT_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "required": [
        "id", "title", "summary", "sentiment", "ui_recommendation",
        "impact_analysis", "category", "tickers", "entities", "tags",
        "published_at", "source", "original_url", "body_excerpt"
    ],
    "properties": {
        "id": {"type": "string"},
        "title": {"type": "string", "minLength": 3},
        "summary": {"type": "string", "minLength": 10},
        "sentiment": {
            "type": "object",
            "required": ["label", "score"],
            "properties": {
                "label": {"type": "string", "enum": ["positive", "neutral", "negative"]},
                "score": {"type": "number", "minimum": 0.0, "maximum": 1.0}
            }
        },
        "ui_recommendation": {"type": "string", "minLength": 5},
        "impact_analysis": {"type": "string", "minLength": 5},
        "category": {
            "type": "string",
            "enum": ["Market News", "Company Update", "Earnings", "Regulatory",
                     "Macro", "Product Launch", "Management", "Funding", "Other"]
        },
        "tickers": {"type": "array", "items": {"type": "string"}},
        "entities": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["type", "value"],
                "properties": {
                    "type": {"type": "string"},
                    "value": {"type": "string"}
                }
            }
        },
        "tags": {"type": "array", "items": {"type": "string"}},
        "published_at": {"type": ["string", "null"]},
        "source": {"type": "string"},
        "original_url": {"type": "string"},
        "body_excerpt": {"type": "string"}
    },
    "additionalProperties": True
}

PROMPT_TMPL = """You are a strict JSON generator.
Output ONLY a valid JSON object (no markdown, no comments).

Your job:
Take a financial news article and convert it into a structured object for direct UI display and MongoDB storage.

JSON FORMAT TO RETURN:
{{
  "article_id": "string (REQUIRED - use input id if exists, else generate a unique uuid4)",
  "title": "string",
  "summary": "string (2–4 sentences, clear & factual)",
  "sentiment": {{"label": "positive|neutral|negative", "score": 0..1}},
  "ui_recommendation": "string (1–2 sentences: key takeaway/actionable insight for users)",
  "impact_analysis": "string (why it matters; likely effect on company/sector/price/market)",
  "category": "Market News|Company Update|Earnings|Regulatory|Macro|Product Launch|Management|Funding|Other",
  "tickers": ["RELIANCE.NS", "TCS.NS"], 
  "tags": ["earnings","ipo","rbi","sebi","results","acquisition"],
  "published_at": "ISO datetime string (YYYY-MM-DDTHH:MM:SS format)",
  "source": "string",
  "original_url": "string",
  "body_excerpt": "string (first 200-300 chars of body)"
}}

CRITICAL RULES:
1) Output ONLY JSON (single object). No prose, no markdown, no code blocks, no backticks.
2) "article_id" field is REQUIRED and MUST be unique for each article.
3) If tickers cannot be determined, return [].
4) Summary must be objective; no hype.
5) 'category' must be one of the allowed options exactly.
6) 'sentiment.score' must be consistent with label (positive: 0.6-1.0, neutral: 0.4-0.6, negative: 0.0-0.4).
7) 'published_at' must be in ISO format: "YYYY-MM-DDTHH:MM:SS" (e.g., "2025-11-15T13:30:00").
8) Keep 'ui_recommendation' and 'impact_analysis' concise and useful to investors.
9) NEVER include null values - use empty string "" or empty array [] instead.

INPUT ARTICLE:
---TITLE---
{title}
---BODY---
{body}
---SOURCE---
{source}
---URL---
{url}
---PUBLISHED_AT---
{published_at}

Remember: Output ONLY the JSON object, nothing else.
"""

# --------------------- Helpers ---------------------
def load_items(path: str) -> List[Dict[str, Any]]:
    if not os.path.exists(path):
        print(f"Input not found: {path}")
        return []
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, dict) and "items" in data:
        data = data["items"]
    return [d for d in data if isinstance(d, dict)]

def iso_parseable(dt: Optional[str]) -> bool:
    if not dt or not isinstance(dt, str):
        return False
    try:
        _ = datetime.fromisoformat(dt.replace("Z", "+00:00")) if ("+" in dt or dt.endswith("Z")) else datetime.fromisoformat(dt)
        return True
    except Exception:
        return False

def coerce_and_validate(obj: Dict[str, Any]) -> Dict[str, Any]:
    # id
    if not obj.get("id"):
        obj["id"] = str(uuid.uuid4())

    # published_at: keep ISO or set null
    pa = obj.get("published_at")
    if not iso_parseable(pa):
        obj["published_at"] = None

    # sentiment label/score bounds
    sent = obj.get("sentiment") or {}
    label = (sent.get("label") or "neutral").lower()
    if label not in {"positive", "neutral", "negative"}:
        label = "neutral"
    score = sent.get("score")
    try:
        score = float(score)
    except Exception:
        score = 0.5
    score = min(max(score, 0.0), 1.0)
    obj["sentiment"] = {"label": label, "score": score}

    # arrays
    obj["tickers"] = obj.get("tickers") or []
    obj["entities"] = obj.get("entities") or []
    obj["tags"] = obj.get("tags") or []

    # strings
    for k in ["title", "summary", "ui_recommendation", "impact_analysis",
              "category", "source", "original_url", "body_excerpt"]:
        if obj.get(k) is None:
            obj[k] = ""

    # validate final
    Draft202012Validator(STRUCT_SCHEMA).validate(obj)
    return obj

# ---------- Robust JSON parsing ----------
def _extract_json_braces(s: str) -> Optional[str]:
    if not s:
        return None
    start = s.find("{")
    if start == -1:
        return None
    depth = 0
    in_string = False
    esc = False
    for i in range(start, len(s)):
        ch = s[i]
        if in_string:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_string = False
            continue
        else:
            if ch == '"':
                in_string = True
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    return s[start:i+1]
    return None

def _strip_code_fences(s: str) -> str:
    s = s.strip()
    if s.startswith("```"):
        s = s.strip("`").strip()
    if s.lower().startswith("json"):
        s = s[4:].lstrip()
    return s

def json_from_text(raw: str) -> Optional[Dict[str, Any]]:
    if not raw:
        return None
    s = _strip_code_fences(raw)

    # 1) strict parse
    try:
        return json.loads(s)
    except Exception:
        pass

    # 2) extract first balanced {...}
    obj_str = _extract_json_braces(s)
    if obj_str:
        try:
            return json.loads(obj_str)
        except Exception:
            # 3) trailing-comma cleanup
            cleaned = re.sub(r",\s*([}\]])", r"\1", obj_str)
            try:
                return json.loads(cleaned)
            except Exception:
                # log raw for debugging
                try:
                    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S%f")
                    with open(f"llm_bad_output_{ts}.txt", "w", encoding="utf-8") as f:
                        f.write(raw)
                except Exception:
                    pass
                return None

    # No braces found → log raw
    try:
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S%f")
        with open(f"llm_bad_output_{ts}.txt", "w", encoding="utf-8") as f:
            f.write(raw)
    except Exception:
        pass
    return None

# ---------- Gemini ----------
def call_gemini(client: genai.Client, title: str, body: str, source: str,
                url: str, published_at: Optional[str], temperature: float = 0.0) -> str:
    max_body = (body or "")[:4000]
    prompt = PROMPT_TMPL.format(
        title=title or "",
        body=max_body,
        source=source or "",
        url=url or "",
        published_at=published_at if published_at else "null"
    )
    # Ensure JSON-only output; catch SDK errors so the loop continues
    try:
        resp = client.models.generate_content(
            model="gemini-2.0-flash",
            contents=prompt,
            config={
                "temperature": temperature,
                "response_mime_type": "application/json"
            }
        )
        return resp.text or ""
    except Exception as e:
        # Return a recognizable non-JSON marker so parser fails gracefully
        return f"__GENAI_EXCEPTION__:{e}"

# --------------------- Main ---------------------
def main() -> None:
    gem_key = os.getenv("GEMINI_API_KEY")
    if not gem_key:
        raise RuntimeError("GEMINI_API_KEY not set (env or .env).")
    client = genai.Client(api_key=gem_key)

    items = load_items(INPUT_FILE)
    if not items:
        print("No input items to structure.")
        return
    print(f"Loaded {len(items)} items from {INPUT_FILE}")

    structured: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []

    for i, it in enumerate(items, 1):
        title = it.get("title") or ""
        body = it.get("body") or ""
        src = it.get("source") or ""
        url = it.get("url") or ""
        pub = it.get("published_at")  # may be None

        attempt = 0
        success = False
        last_err = ""
        while attempt < 2 and not success:
            attempt += 1
            try:
                raw = call_gemini(client, title, body, src, url, pub, temperature=0.0)

                # If the SDK raised and we encoded it into the text:
                if raw.startswith("__GENAI_EXCEPTION__"):
                    raise RuntimeError(raw.replace("__GENAI_EXCEPTION__:", ""))

                obj = json_from_text(raw)
                if obj is None:
                    raise ValueError("LLM did not return valid JSON.")

                # inject missing basics from input
                obj.setdefault("id", it.get("id") or str(uuid.uuid4()))
                obj.setdefault("source", src)
                obj.setdefault("original_url", url)
                obj.setdefault("body_excerpt", (body[:300] if body else ""))
                obj["fetched_at"] = it.get("fetched_at", datetime.utcnow().isoformat())

                obj = coerce_and_validate(obj)

                structured.append(obj)
                print(f"[{i}/{len(items)}] OK: {obj['title'][:90]}")
                success = True

            except Exception as e:
                last_err = str(e)
                if attempt >= 2:
                    errors.append({
                        "id": it.get("id"),
                        "title": title,
                        "url": url,
                        "error": last_err
                    })
                    print(f"[{i}/{len(items)}] FAIL: {title[:90]} | {last_err}")
                else:
                    time.sleep(0.6)  # brief retry backoff

        time.sleep(0.2)  # polite pacing

    # write artifacts
    with open(OUT_STRUCT, "w", encoding="utf-8") as f:
        json.dump(structured, f, indent=2, ensure_ascii=False, default=str)
    print(f"Saved structured: {OUT_STRUCT} ({len(structured)} items)")

    if errors:
        with open(OUT_ERRORS, "w", encoding="utf-8") as f:
            json.dump(errors, f, indent=2, ensure_ascii=False)
        print(f"Saved errors: {OUT_ERRORS} ({len(errors)} items)")
    else:
        if os.path.exists(OUT_ERRORS):
            try:
                os.remove(OUT_ERRORS)
            except Exception:
                pass

    print("Done.")

# ---------- Adapter for pipeline integration (callable by main.py) ----------
_CLIENT = None

def _get_client():
    """Cache Gemini client across calls."""
    global _CLIENT
    if _CLIENT is None:
        gem_key = os.getenv("GEMINI_API_KEY")
        if not gem_key:
            raise RuntimeError("GEMINI_API_KEY not set (env or .env).")
        _CLIENT = genai.Client(api_key=gem_key)
    return _CLIENT

def _structure_one(client, it: Dict[str, Any], i: int, total: int) -> Dict[str, Any]:
    title = it.get("title") or ""
    body = it.get("body") or ""
    src  = it.get("source") or ""
    url  = it.get("url") or ""
    pub  = it.get("published_at")  # may be None

    # 1–2 attempts, like your CLI main()
    last_err = ""
    for attempt in range(2):
        try:
            raw = call_gemini(client, title, body, src, url, pub, temperature=0.0)
            if raw.startswith("__GENAI_EXCEPTION__"):
                raise RuntimeError(raw.replace("__GENAI_EXCEPTION__:", ""))

            obj = json_from_text(raw)
            if obj is None:
                raise ValueError("LLM did not return valid JSON.")

            # --- normalize/mapping ---
            # Some prompts return 'article_id' (your template); schema wants 'id'
            if obj.get("article_id") and not obj.get("id"):
                obj["id"] = obj.pop("article_id")

            # inject basics from input if missing
            obj.setdefault("id", it.get("id") or str(uuid.uuid4()))
            obj.setdefault("source", src)
            obj.setdefault("original_url", url)
            obj.setdefault("body_excerpt", (body[:300] if body else ""))

            # helpful metadata
            obj["fetched_at"] = it.get("fetched_at", datetime.utcnow().isoformat())

            # validate/coerce to your STRUCT_SCHEMA
            obj = coerce_and_validate(obj)

            print(f"[{i}/{total}] OK: {obj.get('title','')[:90]}")
            return obj

        except Exception as e:
            last_err = str(e)
            if attempt == 0:
                time.sleep(0.6)  # brief retry
            else:
                raise RuntimeError(last_err)

    # Should never reach here
    raise RuntimeError(last_err or "Unknown structuring error")

def structure(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Pipeline entrypoint.
    - Takes raw/filtered items list
    - Calls Gemini per item
    - Validates/normalizes to STRUCT_SCHEMA
    - Writes OUT_STRUCT/OUT_ERRORS for observability
    Returns List[Dict] of structured items.
    """
    if not items:
        # keep files in sync
        with open(OUT_STRUCT, "w", encoding="utf-8") as f:
            json.dump([], f, indent=2, ensure_ascii=False)
        if os.path.exists(OUT_ERRORS):
            try: os.remove(OUT_ERRORS)
            except Exception: pass
        print("structure(): no input items")
        return []

    client = _get_client()
    structured: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []

    total = len(items)
    for i, it in enumerate(items, 1):
        try:
            obj = _structure_one(client, it, i, total)
            structured.append(obj)
        except Exception as e:
            errors.append({
                "id": it.get("id"),
                "title": it.get("title"),
                "url": it.get("url"),
                "error": str(e)
            })
        time.sleep(0.2)

    # Write artifacts like your CLI
    with open(OUT_STRUCT, "w", encoding="utf-8") as f:
        json.dump(structured, f, indent=2, ensure_ascii=False, default=str)
    print(f"Saved structured: {OUT_STRUCT} ({len(structured)} items)")

    if errors:
        with open(OUT_ERRORS, "w", encoding="utf-8") as f:
            json.dump(errors, f, indent=2, ensure_ascii=False)
        print(f"Saved errors: {OUT_ERRORS} ({len(errors)} items)")
    else:
        if os.path.exists(OUT_ERRORS):
            try: os.remove(OUT_ERRORS)
            except Exception: pass

    return structured

if __name__ == "__main__":
    main()
