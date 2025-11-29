#!/usr/bin/env python3
"""
news_fetcher.py

- Continuously runs every 2 minutes.
- Sources: LiveMint, Economic Times, The Hindu (Business).
- Follows article links and extracts full cleaned text.
- Persists new raw articles into 'staging_raw.json'.
- Persists seen hashes into 'seen_hashes.json' to avoid re-fetching across restarts.
- Persists per-source last_fetch_time into 'source_state.json' for observability.
- To stop: Ctrl+C
"""

import requests
from bs4 import BeautifulSoup
import time
from datetime import datetime
import json
import hashlib
import re
import os
import sys
from typing import List, Dict, Optional
from datetime import timedelta

# ---------- Config ----------
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
}
STAGING_FILE = "staging_raw.json"
SEEN_FILE = "seen_hashes.json"
SOURCE_STATE_FILE = "source_state.json"
FETCH_INTERVAL_MIN = 2
POLITE_DELAY_BETWEEN_ARTICLES = 0.6  # seconds
# ---------- End Config ----------


class NewsFetcher:
    def __init__(self, headers=None):
        self.headers = headers or HEADERS
        self.seen = self._load_seen()
        self.source_state = self._load_source_state()

    # ---------- Persistence ----------
    def _load_seen(self) -> set:
        if os.path.exists(SEEN_FILE):
            try:
                with open(SEEN_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    print(f"Loaded {len(data)} seen hashes from {SEEN_FILE}")
                    return set(data)
            except Exception as e:
                print(f"Warning: could not load seen hashes: {e}")
                return set()
        return set()

    def _save_seen(self):
        try:
            with open(SEEN_FILE, 'w', encoding='utf-8') as f:
                json.dump(list(self.seen), f, indent=2)
        except Exception as e:
            print(f"Error saving seen hashes: {e}")

    def _load_source_state(self) -> Dict:
        if os.path.exists(SOURCE_STATE_FILE):
            try:
                with open(SOURCE_STATE_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    print(f"Loaded source state from {SOURCE_STATE_FILE}")
                    return data
            except Exception as e:
                print(f"Warning: could not load source state: {e}")
                return {}
        return {}

    def _save_source_state(self):
        try:
            with open(SOURCE_STATE_FILE, 'w', encoding='utf-8') as f:
                json.dump(self.source_state, f, indent=2, default=str)
        except Exception as e:
            print(f"Error saving source state: {e}")

    def _append_staging(self, items: List[Dict]):
        if not items:
            return
        existing = []
        if os.path.exists(STAGING_FILE):
            try:
                with open(STAGING_FILE, 'r', encoding='utf-8') as f:
                    existing = json.load(f)
            except Exception:
                existing = []
        existing.extend(items)
        try:
            with open(STAGING_FILE, 'w', encoding='utf-8') as f:
                json.dump(existing, f, indent=2, ensure_ascii=False)
            print(f"Saved {len(items)} new raw articles to {STAGING_FILE}")
        except Exception as e:
            print(f"Error writing staging file: {e}")

    # ---------- Utilities ----------
    def _hash(self, title: str, url: str) -> str:
        return hashlib.md5(f"{title}{url}".encode()).hexdigest()

    def _safe_get(self, url: str, timeout: int = 20) -> Optional[requests.Response]:
        try:
            r = requests.get(url, headers=self.headers, timeout=timeout)
            r.raise_for_status()
            return r
        except Exception as e:
            print(f"  ✗ HTTP error for {url}: {e}")
            return None

    def _try_parse_datetime(self, text: str) -> Optional[datetime]:
        if not text:
            return None
        text = text.strip()
        # ISO
        iso = re.search(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})', text)
        if iso:
            try:
                return datetime.fromisoformat(iso.group(1))
            except Exception:
                pass
        ymd = re.search(r'(\d{4}-\d{2}-\d{2})', text)
        if ymd:
            try:
                return datetime.strptime(ymd.group(1), "%Y-%m-%d")
            except Exception:
                pass
        # dd MMM YYYY
        m = re.search(r'(\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4})', text)
        if m:
            for fmt in ("%d %b %Y", "%d %B %Y"):
                try:
                    return datetime.strptime(m.group(1), fmt)
                except Exception:
                    pass
        # "2 hours ago"
        ago = re.search(r'(\d+)\s*(minute|min|hour|hr|day|days|hours)\s*ago', text, re.I)
        if ago:
            qty = int(ago.group(1))
            unit = ago.group(2).lower()
            if 'min' in unit:
                return datetime.now() - timedelta(minutes=qty)
            if 'hour' in unit or 'hr' in unit:
                return datetime.now() - timedelta(hours=qty)
            if 'day' in unit:
                return datetime.now() - timedelta(days=qty)
        return None

    def _extract_published_from_soup(self, soup: BeautifulSoup) -> Optional[datetime]:
        # article:published_time meta
        meta = soup.find('meta', {'property': 'article:published_time'})
        if meta and meta.get('content'):
            dt = self._try_parse_datetime(meta['content'])
            if dt:
                return dt
        # og:pubdate or og:updated_time
        for prop in ('og:published_time', 'og:updated_time'):
            m = soup.find('meta', {'property': prop})
            if m and m.get('content'):
                dt = self._try_parse_datetime(m['content'])
                if dt:
                    return dt
        # time tag
        time_tag = soup.find('time')
        if time_tag:
            if time_tag.get('datetime'):
                dt = self._try_parse_datetime(time_tag['datetime'])
                if dt:
                    return dt
            text = time_tag.get_text(" ", strip=True)
            dt = self._try_parse_datetime(text)
            if dt:
                return dt
        # meta date names
        for name in ('pubdate', 'publishdate', 'date', 'article_date_original'):
            meta = soup.find('meta', {'name': name})
            if meta and meta.get('content'):
                dt = self._try_parse_datetime(meta['content'])
                if dt:
                    return dt
        # visible date spans
        nodes = soup.find_all(['span', 'p', 'div'], class_=re.compile('date|time|timestamp', re.I))
        for n in nodes:
            txt = n.get_text(" ", strip=True)
            dt = self._try_parse_datetime(txt)
            if dt:
                return dt
        return None

    # ---------- Site-specific content extraction ----------
    def _get_full_livemint(self, url: str) -> str:
        resp = self._safe_get(url)
        if not resp:
            return "Error fetching content"
        soup = BeautifulSoup(resp.content, 'html.parser')
        for tag in soup(["script", "style", "aside", "nav", "footer", "header"]):
            tag.decompose()
        paragraphs = []
        selectors = [
            ('div', {'class': 'FirstEle'}),
            ('div', {'class': 'contentSec'}),
            ('div', {'class': 'paywall'}),
            ('article', {}),
            ('div', {'id': 'articlebody'}),
        ]
        for tag, attrs in selectors:
            ele = soup.find(tag, attrs=attrs if attrs else None)
            if ele:
                for p in ele.find_all('p'):
                    text = p.get_text(strip=True)
                    if text and len(text) > 30:
                        paragraphs.append(text)
            if paragraphs:
                break
        if not paragraphs:
            content_divs = soup.find_all('div', class_=re.compile('article|content|story|body', re.I))
            for div in content_divs:
                for p in div.find_all('p', recursive=False):
                    text = p.get_text(strip=True)
                    if text and len(text) > 30:
                        paragraphs.append(text)
        skip = ['also read', 'subscribe', 'login', 'sign up', 'unlock', 'premium', 'read more']
        filtered = [t for t in paragraphs if not any(s.lower() in t.lower() for s in skip)]
        if filtered:
            return ' '.join(filtered)
        body = soup.find('body')
        if body:
            txt = re.sub(r'\s+', ' ', body.get_text(" ", strip=True))
            if len(txt) > 400:
                return txt[:5000]
        return "Content not available"

    def _get_full_et(self, url: str) -> str:
        import json, re
        def _collect_paras(ele):
            paras = []
            for p in ele.find_all(['p','div'], recursive=True):
                t = p.get_text(" ", strip=True)
                if t and len(t) > 40:
                    paras.append(t)
            return paras

        def _clean_join(paras):
            skip = ['also read','read more','subscribe','advertisement','follow us',
                    'Add as a Reliable and Trusted News Source']
            out = [p for p in paras if not any(s.lower() in p.lower() for s in skip)]
            text = ' '.join(out).strip()
            return text[:8000] if len(text) > 8000 else text

        def _try_main(resp_html):
            soup = BeautifulSoup(resp_html, 'html.parser')
            for tag in soup(["script","style","aside","nav","footer","header","iframe","noscript"]):
                tag.decompose()
            # 1) known ET containers
            for tag, attrs in [
                ('div', {'class':'artText'}),
                ('div', {'class':'artSyn'}),
                ('div', {'class':'Normal0'}),
                ('div', {'itemprop':'articleBody'}),
                ('div', {'id': re.compile('article|main|content', re.I)}),
                ('article', {})
            ]:
                ele = soup.find(tag, attrs=attrs if attrs else None)
                if ele:
                    paras = _collect_paras(ele)
                    if paras:
                        return _clean_join(paras)
            # 2) JSON-LD articleBody / description
            for s in soup.find_all('script', type='application/ld+json'):
                try:
                    data = json.loads(s.string or "{}")
                    objs = data if isinstance(data, list) else [data]
                    for o in objs:
                        if isinstance(o, dict) and o.get('@type') in ('NewsArticle','Article','Report'):
                            body = o.get('articleBody') or o.get('description')
                            if isinstance(body, str) and len(body) > 160:
                                # split into paragraphs on newlines if present
                                paras = [x.strip() for x in body.split('\n') if len(x.strip())>40]
                                return _clean_join(paras or [body])
                except Exception:
                    continue
            return ""

        # ---- try primary page
        r = self._safe_get(url)
        if r:
            txt = _try_main(r.text)
            if txt:
                return txt

        # ---- try AMP / print variants (ET usually supports ?amp or trailing /amp)
        amp_candidates = []
        if url.endswith('.cms'):
            amp_candidates.append(url + '?amp')
            amp_candidates.append(url.replace('.cms', '.cms?from=mdr'))
        amp_candidates.append(url.rstrip('/') + '/amp')
        amp_candidates.append(url + '?view=print')

        for au in amp_candidates:
            r2 = self._safe_get(au)
            if not r2:
                continue
            txt2 = _try_main(r2.text)
            if txt2:
                return txt2

        # ---- last resort: long chunks from body
        if r:
            soup = BeautifulSoup(r.text, 'html.parser')
            body = soup.find('body')
            if body:
                chunks = [t.strip() for t in body.get_text("¶", strip=True).split('¶') if len(t.strip())>120]
                if chunks:
                    return _clean_join(chunks[:40])
        return "Content not available - likely paywalled or JS-rendered"



    def _get_full_thehindu(self, url: str) -> str:
        """
        The Hindu - business pages often use <div class="article"> or <div class="story-card">
        Fallbacks used for robustness.
        """
        resp = self._safe_get(url)
        if not resp:
            return "Error fetching content"
        soup = BeautifulSoup(resp.content, 'html.parser')
        # Remove noisy elements
        for tag in soup(["script", "style", "aside", "nav", "footer", "header", "figure", "noscript"]):
            tag.decompose()

        paragraphs = []
        # Try common containers
        candidates = [
            ('div', {'class': re.compile('article|main|story|content|section|art', re.I)}),
            ('article', {}),
        ]
        for tag, attrs in candidates:
            ele = soup.find(tag, attrs=attrs if attrs else None)
            if ele:
                # collect paragraphs in order
                for p in ele.find_all('p', recursive=True):
                    text = p.get_text(" ", strip=True)
                    if text and len(text) > 30:
                        paragraphs.append(text)
            if paragraphs:
                break

        # If still empty, look for divs with typical story classes
        if not paragraphs:
            divs = soup.find_all('div', class_=re.compile('story|article|content|col', re.I))
            for div in divs:
                for p in div.find_all('p', recursive=False):
                    text = p.get_text(" ", strip=True)
                    if text and len(text) > 30:
                        paragraphs.append(text)
                if paragraphs:
                    break

        # Filter common boilerplate phrases
        skip = ['also read', 'subscribe', 'send us', 'sign up', 'download', 'follow us']
        filtered = [t for t in paragraphs if not any(s.lower() in t.lower() for s in skip)]

        if filtered:
            return ' '.join(filtered)
        # Last resort: whole body trimmed
        body = soup.find('body')
        if body:
            txt = re.sub(r'\s+', ' ', body.get_text(" ", strip=True))
            if len(txt) > 400:
                return txt[:6000]
        return "Content not available"

    # ---------- Listing fetchers ----------
    def fetch_livemint(self) -> List[Dict]:
        url = "https://www.livemint.com/latest-news"
        print(f"\n→ Fetching LiveMint listing: {url}")
        resp = self._safe_get(url)
        if not resp:
            return []
        soup = BeautifulSoup(resp.content, 'html.parser')
        # try multiple listing selectors
        found = []
        for sel in [
            ('div', {'class': 'listingNew'}),
            ('div', {'class': 'listing'}),
            ('article', {}),
            ('div', {'class': re.compile('story|card|item', re.I)}),
        ]:
            tag, attrs = sel
            nodes = soup.find_all(tag, attrs=attrs if attrs else None)
            if nodes:
                found = nodes
                break
        print(f"  Found {len(found)} listing nodes on LiveMint page")
        results = []
        for idx, node in enumerate(found, 1):
            try:
                title_tag = node.find(['h1', 'h2', 'h3', 'h4']) or node.find('a')
                if not title_tag:
                    continue
                title = title_tag.get_text(strip=True)
                link_tag = node.find('a', href=True)
                if not link_tag:
                    continue
                link = link_tag['href']
                if not link.startswith('http'):
                    link = f"https://www.livemint.com{link}"
                h = self._hash(title, link)
                if h in self.seen:
                    continue
                print(f"  [{idx}] {title[:120]}...")
                # fetch article page & full content
                full = self._get_full_livemint(link)
                # try published datetime from article page
                pub = None
                art_resp = self._safe_get(link)
                if art_resp:
                    art_soup = BeautifulSoup(art_resp.content, 'html.parser')
                    pub_dt = self._extract_published_from_soup(art_soup)
                    if pub_dt:
                        pub = pub_dt.isoformat()
                obj = {
                    "id": h,
                    "source": "LiveMint",
                    "title": title,
                    "url": link,
                    "published_at": pub,
                    "fetched_at": datetime.now().isoformat(),
                    "body": full
                }
                results.append(obj)
                self.seen.add(h)
                # update source state
                self.source_state.setdefault('livemint', {})['last_fetch_time'] = datetime.now().isoformat()
                time.sleep(POLITE_DELAY_BETWEEN_ARTICLES)
            except Exception as e:
                print(f"  ✗ Error processing LiveMint node #{idx}: {e}")
                continue
        return results

    def fetch_economictimes(self) -> List[Dict]:
        url = "https://economictimes.indiatimes.com/markets/stocks/news"
        print(f"\n→ Fetching Economic Times listing: {url}")
        resp = self._safe_get(url)
        if not resp:
            return []
        soup = BeautifulSoup(resp.content, 'html.parser')
        found = []
        for sel in [
            ('div', {'class': 'eachStory'}),
            ('article', {}),
            ('div', {'class': re.compile('story|card|item', re.I)}),
        ]:
            tag, attrs = sel
            nodes = soup.find_all(tag, attrs=attrs if attrs else None)
            if nodes:
                found = nodes
                break
        print(f"  Found {len(found)} listing nodes on Economic Times page")
        results = []
        for idx, node in enumerate(found, 1):
            try:
                title_tag = node.find(['h1', 'h2', 'h3', 'h4']) or node.find('a')
                if not title_tag:
                    continue
                title = title_tag.get_text(strip=True)
                link_tag = node.find('a', href=True)
                if not link_tag:
                    continue
                link = link_tag['href']
                if not link.startswith('http'):
                    link = f"https://economictimes.indiatimes.com{link}"
                if '/slideshow/' in link or '/photostory/' in link:
                    continue
                h = self._hash(title, link)
                if h in self.seen:
                    continue
                print(f"  [{idx}] {title[:120]}...")
                full = self._get_full_et(link)
                pub = None
                art_resp = self._safe_get(link)
                if art_resp:
                    art_soup = BeautifulSoup(art_resp.content, 'html.parser')
                    pub_dt = self._extract_published_from_soup(art_soup)
                    if pub_dt:
                        pub = pub_dt.isoformat()
                obj = {
                    "id": h,
                    "source": "EconomicTimes",
                    "title": title,
                    "url": link,
                    "published_at": pub,
                    "fetched_at": datetime.now().isoformat(),
                    "body": full
                }
                results.append(obj)
                self.seen.add(h)
                self.source_state.setdefault('economictimes', {})['last_fetch_time'] = datetime.now().isoformat()
                time.sleep(POLITE_DELAY_BETWEEN_ARTICLES)
            except Exception as e:
                print(f"  ✗ Error processing ET node #{idx}: {e}")
                continue
        return results

    def fetch_thehindu(self) -> List[Dict]:
        url = "https://www.thehindu.com/business/"
        print(f"\n→ Fetching TheHindu Business listing: {url}")
        resp = self._safe_get(url)
        if not resp:
            return []
        soup = BeautifulSoup(resp.content, 'html.parser')

        # Collect candidate links
        links = set()
        for a in soup.find_all('a', href=True):
            href = a['href']
            if not href.startswith('http'):
                href = f"https://www.thehindu.com{href}"
            links.add((a.get_text(" ", strip=True), href))

        # Whitelist only business/finance/markets sections
        WHITELIST = (
            "/business/",              # base
            "/business/markets/",      # markets
            "/business/Industry/",     # industry
            "/business/Economy/",      # economy/finance
            "/business/companies/",    # companies
            "/business/Banking/",      # banking/finance
            "/business/Stock-Market/", # sometimes used
        )
        BLACKLIST = ("/sport/", "/entertainment/", "/sci-tech/", "/education/")

        filtered = []
        for title, href in links:
            path = href.lower()
            if not any(seg.lower() in path for seg in [w.lower() for w in WHITELIST]):
                continue
            if any(b in path for b in BLACKLIST):
                continue
            if len(title or "") < 8:
                continue
            filtered.append((title, href))

        print(f"  TheHindu: {len(filtered)} article links after business-only filter")
        results = []
        for idx, (title, link) in enumerate(filtered, 1):
            try:
                h = self._hash(title, link)
                if h in self.seen:
                    continue
                print(f"  [{idx}] {title[:140]}...")
                full = self._get_full_thehindu(link)  # your existing body extractor
                # best-effort published_at
                pub = None
                art = self._safe_get(link)
                if art:
                    art_soup = BeautifulSoup(art.content, 'html.parser')
                    pd = self._extract_published_from_soup(art_soup)
                    if pd:
                        pub = pd.isoformat()
                results.append({
                    "id": h,
                    "source": "TheHindu",
                    "title": title,
                    "url": link,
                    "published_at": pub,
                    "fetched_at": datetime.now().isoformat(),
                    "body": full
                })
                self.seen.add(h)
                time.sleep(0.6)
            except Exception as e:
                print(f"  ✗ Error processing TheHindu link #{idx}: {e}")
                continue
        return results

    # ---------- Runner ----------
    def fetch_all(self) -> List[Dict]:
        print(f"\n=== Fetching cycle @ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===")
        items = []
        try:
            items.extend(self.fetch_livemint())
        except Exception as e:
            print(f"Error during LiveMint fetch: {e}")
        try:
            items.extend(self.fetch_economictimes())
        except Exception as e:
            print(f"Error during EconomicTimes fetch: {e}")
        try:
            items.extend(self.fetch_thehindu())
        except Exception as e:
            print(f"Error during TheHindu fetch: {e}")
        print(f"Cycle result: {len(items)} new articles fetched.")
        return items

    def run_continuous(self, interval_min: int = FETCH_INTERVAL_MIN):
        print(f"\n🚀 Starting News Fetcher: fetching every {interval_min} minutes. Press Ctrl+C to stop.")
        try:
            while True:
                items = self.fetch_all()
                if items:
                    self._append_staging(items)
                else:
                    print("No new items this run.")
                # persist seen and source state after each run
                self._save_seen()
                self._save_source_state()
                print(f"Waiting {interval_min} minutes before next run...\n")
                time.sleep(interval_min * 60)
        except KeyboardInterrupt:
            print("\nInterrupted by user. Saving state and exiting.")
            self._save_seen()
            self._save_source_state()
            sys.exit(0)

# --- minimal adapter so other modules can "from news_fetcher import fetch_all" ---
def fetch_all():
    f = NewsFetcher()
    items = f.fetch_all()
    # persist state so next process run also knows these are seen
    try:
        f._append_staging(items)
        f._save_seen()
        f._save_source_state()
    except Exception:
        pass
    return items


if __name__ == "__main__":
    fetcher = NewsFetcher()
    fetcher.run_continuous(interval_min=FETCH_INTERVAL_MIN)
