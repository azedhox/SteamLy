from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import cloudscraper
from bs4 import BeautifulSoup
import re
import json
import time
from collections import defaultdict
from functools import lru_cache
from urllib.parse import quote

# ===============================
# App
# ===============================
app = FastAPI(title="Akwam Scraper API - Ultra Fast v4")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_URL = "https://ak.sv"

# ===============================
# Global Scraper (IMPORTANT)
# ===============================
scraper = cloudscraper.create_scraper(
    browser={"browser": "chrome", "platform": "windows", "desktop": True}
)

scraper.headers.update({
    "Origin": BASE_URL,
    "Accept-Language": "en-US,en;q=0.9,ar;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
})

# ===============================
# Rate Limit
# ===============================
rate_limit_storage = defaultdict(list)

def check_rate_limit(ip: str, max_requests=40, window=60):
    now = time.time()
    rate_limit_storage[ip] = [t for t in rate_limit_storage[ip] if now - t < window]
    if len(rate_limit_storage[ip]) >= max_requests:
        return False
    rate_limit_storage[ip].append(now)
    return True

@app.middleware("http")
async def rate_limit(request: Request, call_next):
    ip = request.client.host
    if not check_rate_limit(ip):
        return JSONResponse(status_code=429, content={"status": "error", "message": "Too many requests"})
    return await call_next(request)

# ===============================
# Utils
# ===============================
def fix_url(url: str):
    if url.startswith("http"):
        return url
    return BASE_URL + url if url.startswith("/") else f"{BASE_URL}/{url}"

def hq_image(url: str):
    return re.sub(r"/thumb/[^/]+", "", url) if url else ""

# ===============================
# Regex (Compiled once)
# ===============================
VIDEO_SOURCE_RE = re.compile(
    r'<source[^>]+src=["\']([^"\']+)["\'][^>]+size=["\'](\d+)["\']',
    re.IGNORECASE
)

REDIRECT_RE = re.compile(
    r'<a[^>]+href=["\']([^"\']+/watch[^"\']*)["\']',
    re.IGNORECASE
)

VIDEO_TAG_RE = re.compile(r'<video[^>]+id=["\']player["\']', re.IGNORECASE)

# ===============================
# Cache (VERY IMPORTANT)
# ===============================
WATCH_CACHE = {}

CACHE_TTL = 60 * 60  # 1 hour

def get_cached(url):
    item = WATCH_CACHE.get(url)
    if not item:
        return None
    if time.time() - item["time"] > CACHE_TTL:
        del WATCH_CACHE[url]
        return None
    return item["data"]

def set_cache(url, data):
    WATCH_CACHE[url] = {"time": time.time(), "data": data}

# ===============================
# Ultra Fast Video Extractor
# ===============================
def extract_videos(html: str):
    videos = []
    for src, size in VIDEO_SOURCE_RE.findall(html):
        videos.append({
            "quality": f"{size}p",
            "link": src,
            "type": "mp4"
        })
    return videos

# ===============================
# Watch Endpoint (FAST)
# ===============================
@app.get("/watch")
async def watch(url: str):
    url = fix_url(url)

    # 1️⃣ Cache
    cached = get_cached(url)
    if cached:
        return cached

    try:
        scraper.headers["Referer"] = quote(url, safe=":/%?=&")
        resp = scraper.get(url, timeout=6)
        html = resp.text

        # 2️⃣ Direct video
        if VIDEO_TAG_RE.search(html):
            videos = extract_videos(html)
            if videos:
                result = {"status": "success", "original_url": url, "videos": videos}
                set_cache(url, result)
                return result

        # 3️⃣ Redirect
        redirect_match = REDIRECT_RE.search(html)
        if not redirect_match:
            raise HTTPException(404, "No redirect found")

        final_url = fix_url(redirect_match.group(1))
        scraper.headers["Referer"] = quote(final_url, safe=":/%?=&")

        final_resp = scraper.get(final_url, timeout=6)
        videos = extract_videos(final_resp.text)

        if not videos:
            raise HTTPException(404, "No video sources")

        result = {"status": "success", "original_url": final_url, "videos": videos}
        set_cache(url, result)
        return result

    except Exception as e:
        raise HTTPException(500, f"Error: {str(e)}")

# ===============================
# Health
# ===============================
@app.get("/health")
def health():
    return {"status": "ok", "time": time.time()}
