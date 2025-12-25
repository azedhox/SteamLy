from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import cloudscraper
from bs4 import BeautifulSoup
from typing import List, Optional
import re
import json
from urllib.parse import quote
from functools import lru_cache
from contextlib import contextmanager
import time
from collections import defaultdict
import threading
import asyncio
from concurrent.futures import ThreadPoolExecutor

app = FastAPI(title="Akwam Scraper API - Ultra Fast")

# Thread pool للعمليات المتزامنة
executor = ThreadPoolExecutor(max_workers=15)

# --- CORS ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

rate_limit_storage = defaultdict(list)
rate_limit_lock = threading.Lock()

def check_rate_limit(ip: str, max_requests: int = 30, window: int = 60):
    current_time = time.time()
    with rate_limit_lock:
        rate_limit_storage[ip] = [
            req_time for req_time in rate_limit_storage[ip]
            if current_time - req_time < window
        ]
        if len(rate_limit_storage[ip]) >= max_requests:
            return False
        rate_limit_storage[ip].append(current_time)
        return True

@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    client_ip = request.client.host
    if not check_rate_limit(client_ip):
        return JSONResponse(
            status_code=429,
            content={"status": "error", "message": "Too many requests. Please try again later."}
        )
    response = await call_next(request)
    return response

# --- Scraper Factory ---
@contextmanager
def get_scraper(referer: str = None):
    scraper = cloudscraper.create_scraper(
        browser={'browser': 'chrome', 'platform': 'windows', 'desktop': True}
    )
    headers = {
        "Origin": "https://ak.sv",
        "Accept-Language": "en-US,en;q=0.9,ar;q=0.8",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
    }
    if referer:
        headers["Referer"] = quote(referer, safe=":/%?=&")
    scraper.headers.update(headers)
    try:
        yield scraper
    finally:
        scraper.close()

BASE_URL = "https://ak.sv"

# --- Categories ---
GENRES = [
    {"id": 0, "name": "الكل"}, {"id": 18, "name": "أكشن"},
    {"id": 20, "name": "كوميدي"}, {"id": 23, "name": "دراما"},
    {"id": 22, "name": "رعب"}, {"id": 35, "name": "إثارة"},
    {"id": 34, "name": "غموض"}, {"id": 24, "name": "خيال علمي"},
    {"id": 27, "name": "رومانسي"}, {"id": 19, "name": "مغامرة"},
    {"id": 21, "name": "جريمة"}, {"id": 43, "name": "فانتازيا"},
    {"id": 33, "name": "عائلي"}, {"id": 30, "name": "أنمي"},
    {"id": 28, "name": "وثائقي"}, {"id": 25, "name": "حربي"},
    {"id": 26, "name": "تاريخي"}, {"id": 29, "name": "سيرة ذاتية"},
    {"id": 31, "name": "موسيقي"}, {"id": 32, "name": "رياضي"},
    {"id": 87, "name": "رمضان"}, {"id": 72, "name": "Netflix"}
]

# --- Compiled Regex Patterns ---
VIDEO_SOURCE_PATTERN = re.compile(
    r'<source[^>]*(?:src=["\']([^"\']+)["\'][^>]*size=["\'](\d+)["\']|size=["\'](\d+)["\'][^>]*src=["\']([^"\']+)["\'])',
    re.IGNORECASE
)
REDIRECT_PATTERN_1 = re.compile(r'<a[^>]*href=["\']([^"\']*ak\.sv/watch[^"\']*)["\']', re.IGNORECASE)
REDIRECT_PATTERN_2 = re.compile(r'<a[^>]*href=["\']([^"\']+)["\'][^>]*>(?:اضغط هنا|Click here)', re.IGNORECASE)
VIDEO_TAG_CHECK = re.compile(r'<video[^>]*id=["\']player["\']', re.IGNORECASE)

# Regex للـ movie details - استخراج بدون BeautifulSoup
TITLE_PATTERN = re.compile(r'<h1[^>]*class=["\']entry-title["\'][^>]*>([^<]+)</h1>', re.IGNORECASE)
POSTER_JSON_PATTERN = re.compile(r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>([^<]+)</script>', re.IGNORECASE)
STORY_PATTERN = re.compile(r'<div[^>]*class=["\']widget-body[^"\']*["\'][^>]*>.*?<h2[^>]*>.*?<span[^>]*class=["\']text-white["\'][^>]*>([^<]+)</span>', re.DOTALL | re.IGNORECASE)

# Regex لاستخراج روابط الجودات
QUALITY_TAB_PATTERN = re.compile(r'<li[^>]*>\s*<a[^>]*href=["\']#([^"\']+)["\'][^>]*>([^<]+)</a>', re.IGNORECASE)
WATCH_LINK_PATTERN = re.compile(r'<a[^>]*class=["\'][^"\']*link-show[^"\']*["\'][^>]*href=["\']([^"\']+)["\']', re.IGNORECASE)
DOWNLOAD_LINK_PATTERN = re.compile(r'<a[^>]*class=["\'][^"\']*link-download[^"\']*["\'][^>]*href=["\']([^"\']+)["\']', re.IGNORECASE)
SIZE_PATTERN = re.compile(r'<span[^>]*class=["\'][^"\']*font-size-14[^"\']*["\'][^>]*>([^<]+)</span>', re.IGNORECASE)

# Regex للحلقات
EPISODE_PATTERN = re.compile(
    r'<div[^>]*class=["\'][^"\']*col-lg-4[^"\']*["\'][^>]*>.*?<h2[^>]*>.*?<a[^>]*href=["\']([^"\']+)["\'][^>]*>([^<]+)</a>.*?<img[^>]*src=["\']([^"\']+)["\']',
    re.DOTALL | re.IGNORECASE
)

# --- Helper Functions ---
def fix_url(url):
    if not url.startswith("http"):
        return f"{BASE_URL}{url}" if url.startswith("/") else f"{BASE_URL}/{url}"
    return url

def get_high_quality_image(img_url):
    if not img_url: return ""
    return re.sub(r'/thumb/[^/]+', '', img_url)

def parse_grid(soup):
    cards = soup.find_all("div", class_="entry-box")
    results = []
    for card in cards:
        title_tag = card.select_one("h3.entry-title a")
        if not title_tag: continue
        
        title = title_tag.text.strip()
        link = fix_url(title_tag['href'])
        
        img_tag = card.select_one("div.entry-image img")
        raw_image = img_tag.get('data-src') or img_tag.get('src') or ""
        image = get_high_quality_image(raw_image)
        
        rating = card.select_one("span.label.rating")
        rating = rating.text.strip() if rating else "0.0"
        
        quality = card.select_one("span.label.quality")
        quality = quality.text.strip() if quality else "N/A"
        
        year = card.select_one("span.badge-secondary")
        year = year.text.strip() if year else "N/A"

        results.append({
            "title": title, "link": link, "image": image,
            "rating": rating, "quality": quality, "year": year
        })
    return results

def fetch_page(url, params=None):
    try:
        with get_scraper(referer=url) as scraper:
            resp = scraper.get(url, params=params, timeout=8)
            if resp.status_code != 200:
                raise HTTPException(404, "Page not found")
            soup = BeautifulSoup(resp.content, "html.parser")
            data = parse_grid(soup)
            has_next = bool(soup.find("a", attrs={"rel": "next"}))
            return {
                "status": "success",
                "has_next": has_next,
                "count": len(data),
                "data": data
            }
    except Exception as e:
        raise HTTPException(500, f"Error: {str(e)}")

# --- Endpoints ---
@app.get("/")
def home():
    return {
        "message": "Akwam API - Ultra Fast v3.1",
        "version": "3.1",
        "endpoints": ["/movies", "/series", "/search", "/movie", "/show", "/watch", "/watch-direct"]
    }

@app.get("/categories")
@lru_cache(maxsize=1)
def get_categories():
    return {"status": "success", "categories": GENRES}

@app.get("/movies")
def movies(page: int = 1, category: int = 0):
    params = {
        "category": category, "formats": 0, "language": 0,
        "quality": 0, "rating": 0, "section": 0, "year": 0
    }
    if page > 1:
        params["page"] = page
    return fetch_page(f"{BASE_URL}/movies", params=params)

@app.get("/series")
def series(page: int = 1, category: int = 0):
    params = {
        "category": category, "formats": 0, "language": 0,
        "quality": 0, "rating": 0, "section": 0, "year": 0
    }
    if page > 1:
        params["page"] = page
    return fetch_page(f"{BASE_URL}/series", params=params)

@app.get("/search")
def search(q: str, page: int = 1):
    if len(q.strip()) < 2:
        raise HTTPException(400, "Search query too short")
    params = {"q": q}
    if page > 1:
        params["page"] = page
    return fetch_page(f"{BASE_URL}/search", params=params)

# --- ULTRA FAST Content Details ---

def extract_title_fast(html: str) -> str:
    """استخراج العنوان بسرعة بدون BeautifulSoup"""
    match = TITLE_PATTERN.search(html)
    return match.group(1).strip() if match else "Unknown"

def extract_poster_fast(html: str) -> str:
    """استخراج الصورة بسرعة"""
    # محاولة JSON-LD أولاً
    for match in POSTER_JSON_PATTERN.finditer(html):
        try:
            data = json.loads(match.group(1))
            if isinstance(data, list) and 'image' in data[0]:
                img = data[0]['image']
                return img[0] if isinstance(img, list) else img
            elif 'image' in data:
                img = data['image']
                return img[0] if isinstance(img, list) else img
        except:
            continue
    
    # Fallback: ابحث عن img في movie-cover
    img_match = re.search(r'<div[^>]*class=["\']movie-cover[^"\']*["\'][^>]*>.*?<img[^>]*src=["\']([^"\']+)["\']', html, re.DOTALL | re.IGNORECASE)
    if img_match:
        return get_high_quality_image(img_match.group(1))
    
    return ""

def extract_story_fast(html: str) -> str:
    """استخراج القصة بسرعة"""
    match = STORY_PATTERN.search(html)
    return match.group(1).strip() if match else ""

def extract_movie_links_fast(html: str) -> list:
    """استخراج روابط الفيلم بسرعة فائقة"""
    links = []
    
    # استخراج التابات (الجودات)
    tabs = QUALITY_TAB_PATTERN.findall(html)
    
    for tab_id, quality_name in tabs:
        # البحث عن محتوى هذا التاب
        tab_content_pattern = re.compile(
            rf'<div[^>]*id=["\']({re.escape(tab_id)})["\'][^>]*>(.*?)</div>',
            re.DOTALL | re.IGNORECASE
        )
        tab_match = tab_content_pattern.search(html)
        
        if tab_match:
            content = tab_match.group(2)
            
            watch_match = WATCH_LINK_PATTERN.search(content)
            download_match = DOWNLOAD_LINK_PATTERN.search(content)
            size_match = SIZE_PATTERN.search(content)
            
            links.append({
                "quality": quality_name.strip(),
                "size": size_match.group(1).strip() if size_match else "",
                "watch_url": watch_match.group(1) if watch_match else None,
                "download_url": download_match.group(1) if download_match else None
            })
    
    return links

def extract_episodes_fast(html: str) -> list:
    """استخراج الحلقات بسرعة فائقة"""
    episodes = []
    
    for match in EPISODE_PATTERN.finditer(html):
        link, name, img = match.groups()
        episodes.append({
            "name": name.strip(),
            "link": fix_url(link),
            "image": get_high_quality_image(img)
        })
    
    return episodes

@app.get("/movie")
async def movie_details(url: str):
    """جلب تفاصيل الفيلم بأقصى سرعة"""
    try:
        url = fix_url(url)
        
        def fetch():
            with get_scraper(referer=url) as scraper:
                resp = scraper.get(url, timeout=8)
                if resp.status_code != 200:
                    raise HTTPException(404, "Content not found")
                
                html = resp.text
                
                return {
                    "status": "success",
                    "type": "movie",
                    "details": {
                        "title": extract_title_fast(html),
                        "poster": extract_poster_fast(html),
                        "story": extract_story_fast(html),
                        "links": extract_movie_links_fast(html)
                    }
                }
        
        return await asyncio.get_event_loop().run_in_executor(executor, fetch)
    except Exception as e:
        raise HTTPException(500, f"Error: {str(e)}")

@app.get("/show")
async def series_details(url: str):
    """جلب تفاصيل المسلسل بأقصى سرعة"""
    try:
        url = fix_url(url)
        
        def fetch():
            with get_scraper(referer=url) as scraper:
                resp = scraper.get(url, timeout=8)
                if resp.status_code != 200:
                    raise HTTPException(404, "Content not found")
                
                html = resp.text
                episodes = extract_episodes_fast(html)
                
                return {
                    "status": "success",
                    "type": "series",
                    "details": {
                        "title": extract_title_fast(html),
                        "poster": extract_poster_fast(html),
                        "story": extract_story_fast(html),
                        "episodes_count": len(episodes),
                        "episodes": episodes
                    }
                }
        
        return await asyncio.get_event_loop().run_in_executor(executor, fetch)
    except Exception as e:
        raise HTTPException(500, f"Error: {str(e)}")

# --- ULTRA FAST VIDEO EXTRACTION ---

def extract_videos_ultra_fast(html: str) -> list:
    """استخراج روابط الفيديو بأقصى سرعة"""
    videos = []
    for match in VIDEO_SOURCE_PATTERN.finditer(html):
        src = match.group(1) or match.group(4)
        size = match.group(2) or match.group(3)
        if src and size:
            videos.append({
                "quality": f"{size}p",
                "link": src,
                "type": "mp4"
            })
    return videos

def find_redirect_ultra_fast(html: str) -> Optional[str]:
    """البحث عن رابط التوجيه بأقصى سرعة"""
    match = REDIRECT_PATTERN_1.search(html)
    if match:
        return match.group(1)
    match = REDIRECT_PATTERN_2.search(html)
    if match:
        return match.group(1)
    return None

@app.get("/watch")
async def watch_video(url: str):
    """جلب روابط MP4 بأقصى سرعة - نسخة محسّنة"""
    try:
        url = fix_url(url)
        
        def fetch_video():
            with get_scraper(referer=url) as scraper:
                resp = scraper.get(url, timeout=7)
                html = resp.text
                
                # فحص وجود فيديو مباشر
                if VIDEO_TAG_CHECK.search(html):
                    videos = extract_videos_ultra_fast(html)
                    if videos:
                        return {"status": "success", "original_url": url, "videos": videos}
                
                # البحث عن التوجيه
                redirect_url = find_redirect_ultra_fast(html)
                if not redirect_url:
                    return {"status": "failed", "message": "Direct link not found"}
                
                # الطلب النهائي
                final_url = fix_url(redirect_url)
                scraper.headers.update({"Referer": quote(final_url, safe=":/%?=&")})
                final_resp = scraper.get(final_url, timeout=7)
                
                videos = extract_videos_ultra_fast(final_resp.text)
                if videos:
                    return {"status": "success", "original_url": final_url, "videos": videos}
                
                return {"status": "failed", "message": "No video sources found"}
        
        return await asyncio.get_event_loop().run_in_executor(executor, fetch_video)
            
    except Exception as e:
        raise HTTPException(500, f"Error: {str(e)}")

@app.get("/watch-direct")
async def watch_direct(watch_url: str):
    """
    Endpoint جديد: يجلب mp4 مباشرة من رابط المشاهدة
    أسرع من /watch لأنه يتخطى خطوة البحث عن الرابط
    """
    try:
        watch_url = fix_url(watch_url)
        
        def fetch():
            with get_scraper(referer=watch_url) as scraper:
                # طلب واحد فقط مباشرة
                resp = scraper.get(watch_url, timeout=6)
                html = resp.text
                
                videos = extract_videos_ultra_fast(html)
                if videos:
                    return {"status": "success", "url": watch_url, "videos": videos}
                
                return {"status": "failed", "message": "No videos found"}
        
        return await asyncio.get_event_loop().run_in_executor(executor, fetch)
    
    except Exception as e:
        raise HTTPException(500, f"Error: {str(e)}")

# --- Health Check ---
@app.get("/health")
def health_check():
    return {"status": "healthy", "timestamp": time.time()}
