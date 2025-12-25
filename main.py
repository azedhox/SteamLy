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
executor = ThreadPoolExecutor(max_workers=10)

# --- CORS ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Rate Limiting ---
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
            resp = scraper.get(url, params=params, timeout=10)
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
        "message": "Akwam API - Ultra Fast",
        "version": "3.0",
        "endpoints": ["/movies", "/series", "/search", "/movie", "/show", "/watch"]
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

# --- Content Details ---
def get_content_details(url: str, is_movie: bool):
    try:
        url = fix_url(url)
        with get_scraper(referer=url) as scraper:
            resp = scraper.get(url, timeout=15)
            if resp.status_code != 200:
                raise HTTPException(404, "Content not found")
            
            soup = BeautifulSoup(resp.content, "html.parser")
            title_elem = soup.select_one("h1.entry-title")
            title = title_elem.text.strip() if title_elem else "Unknown"
            
            poster = ""
            try:
                for script in soup.find_all('script', type='application/ld+json'):
                    if script.string:
                        data = json.loads(script.string)
                        if isinstance(data, list) and 'image' in data[0]:
                            poster = data[0]['image'][0] if isinstance(data[0]['image'], list) else data[0]['image']
                            break
            except:
                pass
            
            if not poster:
                poster_img = soup.select_one("div.movie-cover img")
                if poster_img:
                    raw_poster = poster_img.get('src', '')
                    poster = get_high_quality_image(raw_poster)

            story_elem = soup.select_one("div.widget-body h2 .text-white")
            story = story_elem.text.strip() if story_elem else ""

            if is_movie:
                links = []
                tabs = soup.select("ul.header-tabs li a")
                for tab in tabs:
                    content = soup.select_one(tab['href'])
                    if content:
                        watch = content.select_one("a.link-show")
                        dl = content.select_one("a.link-download")
                        size = content.select_one("a.link-download span.font-size-14")
                        links.append({
                            "quality": tab.text.strip(),
                            "size": size.text.strip() if size else "",
                            "watch_url": watch['href'] if watch else None,
                            "download_url": dl['href'] if dl else None
                        })
                
                return {
                    "status": "success",
                    "type": "movie",
                    "details": {
                        "title": title,
                        "poster": poster,
                        "story": story,
                        "links": links
                    }
                }
            else:
                episodes = []
                ep_container = soup.select_one("#series-episodes .widget-body .row")
                if ep_container:
                    for card in ep_container.find_all("div", class_=lambda x: x and "col-lg-4" in x):
                        tag = card.select_one("h2 a")
                        if not tag:
                            continue
                        img = card.select_one("img")
                        raw_img = img['src'] if img else ""
                        episodes.append({
                            "name": tag.text.strip(),
                            "link": fix_url(tag['href']),
                            "image": get_high_quality_image(raw_img)
                        })
                
                return {
                    "status": "success",
                    "type": "series",
                    "details": {
                        "title": title,
                        "poster": poster,
                        "story": story,
                        "episodes_count": len(episodes),
                        "episodes": episodes
                    }
                }
    except Exception as e:
        raise HTTPException(500, f"Error: {str(e)}")

@app.get("/movie")
def movie_details(url: str):
    return get_content_details(url, is_movie=True)

@app.get("/show")
def series_details(url: str):
    return get_content_details(url, is_movie=False)

# --- ULTRA FAST VIDEO EXTRACTION ---

# Cache للـ regex patterns (compile مرة واحدة فقط)
VIDEO_SOURCE_PATTERN = re.compile(
    r'<source[^>]*(?:src=["\']([^"\']+)["\'][^>]*size=["\'](\d+)["\']|size=["\'](\d+)["\'][^>]*src=["\']([^"\']+)["\'])',
    re.IGNORECASE
)
REDIRECT_PATTERN_1 = re.compile(r'<a[^>]*href=["\']([^"\']*ak\.sv/watch[^"\']*)["\']', re.IGNORECASE)
REDIRECT_PATTERN_2 = re.compile(r'<a[^>]*href=["\']([^"\']+)["\'][^>]*>(?:اضغط هنا|Click here)', re.IGNORECASE)
VIDEO_TAG_CHECK = re.compile(r'<video[^>]*id=["\']player["\']', re.IGNORECASE)

def extract_videos_ultra_fast(html_chunk: str):
    """استخراج روابط الفيديو بأقصى سرعة باستخدام regex مُحسّن"""
    videos = []
    for match in VIDEO_SOURCE_PATTERN.finditer(html_chunk):
        src = match.group(1) or match.group(4)
        size = match.group(2) or match.group(3)
        if src and size:
            videos.append({
                "quality": f"{size}p",
                "link": src,
                "type": "mp4"
            })
    return videos

def find_redirect_ultra_fast(html_chunk: str):
    """البحث عن رابط التوجيه بأقصى سرعة"""
    match = REDIRECT_PATTERN_1.search(html_chunk)
    if match:
        return match.group(1)
    match = REDIRECT_PATTERN_2.search(html_chunk)
    if match:
        return match.group(1)
    return None

@app.get("/watch")
async def watch_video(url: str):
    """جلب روابط MP4 بأقصى سرعة ممكنة - محسّن جداً"""
    try:
        url = fix_url(url)
        
        def fetch_video():
            with get_scraper(referer=url) as scraper:
                # الطلب الأول - بدون stream للحصول على الاستجابة الكاملة مباشرة
                resp = scraper.get(url, timeout=8)
                html = resp.text
                
                # فحص سريع: هل توجد روابط فيديو مباشرة؟
                if VIDEO_TAG_CHECK.search(html):
                    videos = extract_videos_ultra_fast(html)
                    if videos:
                        return {"status": "success", "original_url": url, "videos": videos}
                
                # البحث عن رابط التوجيه
                redirect_url = find_redirect_ultra_fast(html)
                if not redirect_url:
                    return {"status": "failed", "message": "Direct link not found"}
                
                # الطلب الثاني للصفحة النهائية
                final_url = fix_url(redirect_url)
                scraper.headers.update({"Referer": quote(final_url, safe=":/%?=&")})
                final_resp = scraper.get(final_url, timeout=8)
                
                videos = extract_videos_ultra_fast(final_resp.text)
                if videos:
                    return {"status": "success", "original_url": final_url, "videos": videos}
                
                return {"status": "failed", "message": "No video sources found"}
        
        # تشغيل العملية في thread pool
        result = await asyncio.get_event_loop().run_in_executor(executor, fetch_video)
        return result
            
    except Exception as e:
        raise HTTPException(500, f"Error: {str(e)}")

# --- Health Check ---
@app.get("/health")
def health_check():
    return {"status": "healthy", "timestamp": time.time()}
