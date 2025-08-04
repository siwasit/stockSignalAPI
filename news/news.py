import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin, quote
import feedparser
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
import time
from selenium.common.exceptions import TimeoutException

def find_favicon_link(url):
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # รันแบบไม่เปิด browser
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920x1080")

    service = Service("D:/DesperateProject/IdeatradeIntern/scrapSet/chromedriver-win64/chromedriver.exe")
    driver = webdriver.Chrome(service=service, options=chrome_options)

    try:
        driver.set_page_load_timeout(1)
        driver.get(url)
        soup = BeautifulSoup(driver.page_source, 'html.parser')
    except TimeoutException:
        driver.execute_script("window.stop();")
        soup = BeautifulSoup(driver.page_source, "html.parser")
    finally:
        driver.quit()

    parsed = urlparse(url)
    root_url = f"{parsed.scheme}://{parsed.netloc}"

    for link in soup.find_all('link'):
        href = link.get('href')
        if href and "favicon.ico" in href.lower():
            # ถ้า href ไม่ใช่ full URL ให้แปลงเป็น full URL
            if not href.startswith("http"):
                href = urljoin(root_url, href)
            return href

    return urljoin(root_url, "/favicon.ico")

def get_favicons(url):
    parsed = urlparse(url)
    root_url = f"{parsed.scheme}://{parsed.netloc}"
    favicons = []

    root_favicon_url = urljoin(root_url, "/favicon.ico")

    try:
        resp = requests.get(url, timeout=3)
        
        if resp.status_code != 200:
            raise Exception(f"HTTP error: {resp.status_code}")
        soup = BeautifulSoup(resp.text, "html.parser")

        for link in soup.find_all("link", rel=True):
            rel_value = link["rel"]
            if isinstance(rel_value, list):
                rel_str = " ".join(rel_value).lower()
            else:
                rel_str = rel_value.lower()

            if "icon" in rel_str:
                href = link.get("href")
                if href:
                    if not href.startswith("http"):
                        href = urljoin(root_url, href)
                    favicons.append(href)

        if (favicons != []):
            return favicons
        else: 
            return [root_favicon_url]
    except Exception:
        favicon = find_favicon_link(url)
        return [favicon]

import feedparser
from urllib.parse import quote

def get_news(symbol, stock_market, thai_name, eng_name, limit=3):
    """
    ดึงข่าว Google News RSS ตามตลาดหุ้น
    รวมทั้งจีนและสากลถ้าเป็น SSE/SZSE
    """
    feeds = []

    # ตลาดสหรัฐ
    if stock_market in ["AMEX", "CBOE", "NASDAQ", "NYSE"]:
        query = f'"{stock_market}:{symbol}" OR "{eng_name}"'
        rss_url = f"https://news.google.com/rss/search?q={quote(query)}"
        feeds.append(rss_url)

    # ตลาด SET (ใช้ไทย)
    elif stock_market == "SET":
        query = f'"{thai_name}" OR "{eng_name}"'
        rss_url = f"https://news.google.com/rss/search?q={quote(query)}&hl=th&gl=TH&ceid=TH:th"
        feeds.append(rss_url)

    # ตลาดจีน SSE/SZSE (ดึงทั้งจีนและสากล)
    elif stock_market in ["SSE", "SZSE"]:
        query = f'"{stock_market}:{symbol}" OR "{eng_name}" OR "{thai_name}"'
        # สากล (default EN)
        feeds.append(f"https://news.google.com/rss/search?q={quote(query)}")
        # จีนแผ่นดินใหญ่
        feeds.append(f"https://news.google.com/rss/search?q={quote(query)}&hl=zh-CN&gl=CN&ceid=CN:zh-Hans")

    # ถ้าไม่ตรงตลาดข้างบน ใช้ชื่ออังกฤษอย่างเดียว
    # else:
    #     query = f'"{eng_name}"'
    #     feeds.append(f"https://news.google.com/rss/search?q={quote(query)}")

    # รวมข่าวจากทุก feed
    all_entries = []
    for rss_url in feeds:
        feed = feedparser.parse(rss_url)
        all_entries.extend(feed.entries)

    # เอาเฉพาะข่าวล่าสุด limit รายการ
    all_entries = sorted(all_entries, key=lambda e: e.get("published_parsed", None), reverse=True)[:limit]

    data = []
    for entry in all_entries:
        source = entry.get("source", {})
        source_title = source.get("title", "")
        source_href = source.get("href", "")

        favicons = []
        if source_href and source_href.startswith("http"):
            try:
                favicons = get_favicons(source_href)
            except:
                favicons = []

        data.append({
            "title": entry.get("title", ""),
            "published": entry.get("published", ""),
            "source": source_title,
            "web_source": source_href,
            "favicons": favicons,
            "link": entry.get("link", ""),
        })

    return data
