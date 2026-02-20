
from __future__ import annotations
import re, time, random
from typing import List, Dict
import requests
from bs4 import BeautifulSoup

from .db import insert_raw_many

SITE_BASE = 'https://sn.coinafrique.com'
CATEGORIES = {
    'Chiens': '/categorie/chiens',
    'Moutons': '/categorie/moutons',
    'Poules-Lapins-Pigeons': '/categorie/poules-lapins-et-pigeons',
    'Autres animaux': '/categorie/autres-animaux',
}
PAGE_PATTERNS = ['{base}{path}?page={n}', '{base}{path}/{n}']
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36'}

PRICE = re.compile(r'(\d[\d\s\.,]*)', re.I)

# ---- DETAIL SELECTORS
DETAIL = {
    'Chiens': {
        'title': '.hide-on-med-and-down h1',
        'price': '.hide-on-med-and-down p.price',
        'addr':  '.hide-on-med-and-down [data-address] span',
        'img':   'div.col:nth-of-type(1) img.ad__card-img',
    },
    'Moutons': {
        'title': '.hide-on-med-and-down h1',
        'price': '.hide-on-med-and-down p.price',
        'addr':  '.hide-on-med-and-down [data-address] span',
        'img':   'div.col:nth-of-type(1) img.ad__card-img',
    },
    'Poules-Lapins-Pigeons': {
        'title': '.hide-on-med-and-down h1',  
        'price': '.hide-on-med-and-down p.price',
        'addr':  '.hide-on-med-and-down [data-address] span',
        'img':   'div.col:nth-of-type(1) img.ad__card-img',
    },
    'Autres animaux': {
        'title': '.hide-on-med-and-down h1',
        'price': '.hide-on-med-and-down p.price',
        'addr':  '.hide-on-med-and-down [data-address] span',
        'img':   'div.col:nth-of-type(2) img.ad__card-img',
    },
}

BAD_IMG_TOKENS = ['/static/images/countries/', '/static/flags/', '/svg', 'data:image']

# ------------- Helpers -------------

def _fetch(url: str, timeout: int=25) -> requests.Response:
    r = requests.get(url, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    return r


def _abs(href: str | None) -> str | None:
    if not href:
        return None
    return href if href.startswith('http') else f"{SITE_BASE}{href}"


def _select_text(soup: BeautifulSoup, css: str) -> str | None:
    el = soup.select_one(css)
    if el:
        t = el.get_text(strip=True)
        if t:
            return t
    return None


def _select_img(soup: BeautifulSoup, css: str) -> str | None:
    el = soup.select_one(css)
    if not el:
        return None
    # Try common image attributes
    for attr in ['data-src','data-lazy','data-original','src','srcset']:
        v = el.get(attr)
        if v:
            if attr=='srcset' and ' ' in v:
                v = v.split(' ')[0]
            if any(tk in v for tk in BAD_IMG_TOKENS):
                return None
            return v if v.startswith('http') else (SITE_BASE + v)
    return None


def _detail_from_url(url: str, category: str) -> Dict:
    try:
        r = _fetch(url)
        s = BeautifulSoup(r.text, 'lxml')
        sel = DETAIL[category]
        title = _select_text(s, sel['title'])
        price = _select_text(s, sel['price'])
        addr  = _select_text(s, sel['addr'])
        img   = _select_img(s, sel['img'])
        # Fallback og:image
        if not img:
            og = s.select_one('meta[property="og:image"]')
            if og and og.get('content'):
                img = og.get('content')
        # Price fallback regex
        if not price:
            m = PRICE.search(s.get_text(' ', strip=True))
            price = m.group(1) if m else None
        return {'title': title, 'price_raw': price, 'address_raw': addr, 'image_url': img}
    except Exception:
        return {'title': None, 'price_raw': None, 'address_raw': None, 'image_url': None}

# ------------- BS4 main -------------

def bs4_scrape_insert(category: str, start_page: int, end_page: int, sleep=(0.8,1.6), visit_detail: bool=True) -> int:
    total = 0
    for p in range(start_page, end_page+1):
        urls = [pat.format(base=SITE_BASE, path=CATEGORIES[category], n=p) for pat in PAGE_PATTERNS]
        # find listing links
        listing_html = None
        for u in urls:
            try:
                r = _fetch(u)
                listing_html = r.text
                break
            except Exception:
                continue
        if not listing_html:
            continue
        s = BeautifulSoup(listing_html, 'lxml')
        # Web Scraper uses .ad__card-description a
        anchors = s.select('.ad__card-description a[href]')
        links = []
        for a in anchors:
            href = a.get('href'); ah = _abs(href)
            if ah and ('/annonce/' in ah):
                links.append(ah)
        # Visit details
        rows = []
        for href in links:
            data = _detail_from_url(href, category) if visit_detail else {}
            rows.append({
                'source':'coinafrique-sn','category':category,'title':data.get('title'),
                'price_raw':data.get('price_raw'),'address_raw':data.get('address_raw'),
                'image_url':data.get('image_url'),'link':href,'page':p
            })
            time.sleep(random.uniform(*sleep))
        insert_raw_many(rows)
        total += len(rows)
    return total

# ------------- Selenium main -------------

def selenium_scrape_insert(category: str, start_page: int, end_page: int, headless=True, sleep=(0.8,1.6), visit_detail: bool=True) -> int:
    try:
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.chrome.service import Service as ChromeService
        from webdriver_manager.chrome import ChromeDriverManager
    except Exception as e:
        raise RuntimeError('Selenium non disponible. Installez Chrome + webdriver-manager.') from e

    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument('--headless=new')
    options.add_argument('--no-sandbox'); options.add_argument('--disable-dev-shm-usage'); options.add_argument('--window-size=1600,1200')
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)

    total = 0
    try:
        for p in range(start_page, end_page+1):
            urls = [pat.format(base=SITE_BASE, path=CATEGORIES[category], n=p) for pat in PAGE_PATTERNS]
            loaded=False
            for url in urls:
                try:
                    driver.get(url)
                    WebDriverWait(driver, 12).until(
                        EC.presence_of_all_elements_located((By.CSS_SELECTOR, '.ad__card-description a[href]'))
                    )
                    loaded=True; break
                except Exception:
                    continue
            if not loaded:
                continue
            # Collect links
            anchors = driver.find_elements(By.CSS_SELECTOR, '.ad__card-description a[href]')
            links = []
            for a in anchors:
                href = a.get_attribute('href')
                if href and '/annonce/' in href:
                    links.append(href)
            # Visit each detail
            rows = []
            for href in links:
                if visit_detail:
                    try:
                        driver.get(href)
                        WebDriverWait(driver, 12).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, '.hide-on-med-and-down'))
                        )
                        sel = DETAIL[category]
                        # title
                        try:
                            title = driver.find_element(By.CSS_SELECTOR, sel['title']).text
                        except Exception:
                            title = None
                        # price
                        try:
                            price_raw = driver.find_element(By.CSS_SELECTOR, sel['price']).text
                        except Exception:
                            # fallback regex on page text
                            import re as _re
                            m = _re.search(r'(\d[\d\s\.,]*)', driver.page_source)
                            price_raw = m.group(1) if m else None
                        # address
                        try:
                            address_raw = driver.find_element(By.CSS_SELECTOR, sel['addr']).text
                        except Exception:
                            address_raw = None
                        # image
                        image_url = None
                        try:
                            img = driver.find_element(By.CSS_SELECTOR, sel['img'])
                            for attr in ['data-src','data-lazy','data-original','src','srcset']:
                                v = img.get_attribute(attr)
                                if v:
                                    if attr=='srcset' and ' ' in v:
                                        v = v.split(' ')[0]
                                    image_url = v; break
                        except Exception:
                            pass
                        if image_url and any(tk in image_url for tk in BAD_IMG_TOKENS):
                            image_url = None
                        if image_url and not image_url.startswith('http'):
                            image_url = SITE_BASE + image_url
                    except Exception:
                        title = price_raw = address_raw = image_url = None
                else:
                    title = price_raw = address_raw = image_url = None

                rows.append({
                    'source':'coinafrique-sn','category':category,'title':title,'price_raw':price_raw,
                    'address_raw':address_raw,'image_url':image_url,'link':href,'page':p
                })
                time.sleep(random.uniform(*sleep))
            insert_raw_many(rows)
            total += len(rows)
    finally:
        try: driver.quit()
        except Exception: pass
    return total
