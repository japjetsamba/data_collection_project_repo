from __future__ import annotations
import os, time, random, re
from typing import Tuple
from .db import insert_raw_many
from .driver_factory import get_driver

SITE_BASE = 'https://sn.coinafrique.com'
CATEGORIES = {'Chiens':'/categorie/chiens','Moutons':'/categorie/moutons','Poules-Lapins-Pigeons':'/categorie/poules-lapins-et-pigeons','Autres animaux':'/categorie/autres-animaux'}
PAGE_PATTERNS = ['{base}{path}?page={n}', '{base}{path}/{n}']
PRICE = re.compile(r'(\d[\d\s\.,]*)', re.I)
BAD_IMG_TOKENS = ['/static/images/countries/','/static/flags/','/svg','data:image']

DETAIL = {
 'Chiens': {'title': '.hide-on-med-and-down h1','price': '.hide-on-med-and-down p.price','addr': '.hide-on-med-and-down [data-address] span','img': 'div.col:nth-of-type(1) img.ad__card-img'},
 'Moutons': {'title': '.hide-on-med-and-down h1','price': '.hide-on-med-and-down p.price','addr': '.hide-on-med-and-down [data-address] span','img': 'div.col:nth-of-type(1) img.ad__card-img'},
 'Poules-Lapins-Pigeons': {'title': '.hide-on-med-and-down h1','price': '.hide-on-med-and-down p.price','addr': '.hide-on-med-and-down [data-address] span','img': 'div.col:nth-of-type(1) img.ad__card-img'},
 'Autres animaux': {'title': '.hide-on-med-and-down h1','price': '.hide-on-med-and-down p.price','addr': '.hide-on-med-and-down [data-address] span','img': 'div.col:nth-of-type(2) img.ad__card-img'},
}

def _block_media_if_chromium(driver):
    try:
        name = (driver.capabilities.get('browserName') or '').lower()
        if ('chrome' in name) or ('edge' in name):
            driver.execute_cdp_cmd('Network.enable', {})
            driver.execute_cdp_cmd('Network.setBlockedURLs', {'urls': ['*.png','*.jpg','*.jpeg','*.gif','*.webp','*.svg','*.woff','*.woff2','*.ttf','*.otf','*.mp4','*.webm']})
    except Exception:
        pass

def selenium_scrape_insert(category: str, start_page: int, end_page: int, headless: bool = True, sleep: Tuple[float, float] = (0.8,1.6), visit_detail: bool = True) -> int:
    browser = os.getenv('BROWSER', 'chrome')
    binary = os.getenv('BROWSER_BINARY', None)
    driver = get_driver(browser=browser, headless=headless, binary_path=binary)

    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    WAIT_SEC = 8
    total = 0

    try:
        _block_media_if_chromium(driver)

        def wait(css: str):
            return WebDriverWait(driver, WAIT_SEC).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, css)))

        for p in range(start_page, end_page+1):
            listing_loaded = False
            for url in (PAGE_PATTERNS[0].format(base=SITE_BASE, path=CATEGORIES[category], n=p), PAGE_PATTERNS[1].format(base=SITE_BASE, path=CATEGORIES[category], n=p)):
                try:
                    driver.get(url)
                    wait('.ad__card-description a[href]')
                    listing_loaded = True; break
                except Exception:
                    continue
            if not listing_loaded: continue

            anchors = driver.find_elements(By.CSS_SELECTOR, '.ad__card-description a[href]')
            links = []
            for a in anchors:
                href = a.get_attribute('href') or ''
                if '/annonce/' in href: links.append(href)

            rows = []
            for href in links:
                title = price_raw = address_raw = image_url = None
                if visit_detail:
                    try:
                        driver.get(href)
                        WebDriverWait(driver, WAIT_SEC).until(EC.presence_of_element_located((By.CSS_SELECTOR, '.hide-on-med-and-down')))
                        sel = DETAIL[category]
                        try: title = driver.find_element(By.CSS_SELECTOR, sel['title']).get_attribute('textContent').strip()
                        except Exception: title = None
                        try: price_raw = driver.find_element(By.CSS_SELECTOR, sel['price']).get_attribute('textContent').strip()
                        except Exception:
                            m = PRICE.search(driver.page_source); price_raw = m.group(1) if m else None
                        try: address_raw = driver.find_element(By.CSS_SELECTOR, sel['addr']).get_attribute('textContent').strip()
                        except Exception: address_raw = None
                        try:
                            img_el = driver.find_element(By.CSS_SELECTOR, sel['img'])
                            for attr in ['data-src','data-lazy','data-original','src','srcset']:
                                v = img_el.get_attribute(attr)
                                if not v: continue
                                if attr == 'srcset' and ' ' in v: v = v.split(' ')[0]
                                image_url = v; break
                        except Exception: pass
                        if image_url and any(tok in image_url for tok in BAD_IMG_TOKENS): image_url = None
                        if image_url and not image_url.startswith('http'): image_url = SITE_BASE + image_url
                    except Exception: pass
                rows.append({'source':'coinafrique-sn','category':category,'title':title,'price_raw':price_raw,'address_raw':address_raw,'image_url':image_url,'link':href,'page':p})
                time.sleep(random.uniform(*sleep))

            insert_raw_many(rows); total += len(rows)
    finally:
        try: driver.quit()
        except Exception: pass
    return total
