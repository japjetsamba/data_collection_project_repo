from __future__ import annotations
import os
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.edge.service import Service as EdgeService
from selenium.webdriver.firefox.service import Service as GeckoService
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.edge.options import Options as EdgeOptions
from selenium.webdriver.firefox.options import Options as FirefoxOptions

def get_driver(browser: str | None = None, headless: bool = True, binary_path: str | None = None):
    """Creer un WebDriver en laissant Selenium Manager gerer la bonne version du driver."""
    browser = (browser or os.getenv('BROWSER', 'chrome')).lower()

    if browser in ('chrome', 'chromium'):
        options = ChromeOptions()
        if headless: options.add_argument('--headless=new')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--window-size=1600,1200')
        options.add_argument('--disable-extensions')
        options.add_argument('--blink-settings=imagesEnabled=false')
        if binary_path: options.binary_location = binary_path
        return webdriver.Chrome(service=ChromeService(), options=options)

    if browser == 'edge':
        options = EdgeOptions()
        if headless: options.add_argument('--headless=new')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--window-size=1600,1200')
        options.add_argument('--disable-extensions')
        options.add_argument('--blink-settings=imagesEnabled=false')
        if binary_path: options.binary_location = binary_path
        return webdriver.Edge(service=EdgeService(), options=options)

    if browser == 'firefox':
        options = FirefoxOptions()
        if headless: options.add_argument('-headless')
        if binary_path: options.binary_location = binary_path
        return webdriver.Firefox(service=GeckoService(), options=options)

    raise ValueError(f'Navigateur non supporte: {browser}')
