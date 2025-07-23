from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import json
import time

def return_json_from_html(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    pre_tag = soup.find('pre')
    if pre_tag:
        json_text = pre_tag.get_text()
        return json.loads(json_text)
    else:
        return {}

def scrape_stock_data(symbol):
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")

    service = Service("chromedriver-win64/chromedriver.exe")
    driver = webdriver.Chrome(service=service, options=chrome_options)

    try:
        # print(f"กำลังดึงข้อมูล: {symbol}")

        driver.get(f'https://www.set.or.th/th/market/product/stock/quote/{symbol}/price')
        # time.sleep(1)

        driver.get(f'https://www.set.or.th/api/set/stock/{symbol}/highlight-data?lang=th')
        # time.sleep(0.5)
        data_highlight = return_json_from_html(driver.page_source)

        driver.get(f'https://www.set.or.th/api/set/company/{symbol}/profile?lang=th')
        # time.sleep(0.5)
        data_profile = return_json_from_html(driver.page_source)

        driver.get(f'https://www.set.or.th/th/market/product/stock/quote/{symbol}/company-profile/board-of-directors')
        driver.get(f'https://www.set.or.th/api/set/company/{symbol}/board-of-director?lang=th')
        # time.sleep(0.5)
        data_board = return_json_from_html(driver.page_source)

        stock_data = {
            'symbol': symbol,
            'highlight_data': data_highlight,
            'profile_data': data_profile,
            'board_of_director': data_board
        }

        # print(f"เสร็จสิ้น {symbol}")
        return stock_data

    finally:
        driver.quit()
