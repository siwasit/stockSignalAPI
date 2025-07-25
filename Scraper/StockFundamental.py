from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import json
import time
import re
import requests

def return_json_from_html(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    pre_tag = soup.find('pre')
    if pre_tag:
        json_text = pre_tag.get_text()
        return json.loads(json_text)
    else:
        return {}

# def scrape_stock_data(symbol):
#     chrome_options = Options()
#     chrome_options.add_argument("--headless=new")
#     chrome_options.add_argument("--disable-gpu")
#     chrome_options.add_argument("--no-sandbox")

#     service = Service("chromedriver-win64/chromedriver.exe")
#     driver = webdriver.Chrome(service=service, options=chrome_options)

#     try:
#         # print(f"กำลังดึงข้อมูล: {symbol}")

#         driver.get(f'https://www.set.or.th/th/market/product/stock/quote/{symbol}/price')
#         # time.sleep(1)

#         driver.get(f'https://www.set.or.th/api/set/stock/{symbol}/highlight-data?lang=th')
#         # time.sleep(0.5)
#         data_highlight = return_json_from_html(driver.page_source)

#         driver.get(f'https://www.set.or.th/api/set/company/{symbol}/profile?lang=th')
#         # time.sleep(0.5)
#         data_profile = return_json_from_html(driver.page_source)

#         driver.get(f'https://www.set.or.th/th/market/product/stock/quote/{symbol}/company-profile/board-of-directors')
#         driver.get(f'https://www.set.or.th/api/set/company/{symbol}/board-of-director?lang=th')
#         # time.sleep(0.5)
#         data_board = return_json_from_html(driver.page_source)

#         stock_data = {
#             'symbol': symbol,
#             'highlight_data': data_highlight,
#             'profile_data': data_profile,
#             'board_of_director': data_board
#         }

#         # print(f"เสร็จสิ้น {symbol}")
#         return stock_data

#     finally:
#         driver.quit()

def is_data_invalid(data):
    return data is None or data == {} or data == []

def scrape_stock_data(symbol, max_retries=3):
    for attempt in range(1, max_retries + 1):
        chrome_options = Options()
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")

        service = Service("chromedriver-win64/chromedriver.exe")
        driver = webdriver.Chrome(service=service, options=chrome_options)

        try:
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

            # ตรวจสอบว่า field สำคัญว่างหรือไม่
            if is_data_invalid(data_highlight) or is_data_invalid(data_profile) or is_data_invalid(data_board):
                print(f"[{symbol}] Attempt {attempt}: Incomplete data:")
                # print("  Highlight:", data_highlight)
                # print("  Profile:", data_profile)
                # print("  Board:", data_board)
                time.sleep(1)
                continue

            return {
                'symbol': symbol,
                'highlight_data': data_highlight,
                'profile_data': data_profile,
                'board_of_director': data_board
            }

        except Exception as e:
            print(f"[{symbol}] Attempt {attempt} failed with error: {e}")
            time.sleep(2)  # รอ 2 วินาทีแล้วลองใหม่
        finally:
            driver.quit()

    print(f"[{symbol}] Failed after {max_retries} attempts.")
    return None

def parse_value_string(value_str):
    # ล้าง control characters
    # cleaned = ''.join(c for c in value_str if not unicodedata.category(c).startswith('C'))

    # แยกเป็นกลุ่ม เช่น 149.91, B, USD
    parts = re.findall(r'\d+\.\d+|\d+|[A-Za-z]+', value_str)

    result = {}

    for part in parts:
        if part.replace('.', '', 1).isdigit():
            result['value'] = float(part)
        elif len(part) == 1:
            result['prefix'] = part  # เช่น B, M
        elif len(part) > 1:
            result['currency'] = part  # เช่น USD

    return result

def trading_view_stock_data(symbol):
    url = f"https://www.tradingview.com/symbols/{symbol}/"  # ใช้ symbol เช่น NYSE-NEE

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"
    }

    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        print("❌ Failed to fetch page:", response.status_code)
        return {}

    soup = BeautifulSoup(response.text, "html.parser")

    key_stats_div = soup.find("div", attrs={
        "data-cms-base-widget": "true",
        "data-container-name": "key-stats-id"
    })

    company_stats_div = soup.find("div", attrs={
        "data-cms-base-widget": "true",
        "data-container-name": "company-info-id"
    })

    company_profile = {}

    def extract_stat_blocks(div, parse_values=True):
        if not div:
            return
        
        container_div = div.find("div", class_="container-RUwl8xXG")
        if not container_div:
            return
        
        stat_blocks = container_div.find_all("div", class_="block-QCJM7wcY")
        for block in stat_blocks:
            stat_label = block.find("div", class_="apply-overflow-tooltip label-QCJM7wcY")
            stat_value = block.find("div", class_="apply-overflow-tooltip value-QCJM7wcY")

            label_text = stat_label.get_text(strip=True) if stat_label else "(ไม่พบ label)"
            value_text = stat_value.get_text(strip=True) if stat_value else "(ไม่พบ value)"

            if parse_values:
                company_profile[label_text] = parse_value_string(value_text)
            else:
                company_profile[label_text] = value_text

    # 🔍 Block 1: key-stats-id (parse structured values)
    extract_stat_blocks(key_stats_div, parse_values=True)

    # 🔍 Block 2: company-info-id (keep as plain text)
    extract_stat_blocks(company_stats_div, parse_values=False)

    return company_profile