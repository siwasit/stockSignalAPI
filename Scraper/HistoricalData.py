from tvDatafeed import TvDatafeed, Interval
import pandas as pd
import json
import asyncio
import logging
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_historical_data(symbol):
    tv = TvDatafeed()

    matched_row = None

    with open('StockData.csv', newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if row['symbol'].endswith(symbol):
                matched_row = row
                break
    bars_count = 1000  # approx 1 year daily bars
    # print(matched_row['symbol'])
    exchange, stock_symbol = matched_row['symbol'].split(':', 1)
    # Fetch historical data: daily bars, for about 1 year
    historical_data = tv.get_hist(
        symbol=stock_symbol,
        exchange=exchange,
        interval=Interval.in_4_hour,
        n_bars=bars_count
    )

    historical_data = historical_data.copy()
    for col in historical_data.select_dtypes(include=['datetime64[ns]', 'datetime64[ns, UTC]']).columns:
        historical_data[col] = historical_data[col].dt.strftime('%Y-%m-%dT%H:%M:%S')

    json_data = historical_data.reset_index().to_dict(orient='records')
    return json_data

TV_USER = "siwasit2546"
TV_PASS = "fightfortriam1#"
tv = TvDatafeed(username=TV_USER, password=TV_PASS)

def fetch_one_stock(row):
    full_symbol = row['symbol']
    thai_name = row['ThaiCompanyName']
    eng_name = row['EngCompanyName']
    logo = row['logo']

    logger.info(f"🔄 กำลังดึงข้อมูล: {full_symbol} ({eng_name})")

    try:
        exchange, symbol = full_symbol.split(':', 1)
        historical_data = tv.get_hist(
            symbol=symbol,
            exchange=exchange,
            interval=Interval.in_4_hour,
            n_bars=2
        )

        if historical_data is None or historical_data.empty or len(historical_data) < 2:
            logger.warning(f"⚠️ {symbol} ไม่มีข้อมูลเพียงพอ")
            return {
                "stockSymbol": symbol,
                "stockMarket": exchange,
                "ThaiCompanyName": thai_name,
                "EngCompanyName": eng_name,
                "logo": logo,
                "error": "No sufficient data"
            }

        last_close = historical_data['close'].iloc[-1]
        previous_close = historical_data['close'].iloc[-2]
        pct_change = ((last_close - previous_close) / previous_close) * 100

        logger.info(f"✅ {symbol}: ราคา {last_close} | เปลี่ยนแปลง {pct_change:.2f}%")

        return {
            "stockSymbol": symbol,
            "stockMarket": exchange,
            "ThaiCompanyName": thai_name,
            "companyName": eng_name,
            "logo": logo,
            "stockPrice": round(float(last_close), 2),
            "changePct": round(float(pct_change), 2)
        }

    except Exception as e:
        logger.error(f"❌ {symbol} เกิดข้อผิดพลาด: {e}")
        return {
            "stockSymbol": symbol,
            "stockMarket": exchange,
            "ThaiCompanyName": thai_name,
            "EngCompanyName": eng_name,
            "logo": logo,
            "error": str(e)
        }

def get_stock_price(symbol_list):
    df = pd.read_csv("StockData.csv")

    # กรอง dataframe ให้เหลือแค่ symbol ที่ต้องการ
    df_filtered = df[df['symbol'].str.upper().isin([s.upper() for s in symbol_list])]

    if df_filtered.empty:
        logger.warning("⚠️ ไม่มีข้อมูลบริษัทสำหรับ symbol ที่ระบุ")
        return []

    result = []

    logger.info(f"เริ่มดึงข้อมูล {len(df_filtered)} ตัวที่ระบุเข้ามา ด้วย ThreadPoolExecutor max_workers=5")

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(fetch_one_stock, row): row['symbol'] for _, row in df_filtered.iterrows()}

        for future in as_completed(futures):
            res = future.result()
            result.append(res)

    logger.info("✅ ดึงข้อมูลครบแล้ว")
    return result

async def async_fetch_one_stock(row):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, fetch_one_stock, row)

async def event_generator(symbol_list):
    df = pd.read_csv("StockData.csv")
    df_filtered = df[df['symbol'].str.upper().isin([s.upper() for s in symbol_list])]

    if df_filtered.empty:
        yield f"data: {json.dumps({'error': 'No symbols found'})}\n\n"
        return

    batch_size = 2
    rows = list(df_filtered.iterrows())

    for i in range(0, len(rows), batch_size):
        batch_rows = rows[i:i + batch_size]

        tasks = [async_fetch_one_stock(row) for _, row in batch_rows]
        results = await asyncio.gather(*tasks)  # ดึงทีละ 2 ตัวพร้อมกัน

        for res in results:
            yield f"data: {json.dumps(res)}\n\n"

    # Keep connection alive
    while True:
        await asyncio.sleep(15)
        yield ":\n\n"  # SSE comment keep-alive

def get_cron_stock_price(symbol_list, max_retries=1000):
    import time

    df = pd.read_csv("StockData.csv")
    df_filtered = df[df['symbol'].str.upper().isin([s.upper() for s in symbol_list])]

    if df_filtered.empty:
        logger.warning("⚠️ ไม่มีข้อมูลบริษัทสำหรับ symbol ที่ระบุ")
        return [], []

    result = []
    failed_symbols = []

    logger.info(f"เริ่มดึงข้อมูล {len(df_filtered)} ตัวที่ระบุเข้ามา ด้วย ThreadPoolExecutor max_workers=5")

    for attempt in range(1, max_retries + 1):
        current_result = []
        current_failed = []

        logger.info(f"📦 เริ่มรอบดึงข้อมูล Attempt #{attempt}")

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {
                executor.submit(fetch_one_stock, row): row['symbol']
                for _, row in df_filtered.iterrows()
            }

            for future in as_completed(futures):
                symbol = futures[future]
                try:
                    res = future.result()

                    # ตรวจสอบว่า res มี error หรือข้อมูลไม่สมบูรณ์
                    if isinstance(res, dict) and res.get("error"):
                        logger.warning(f"❌ ข้อมูล {symbol} ไม่สมบูรณ์ (error field): {res['error']}")
                        current_failed.append(symbol)
                    else:
                        current_result.append(res)

                except Exception as e:
                    logger.warning(f"❌ ดึงข้อมูล {symbol} ไม่สำเร็จใน Attempt #{attempt}: {e}")
                    current_failed.append(symbol)

        result.extend(current_result)
        logger.info(f"✅ Attempt #{attempt}: สำเร็จ {len(current_result)} ตัว | ล้มเหลว {len(current_failed)} ตัว")

        # ถ้าไม่มีตัวที่ล้มเหลว -> ออกจาก Loop
        if not current_failed:
            break

        # เตรียมตัววนใหม่เฉพาะตัวที่ fail
        df_filtered = df[df['symbol'].str.upper().isin([s.upper() for s in current_failed])]
        failed_symbols = current_failed

        time.sleep(1)  # Optional: พักสักเล็กน้อยก่อน Retry รอบถัดไป

    return result, failed_symbols

