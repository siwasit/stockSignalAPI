from fastapi import FastAPI, HTTPException, Request, status
from fastapi.responses import StreamingResponse, JSONResponse
import pandas as pd
from Scraper.StockFundamental import scrape_stock_data, trading_view_stock_data
from Scraper.HistoricalData import get_historical_data, get_stock_price, event_generator, get_cron_stock_price
import logging
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import csv, json

#uvicorn main:app --reload --port 3007

app = FastAPI()
scheduler = BackgroundScheduler()
STORAGE_FOLDER = "./storage"
os.makedirs(STORAGE_FOLDER, exist_ok=True)

origins = [
    "http://localhost:3006",
    "http://127.0.0.1:3006",
    # ถ้าต้องการอนุญาตหลายที่ ใส่เพิ่มที่นี่
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # หรือระบุ http://localhost:3006 ชัดเจน
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# โหลดข้อมูล CSV ตอนเริ่มเซิร์ฟเวอร์
# df = pd.read_csv("StockData.csv")

# ตั้งค่า logger
logger = logging.getLogger("uvicorn.error")
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_symbols_from_csv():
    symbols = []
    csv_file = "StockData.csv"
    with open(csv_file, mode="r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        symbols = [row["symbol"] for row in reader if row.get("symbol")]
    return symbols

def fetch_stock_data_routine():
    try:
        # symbols = [
        #     "ADVANC", "AOT", "AWC", "BANPU", "BBL", "BCP", "BDMS", "BEM", "BH", "BJC",
        #     "BTS", "CBG", "CCET", "COM7", "CPALL", "CPF", "CPN", "CRC", "DELTA", "EGCO",
        #     "GPSC", "GULF", "HMPRO", "IVL", "KBANK", "KKP", "KTB", "KTC", "LH", "MINT",
        #     "MTC", "OR", "OSP", "PTT", "PTTEP", "PTTGC", "RATCH", "SCB", "SCC", "SCGP",
        #     "TCAP", "TIDLOR", "TISCO", "TLI", "TOP", "TRUE", "TTB", "TU", "VGI", "WHA"
        # ]

        symbols = load_symbols_from_csv()

        result, failed_symbols = get_cron_stock_price(symbols, max_retries=3)

        if failed_symbols:
            logger.warning(f"❌ ยังมี {len(failed_symbols)} ตัวที่ดึงข้อมูลไม่สำเร็จหลัง retry: {failed_symbols}")

        if not result:
            logger.warning("No stock data fetched.")
            return

        filename = 'StockData.json'
        filepath = os.path.join(STORAGE_FOLDER, filename)

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=4)

        logger.info(f"📊 บันทึกข้อมูลหุ้นทั้งหมด {len(result)} ตัวลงไฟล์ {filename}")

    except Exception as e:
        logger.error(f"Error in stock data routine: {e}")


# เริ่ม Scheduler เมื่อแอปเปิดใช้งาน
@app.on_event("startup")
def start_scheduler():
    # 1️⃣ ดึงข้อมูลทันทีเมื่อแอปเริ่มทำงาน
    logger.info("Fetching stock data immediately at startup...")
    fetch_stock_data_routine()

    # 2️⃣ ค่อยเริ่ม routine ทำงานทุก 5 นาทีตามปกติ
    scheduler.add_job(fetch_stock_data_routine, "interval", hours=4)
    scheduler.start()
    logger.info("Scheduler started.")

@app.on_event("shutdown")
def shutdown_scheduler():
    scheduler.shutdown()
    logger.info("Scheduler stopped.")

@app.get("/")
async def hello_world():
    return {"message": "This is G2 StockSignal Project API any issue please contact Punt Web dev"}

@app.get("/CompanyData/{symbol}")
async def get_live_stock_data(symbol: str):
    try:
        data = scrape_stock_data(symbol)
        if not data:
            raise HTTPException(status_code=404, detail=f"No live stock data found for symbol '{symbol}'")
        return data
    except Exception as e:
        logger.error(f"Error fetching live stock data for {symbol}: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@app.get("/CompanyProfile/{symbol}")
async def trading_view_scraper(symbol: str):
    try:
        matched_row = None
        with open('StockData.csv', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                if row['symbol'].endswith(symbol):
                    matched_row = row
                    break
        
        stock_symbol = matched_row['symbol'].replace(':', '-')
        data = trading_view_stock_data(stock_symbol)
        if not data:
            raise HTTPException(status_code=404, detail=f"No live stock data found for symbol '{symbol}'")
        return data
    except Exception as e:
        logger.error(f"Error fetching live stock data for {symbol}: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@app.get("/getHistData/{symbol}")
async def get_stock_historical_data(symbol: str):
    try:
        data = get_historical_data(symbol)
        if not data:
            raise HTTPException(status_code=404, detail=f"No historical data found for symbol '{symbol}'")
        return data
    except Exception as e:
        logger.error(f"Error fetching historical data for {symbol}: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

# @app.get("/StockData/{symbol_list}")
# async def get_favorite_stocks(symbol_list: str):
#     try:
#         # แปลง symbol_list เป็น list เช่น "PTT,CPALL,SCB" -> ['PTT', 'CPALL', 'SCB']
#         symbols = [s.strip() for s in symbol_list.split(",") if s.strip()]

#         if not symbols:
#             raise HTTPException(status_code=400, detail="No symbols provided")

#         result = get_stock_price(symbols)

#         if not result:
#             raise HTTPException(status_code=404, detail="No stock data available")

#         return {"data": result}

#     except Exception as e:
#         logger.error(f"Error fetching favorite stocks: {e}")
#         raise HTTPException(status_code=500, detail="Internal Server Error")

@app.get("/StockData")
async def get_all_stock_data():
    file_path = os.path.join(STORAGE_FOLDER, 'StockData.json')

    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="Data file not found")

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)  # data เป็น list of dict

        if not data:
            raise HTTPException(status_code=404, detail="No stock data available")

        return JSONResponse(content={"data": data})

    except json.JSONDecodeError:
        raise HTTPException(status_code=500, detail="Data file is corrupted")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {e}")
    
@app.get("/streamStockPrice")
async def stream_stock_price(symbols: str):
    symbol_list = symbols.split(",")
    return StreamingResponse(event_generator(symbol_list), media_type="text/event-stream")

# จัดการ error ทั่วไปไม่ถูกจับใน route
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": exc.detail},
    )

@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unexpected error: {exc}")
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"error": "Internal Server Error"},
    )