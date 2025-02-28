import yfinance as yf
import pandas as pd
import os 
from datetime import datetime, timedelta
import pandas_market_calendars as mcal
import time
from dotenv import load_dotenv
import psycopg2
from psycopg2 import sql

# .env file load
load_dotenv()


# PostgreSQL 연결 정보 설정
DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT"),
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD")
}

CSV_DIR = os.getenv("CSV_DIR")

"""
assert CSV_DIR, "CSV_DIR 환경 변수가 설정되지 않았습니다."
for key, value in DB_CONFIG.items():
    assert value, f"{key} 환경 변수가 설정되지 않았습니다."
"""

LOG_TABLE_NAME = "stock_data_log"

def create_log_table():
    """ 📑 로그 저장을 위한 테이블 생성 함수 """
    create_table_query = sql.SQL(f"""
    CREATE TABLE IF NOT EXISTS {LOG_TABLE_NAME} (
        id SERIAL PRIMARY KEY,
        execution_time TIMESTAMP NOT NULL,
        extraction_date DATE NOT NULL,
        tickers TEXT NOT NULL,
        step TEXT NOT NULL,
        status TEXT NOT NULL,
        message TEXT,
        duration_seconds DOUBLE PRECISION
    );
    """)

    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor() as cur:
            cur.execute(create_table_query)
            conn.commit()
            print(f"[INFO] 테이블 '{LOG_TABLE_NAME}' 생성 완료 또는 이미 존재합니다.")
    except Exception as e:
        print(f"[ERROR] 테이블 생성 실패: {e}")
    finally:
        if conn:
            conn.close()

def is_market_closed(date):
    nyse = mcal.get_calendar("NYSE")
    holidays = nyse.holidays().holidays
    is_weekend = date.weekday() in [5,6]

    return date in holidays or is_weekend



# 로그 기록 함수
def log_to_db(execution_time, extraction_date, tickers, step, status, message, duration_seconds):
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO stock_data_log 
                (execution_time, extraction_date, tickers, step, status, message, duration_seconds) 
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (execution_time, extraction_date, tickers, step, status, message, duration_seconds)
            )
            conn.commit()
            print(f"[INFO] 로그 저장 완료: {step} - {status}")
    except Exception as e:
        print(f"[ERROR] 로그 저장 실패: {e}")
    finally:
        if conn:
            conn.close()


def save_csv(data, extract_date, ticker=None):
    """ CSV 파일을 저장하고 로그를 남기는 함수 """
    try:
        start_time = datetime.now()  # 시작 시간 기록

        # 📅 날짜 기반 폴더 구조 생성
        date_folder = extract_date.strftime("%Y/%m/%d")
        save_folder = os.path.join(CSV_DIR, date_folder)
        os.makedirs(save_folder, exist_ok=True)
        # os.chmod(save_folder, 0o755)  # 권한 설정

        # 📄 파일명에 날짜 포함
        date_str = extract_date.strftime("%Y%m%d")
        file_path = ""

        # ✅ Ticker별 저장 여부에 따라 Step과 Message 설정
        if ticker:
            file_name = f"{ticker}_{date_str}.csv"
            step = "SAVE_CSV_TICKER"
            file_path = os.path.join(save_folder, file_name)
            message = f"{ticker} Data: {file_path} 저장 완료"
            ticker_info = ticker
        else:
            file_name = f"ALL_{date_str}.csv"
            step = "SAVE_CSV_ALL"
            file_path = os.path.join(save_folder, file_name)
            message = f"ALL Data: {file_path} 저장 완료"
            ticker_info = "ALL"


        # CSV 저장
        data.to_csv(file_path, index=False)
        # print(f"[INFO] CSV 저장 완료: {file_path}")

        # 저장 경로를 로그 파일에 기록
        log_file_path = os.getenv("CSV_LOG_PATH", "csv_files.log")
        os.makedirs(os.path.dirname(log_file_path), exist_ok=True)

        with open(log_file_path, "a") as log_file:
            log_file.write(file_path + "\n")

        duration_seconds = (datetime.now() - start_time).total_seconds()
        # 📝 로그 작성
        log_to_db(
            execution_time=datetime.now(),
            extraction_date=extract_date,
            tickers=ticker_info,
            step=step,
            status="SUCCESS",
            message=message,
            duration_seconds=duration_seconds
        )

        return file_path
    except Exception as e:
        # print(f"[ERROR] CSV 저장 실패: {e}")
        duration_seconds = (datetime.now() - start_time).total_seconds()

        log_to_db(datetime.now(),
                  extract_date,
                  ticker or "ALL",
                  step,
                  "FAIL",
                  f"CSV 저장 실패: {e}",
                  duration_seconds)
        return None



# 주식 데이터 가져오기
def fetch_stock_data(tickers, from_date, to_date):
    extract_date = datetime.strptime(from_date, "%Y-%m-%d")
    end_date = datetime.strptime(to_date, "%Y-%m-%d")

    log_to_db(execution_time=datetime.now(),
              extraction_date=extract_date,
              tickers=','.join(tickers),
              step="START",
              status="START",
              message="데이터 추출 시작",
              duration_seconds=0)

    while extract_date <= end_date:
        if is_market_closed(extract_date):
            log_to_db(execution_time=datetime.now(),
                      extraction_date=extract_date,
                      tickers="ALL",
                      step="FETCH_DATA",
                      status="SKIP",
                      message="휴장일",
                      duration_seconds=0)
            extract_date += timedelta(days=1)
            continue
        else:
            try:
                start_time = datetime.now()  # 데이터 수집 시작 시간

                stock_data = yf.download(tickers,
                                         start=extract_date,
                                         end=str(extract_date+timedelta(days=1)),
                                         group_by='ticker',
                                         auto_adjust=True)
                if stock_data.empty:
                    log_to_db(
                        execution_time = start_time,
                        extraction_date = extract_date,
                        tickers = "ALL",
                        step = "FETCH_DATA",
                        status = "FAIL",
                        message = "데이터 없음",
                        duration_seconds = (datetime.now() - start_time).total_seconds()
                    )
                else:
                    # 📂 전체 데이터 CSV 저장
                    all_file_path = save_csv(stock_data, extract_date)

                    #  🔁 ticker별 CSV 저장
                    for ticker in tickers:
                        if ticker in stock_data.columns.levels[0] and not stock_data[ticker].empty:  # 데이터가 있는 ticker만 저장
                            ticker_data = stock_data[ticker].reset_index()
                            save_csv(ticker_data, extract_date, ticker=ticker)
                        else:   # 데이터가 없는 ticker 로그 처리
                            log_to_db(
                                execution_time=start_time,
                                extraction_date=extract_date,
                                tickers=ticker,
                                step="SAVE_CSV_TICKER",
                                status="FAIL",
                                message=f"Ticker {ticker} 데이터 없음",
                                duration_seconds=(datetime.now() - start_time).total_seconds()
                            )
            except Exception as e:
                log_to_db(
                    execution_time = start_time,
                    extraction_date = extract_date,
                    tickers = "ALL",
                    step = "FETCH_DATA",
                    status = "FAIL",
                    message = f"데이터 수집 실패: {e}",
                    duration_seconds=(datetime.now() - start_time).total_seconds()
                )

            extract_date += timedelta(days=1)


if __name__ == "__main__":
    create_log_table()
    tickers = ["AAPL", "MSFT"]  # 예시 ticker 목록
    fetch_stock_data(tickers, "2023-01-01", "2023-01-10")