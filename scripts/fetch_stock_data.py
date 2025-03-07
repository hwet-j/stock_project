import argparse
import logging
import time

import pandas as pd
import yfinance as yf
import os
from datetime import datetime, timedelta
import pandas_market_calendars as mcal
from dotenv import load_dotenv
import psycopg2
from psycopg2 import sql



# .env file load
load_dotenv()

# PostgreSQL 연결 정보 설정
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASS")
}

CSV_DIR = os.getenv("CSV_DIR")

CSV_LOG_DIR = os.getenv("CSV_LOG_DIR")

"""
assert CSV_DIR, "CSV_DIR 환경 변수가 설정되지 않았습니다."
for key, value in DB_CONFIG.items():
    assert value, f"{key} 환경 변수가 설정되지 않았습니다."
"""

LOG_TABLE_NAME = "stock_data_log"

TICKER_PATH = os.getenv("TICKER_FILE_PATH")

def create_log_table():
    """ 📑 로그 저장을 위한 테이블 생성 함수 """
    create_table_query = sql.SQL(f"""
    CREATE TABLE IF NOT EXISTS {LOG_TABLE_NAME} (
        id SERIAL PRIMARY KEY,
        execution_time TIMESTAMP NOT NULL,
        from_date DATE NOT NULL,
        to_date DATE NOT NULL,
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
            # print(f"[INFO] 테이블 '{LOG_TABLE_NAME}' 생성 완료 또는 이미 존재합니다.")
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

def load_tickers_from_file(file_path: str) -> list:
    """📂 파일에서 Ticker 목록을 불러오는 함수"""
    tickers = []
    try:
        with open(file_path, "r") as f:
            tickers = [line.strip() for line in f if line.strip()]  # 빈 줄 제외
        print(f"[INFO] Ticker {len(tickers)}개 로드 완료")
    except FileNotFoundError:
        print(f"[ERROR] 파일을 찾을 수 없습니다: {file_path}")
    except Exception as e:
        print(f"[ERROR] Ticker 파일 로드 실패: {e}")
    return tickers

def get_default_dates() -> tuple:
    """🗓️ 기본 날짜를 전날로 설정"""
    today = datetime.now().strftime("%Y-%m-%d")
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    return yesterday, today


# 로그 기록 함수
def log_to_db(execution_time, from_date, to_date, tickers, step, status, message, duration_seconds):
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO stock_data_log 
                (execution_time, from_date, to_date, tickers, step, status, message, duration_seconds) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (execution_time, from_date, to_date, tickers, step, status, message, duration_seconds)
            )
            conn.commit()
            # print(f"[INFO] 로그 저장 완료: {step} - {status}")
    except Exception as e:
        print(f"[ERROR] 로그 저장 실패: {e}")
    finally:
        if conn:
            conn.close()


def save_csv(data, extract_date, ticker_list):
    """ CSV 파일을 저장하고 로그를 남기는 함수 """
    start_time = datetime.now()  # 시작 시간 기록
    try:
        # 📅 날짜 기반 폴더 구조 생성
        date_folder = extract_date.strftime("%Y/%m")
        save_folder = os.path.join(CSV_DIR, date_folder)
        os.makedirs(save_folder, exist_ok=True)
        # os.chmod(save_folder, 0o755)  # 권한 설정


        # ✅ Ticker별 저장 여부에 따라 Step과 Message 설정
        file_name = f"stock_data_{extract_date}.csv"
        step = "SAVE_CSV_TICKER"
        file_path = os.path.join(save_folder, file_name)
        message = f"Data: {file_path} 저장 완료"


        # CSV 저장
        data.to_csv(file_path, index=False)
        # print(f"[INFO] CSV 저장 완료: {file_path}")

        # 저장 경로를 로그 파일에 기록
        log_file_path = CSV_LOG_DIR

        with open(log_file_path, "a") as log_file:
            log_file.write(file_path + "\n")

        duration_seconds = (datetime.now() - start_time).total_seconds()
        # 📝 로그 작성
        log_to_db(
            execution_time=datetime.now(),
            from_date=extract_date,
            to_date=extract_date,
            tickers=ticker_list,
            step="SAVE_CSV",
            status="SUCCESS",
            message=message,
            duration_seconds=duration_seconds
        )

        return file_path
    except Exception as e:
        print(f"[ERROR] CSV 저장 실패: {e}")
        duration_seconds = (datetime.now() - start_time).total_seconds()

        log_to_db(execution_time=datetime.now(),
                  from_date=extract_date,
                  to_date=extract_date,
                  tickers=ticker_list,
                  step="SAVE_CSV",
                  status="FAIL",
                  message=f"CSV 저장 실패: {e}",
                  duration_seconds=duration_seconds)
        return None




# 주식 데이터 가져오기
def fetch_stock_data(tickers, from_date, to_date):
    ticker_list = ','.join(tickers)
    extraction_date = f"{from_date} ~ {to_date}"
    # 데이터 추출 시작
    log_to_db(execution_time=datetime.now(),
              from_date=from_date,
              to_date=to_date,
              tickers=ticker_list,
              step="START",
              status="START",
              message="데이터 추출 시작",
              duration_seconds=0)

    start_time = datetime.now()  # 데이터 수집 시작 시간

    # 데이터 다운로드에 대한 재시도 로직 추가
    retries = 3
    for attempt in range(retries):
        try:
            # ticker, from_date, to_date 해당하는 데이터 수집
            stock_data = yf.download(tickers, start=from_date, end=to_date)
            if stock_data.empty:
                print("[WARN] 데이터 없음")

                log_to_db(
                    execution_time=start_time,
                    from_date=from_date,
                    to_date=to_date,
                    tickers=ticker_list,
                    step="FETCH_DATA",
                    status="FAIL",
                    message="데이터 없음",
                    duration_seconds=(datetime.now() - start_time).total_seconds()
                )
            else:
                #  🔁 ticker 컬럼 생성 및 ticker별 데이터 분리
                data_list = []
                for ticker in tickers:
                    df_ticker = stock_data.xs(ticker, axis=1, level=1)  # 특정 ticker 데이터 추출
                    df_ticker.insert(0, "Ticker", ticker)  # Ticker 컬럼 추가
                    data_list.append(df_ticker)  # 리스트에 추가

                # ✅ 모든 Ticker 데이터를 하나의 DataFrame으로 병합
                df_final = pd.concat(data_list).reset_index()

                # ✅ 필요한 컬럼만 선택
                df_final = df_final[['Date', 'Ticker', 'Close', 'High', 'Low', 'Open', 'Volume']]

                # ✅ 날짜별로 나눠 저장 (파일명: stock_data_YYYY_MM_DD.csv)
                for date, df_date in df_final.groupby("Date"):
                    date_str = date.strftime("%Y_%m_%d")  # '2025-03-05' → '2025_03_05'
                    save_csv(df_date, date, ticker_list)

                # 데이터 다운 및 저장 완료시 루프 탈출
                break
        except Exception as e:
            print(f"[ERROR] 데이터 수집 실패: {e}")
            if attempt < retries - 1:
                logging.info(f"[INFO] {ticker} 데이터 수집 실패, 재시도 중... (Attempt {attempt + 1}/{retries})")
                time.sleep(5)  # 재시도 간의 딜레이
            else:
                log_to_db(
                    execution_time=start_time,
                    from_date=from_date,
                    to_date=to_date,
                    tickers=ticker_list,
                    step="FETCH_DATA",
                    status="FAIL",
                    message=f"데이터 수집 실패: {e}",
                    duration_seconds=(datetime.now() - start_time).total_seconds()
                )


if __name__ == "__main__":
    create_log_table()
    tickers = load_tickers_from_file(TICKER_PATH)

    # 🆕 커맨드라인 인자 처리
    parser = argparse.ArgumentParser(description="주식 데이터 수집기")
    parser.add_argument("from_date", type=str, nargs="?", default=None, help="시작 날짜 (YYYY-MM-DD)")
    parser.add_argument("to_date", type=str, nargs="?", default=None, help="종료 날짜 (YYYY-MM-DD)")

    args = parser.parse_args()

    # 날짜 설정
    if args.from_date and args.to_date:
        from_date = args.from_date
        to_date = args.to_date
    else:
        from_date, to_date = get_default_dates()

    fetch_stock_data(tickers, from_date, to_date)

