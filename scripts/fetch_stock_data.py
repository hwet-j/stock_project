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


def save_csv(data, extract_date, tickers, is_monthly=False):
    """ CSV 파일을 저장하고 로그를 남기는 함수 """
    start_time = datetime.now()  # 시작 시간 기록
    try:
        # 📅 날짜 기반 폴더 구조 생성
        date_folder_base = extract_date.replace("_", "/")[:7]  # "YYYY/MM"
        full_date_folder = extract_date.replace("_", "/")  # "YYYY/MM/DD"

        # ✅ 월별/티커별 저장
        if is_monthly:
            save_folder = os.path.join(CSV_DIR, date_folder_base, "date_data")
            file_name = f"ALL_DATA_{extract_date}.csv"
        else:
            save_folder = os.path.join(CSV_DIR, full_date_folder)
            file_name = f"TICKER_DATA_{tickers}_{extract_date}.csv"

        os.makedirs(save_folder, exist_ok=True)  # 폴더 생성

        file_path = os.path.join(save_folder, file_name)
        message = f"Data: {file_path} 저장 완료"

        # CSV 저장
        data.to_csv(file_path, index=False)

        # 저장 경로를 로그 파일에 기록 (전체 하나만)
        if is_monthly:
            log_file_path = CSV_LOG_DIR
            with open(log_file_path, "a") as log_file:
                log_file.write(file_path + "\n")

        duration_seconds = (datetime.now() - start_time).total_seconds()
        # 📝 로그 작성
        log_to_db(
            execution_time=datetime.now(),
            from_date=extract_date,
            to_date=extract_date,
            tickers=tickers,
            step="SAVE_CSV",
            status="SUCCESS",
            message=message,
            duration_seconds=duration_seconds
        )

        return file_path
    except Exception as e:
        duration_seconds = (datetime.now() - start_time).total_seconds()
        # 📝 로그 작성
        log_to_db(
            execution_time=datetime.now(),
            from_date=extract_date,
            to_date=extract_date,
            tickers=tickers,
            step="SAVE_CSV",
            status="FAIL",
            message=f"CSV 저장 실패: {e}",
            duration_seconds=duration_seconds
        )

        return None



# 주식 데이터 가져오기
def fetch_stock_data(tickers, from_date, to_date):
    ticker_list = ','.join(tickers)

    # ✅ 데이터 추출 시작 로그
    log_to_db(
        execution_time=datetime.now(),
        from_date=from_date,
        to_date=to_date,
        tickers=ticker_list,
        step="START",
        status="START",
        message="데이터 추출 시작",
        duration_seconds=0
    )

    start_time = datetime.now()
    retries = 3  # 최대 재시도 횟수

    for attempt in range(retries):
        try:
            # ✅ Ticker 데이터를 가져옴 (MultiIndex DataFrame)
            stock_data = yf.download(tickers, start=from_date, end=to_date, group_by='ticker')
            # print(stock_data.head(5))
            # ✅ 모든 데이터가 비어 있는지 확인
            if stock_data.empty:
                print("[WARN] 모든 데이터가 없음")
                log_to_db(
                    execution_time=start_time,
                    from_date=from_date,
                    to_date=to_date,
                    tickers=ticker_list,
                    step="FETCH_DATA",
                    status="FAIL",
                    message="모든 데이터 없음",
                    duration_seconds=(datetime.now() - start_time).total_seconds()
                )
                return

            valid_tickers = []  # ✅ 데이터가 있는 티커 리스트
            data_list = []      # ✅ 유효한 데이터 저장

            # ✅ 티커 목록 추출 (MultiIndex 구조에서 2번째 레벨 값 가져오기)
            ticker_in_column = stock_data.stack(level=0, future_stack=True).reset_index()

            for ticker in tickers:
                df_ticker = ticker_in_column[ticker_in_column["Ticker"] == ticker]
                if df_ticker[["Open", "High", "Low", "Close", "Volume"]].isna().all().all():
                    print(f"[WARN] {ticker} 데이터 없음")
                    log_to_db(
                        execution_time=start_time,
                        from_date=from_date,
                        to_date=to_date,
                        tickers=ticker,
                        step="FETCH_DATA",
                        status="FAIL",
                        message=str(e),
                        duration_seconds=(datetime.now() - start_time).total_seconds()
                    )
                else:
                    valid_tickers.append(ticker)  # ✅ 데이터 있는 티커만 추가
                    df_ticker.reset_index(inplace=True)

                    # ✅ 필요한 컬럼만 선택
                    df_ticker = df_ticker[['Date', 'Ticker', 'Close', 'High', 'Low', 'Open', 'Volume']]
                    data_list.append(df_ticker)


            if not data_list:
                print("[WARN] 모든 티커의 데이터가 없음")
                return

                # ✅ 모든 Ticker 데이터를 하나의 DataFrame으로 병합
            df_final = pd.concat(data_list).reset_index(drop=True)

            # ✅ 필요한 컬럼만 선택
            df_final = df_final[['Date', 'Ticker', 'Close', 'High', 'Low', 'Open', 'Volume']]

            # ✅ 날짜별로 나눠 저장
            for date, df_date in df_final.groupby("Date"):
                date_str = date.strftime("%Y_%m_%d")  # '2025-03-05' → '2025_03_05'
                save_csv(df_date, date_str, '_'.join(valid_tickers), is_monthly=True)

                for tick in valid_tickers:
                    ticker_data = df_date[df_date['Ticker'] == tick]
                    save_csv(ticker_data, date_str, tick, is_monthly=False)

            break  # 정상적으로 완료되면 루프 종료

        except Exception as e:
            print(f"[ERROR] 데이터 수집 실패: {e}")
            if attempt < retries - 1:
                logging.info(f"[INFO] 데이터 수집 실패, 재시도 중... (Attempt {attempt + 1}/{retries})")
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
    logging.info(f"[INFO] {from_date} ~ {to_date}")
    print(f"[INFO] {from_date} ~ {to_date}")

    fetch_stock_data(tickers, from_date, to_date)

