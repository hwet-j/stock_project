import yfinance as yf
import pandas as pd
import os 
from datetime import datetime, timedelta
import pandas_market_calendars as mcal
import time
from dotenv import load_dotenv
import psycopg2

# .env file load
load_dotenv()


# PostgreSQL 연결 정보 불러오기
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")




def is_market_closed(date):

    nyse = mcal.get_calendar("NYSE")
    holidays = nyse.holidays().holidays

    is_weekend = date.weekday() in [5,6]

    return date in holidays or is_weekend



def connect_postgresql():
    """PostgreSQL 데이터베이스에 연결"""
    try:
        connection = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        print("[INFO] PostgreSQL에 성공적으로 연결되었습니다.")
        return connection
    except Exception as e:
        print(f"[ERROR] PostgreSQL 연결 실패: {e}")
        return None


# 연결 테스트
if __name__ == "__main__":
    conn = connect_postgresql()
    if conn:
        conn.close()
        
