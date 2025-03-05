import argparse
import subprocess
import os
from datetime import datetime
from dotenv import load_dotenv
import psycopg2
import csv

# .env 파일 로드
load_dotenv()

# PostgreSQL 연결 정보
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASS")
}

CSV_LOG_PATH = os.getenv("CSV_LOG_DIR")

TICKER_PATH = os.getenv("TICKER_FILE_PATH")

def create_stock_data_table():
    """📊 stock_data 테이블 생성 (없으면 생성)"""
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        create_table_query = """
        CREATE TABLE IF NOT EXISTS stock_data (
            id SERIAL PRIMARY KEY,
            ticker TEXT NOT NULL,
            date DATE NOT NULL,
            open NUMERIC,
            high NUMERIC,
            low NUMERIC,
            close NUMERIC,
            volume BIGINT,
            UNIQUE (ticker, date)
        );
        """
        cur.execute(create_table_query)
        conn.commit()
        print("✅ stock_data 테이블이 확인되었습니다.")

    except Exception as e:
        print(f"❌ 테이블 생성 오류: {e}")

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def create_temp_table():
    """📌 stock_data_temp 테이블 생성 (없으면 생성)"""
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS stock_data_temp (
                ticker TEXT,
                date DATE,
                open NUMERIC,
                high NUMERIC,
                low NUMERIC,
                close NUMERIC,
                volume BIGINT
            );
        """)
        conn.commit()
        print("✅ stock_data_temp 테이블이 확인되었습니다.")

    except Exception as e:
        print(f"❌ 테이블 생성 오류: {e}")

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def csv_to_temp_table(csv_file, target_table="stock_data_temp"):
    """📥 psql COPY 명령어를 이용하여 CSV 데이터를 PostgreSQL에 적재"""
    if not os.path.exists(csv_file):
        print(f"❌ CSV 파일이 존재하지 않습니다: {csv_file}")
        return False

    # 파일 이름에서 ticker와 날짜 추출
    file_name = os.path.basename(csv_file)
    file_name_without_ext = os.path.splitext(file_name)[0]
    print("파일명 : ", file_name)

    try:
        ticker, date_str = file_name_without_ext.split("_")
        date_formatted = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
    except ValueError:
        print(f"❌ 파일명에서 ticker와 날짜를 추출할 수 없습니다: {file_name}")
        return False

    conn = None
    cur = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # COPY 명령어를 사용하여 CSV 데이터를 테이블에 적재
        copy_query = f"""
        COPY {target_table} (date, open, high, low, close, volume, ticker)
        FROM STDIN WITH CSV HEADER DELIMITER ',' QUOTE '"';
        """

        # 파일에서 데이터를 읽어 COPY 명령어 실행
        with open(csv_file, "r", encoding="utf-8") as f:
            content = f.read()
            print(content)
            cur.copy_expert(sql=copy_query, file=f)

        conn.commit()
        print(f"✅ {csv_file} 데이터가 {target_table} 테이블에 성공적으로 적재되었습니다.")

    except Exception as e:
        print(f"❌ CSV 적재 실패: {e}")
        return False

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
    return True


def move_data_from_temp_to_main():
    """📤 stock_data_temp 테이블에서 stock_data 테이블로 데이터 이동"""
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # 임시 테이블에서 실제 테이블로 데이터 이동
        move_data_query = """
        INSERT INTO stock_data (ticker, date, open, high, low, close, volume)
        SELECT ticker, date, open, high, low, close, volume
        FROM stock_data_temp
        ON CONFLICT (ticker, date) DO NOTHING;
        """

        cur.execute(move_data_query)
        conn.commit()
        print("✅ 임시 테이블에서 실제 테이블로 데이터가 성공적으로 이동되었습니다.")

    except Exception as e:
        print(f"❌ 데이터 이동 실패: {e}")

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def drop_temp_table():
    """📂 stock_data_temp 테이블 삭제"""
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # 임시 테이블 삭제
        drop_table_query = "DROP TABLE IF EXISTS stock_data_temp;"
        cur.execute(drop_table_query)
        conn.commit()
        print("✅ 임시 테이블이 삭제되었습니다.")

    except Exception as e:
        print(f"❌ 임시 테이블 삭제 실패: {e}")

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def process_csv_files():
    """📂 로그 파일에서 CSV 목록을 읽어 처리"""
    if not os.path.exists(CSV_LOG_PATH):
        print(f"❌ CSV 로그 디렉토리가 존재하지 않습니다: {CSV_LOG_PATH}")
        return
    CSV_LOG_FILE = CSV_LOG_PATH + "/csv_files.log"
    with open(CSV_LOG_FILE, "r") as file:
        csv_files = [line.strip() for line in file.readlines() if line.strip()]

    if not csv_files:
        print("📂 적재할 CSV 파일이 없습니다.")
        return

    print(f"📂 총 {len(csv_files)}개의 CSV 파일을 처리합니다.")

    for csv_file_path in csv_files:
        if os.path.exists(csv_file_path):
            # Step 1: 임시 테이블에 CSV 파일 적재
            success = csv_to_temp_table(csv_file_path)
            if success:
                print(f"✅ {csv_file_path} 임시 테이블에 데이터가 적재되었습니다.")

                # Step 2: 임시 테이블에서 실제 테이블로 데이터 이동
                move_data_from_temp_to_main()

                # Step 3: 임시 테이블 삭제
                drop_temp_table()
                print(f"✅ {csv_file_path} 처리 완료")
        else:
            print(f"⚠️ 파일을 찾을 수 없음: {csv_file_path}")

    print("✅ 모든 CSV 파일 처리 완료")


if __name__ == "__main__":
    create_stock_data_table()
    create_temp_table()
    process_csv_files()
