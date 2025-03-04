import argparse
import subprocess
import os
from datetime import datetime
from dotenv import load_dotenv
import psycopg2
from psycopg2 import sql
import csv

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

CSV_LOG_FILE = os.getenv("CSV_LOG_DIR") + "/csv_files.log"  # 로그 파일 경로


def create_stock_data_table():
    """ 📊 stock_data 테이블이 없으면 자동 생성하는 함수 """
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


def fix_csv_headers(input_file, output_file):
    """
    CSV 파일의 헤더에서 공백을 언더스코어(_)로 변경
    """
    with open(input_file, newline='', encoding='utf-8') as infile, open(output_file, "w", newline='', encoding='utf-8') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)

        # (1) 헤더 수정: 공백을 언더스코어(_)로 변경
        header = next(reader)
        new_header = [col.replace(" ", "_") for col in header]  # 공백 → "_"
        writer.writerow(new_header)

        # (2) 데이터 그대로 복사
        for row in reader:
            writer.writerow(row)

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
            # print(f"[INFO] 로그 저장 완료: {step} - {status}")
    except Exception as e:
        print(f"[ERROR] 로그 저장 실패: {e}")
    finally:
        if conn:
            conn.close()



def csv_to_db_pgfutter(csv_file, target_table="stock_data"):
    """ 📥 pgfutter를 이용하여 CSV 데이터를 PostgreSQL에 적재하는 함수 """
    conn = None
    schema = "public"
    table_name = target_table + '_temp'

    start_time = datetime.now()  # 시작 시간 기록


    file_name = os.path.basename(csv_file)
    file_name_without_ext = os.path.splitext(file_name)[0]
    ticker, date_str = file_name_without_ext.split("_")
    date_formatted = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"

    fixed_csv_file = csv_file.replace(".csv", "_fixed.csv")
    fix_csv_headers(csv_file, fixed_csv_file)

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # 환경 변수 설정
        """
        env = os.environ.copy()
        env["DB_NAME"] = DB_CONFIG["dbname"]
        env["DB_USER"] = DB_CONFIG["user"]
        env["DB_PASS"] = DB_CONFIG["password"]
        env["DB_HOST"] = DB_CONFIG["host"]
        env["DB_PORT"] = str(DB_CONFIG["port"])
        env["DB_SCHEMA"] = schema
        env["DB_TABLE"] = table_name

        # pgfutter 실행 명령어
        command = [
            "pgfutter", "csv",
            fixed_csv_file  # 삽입할 CSV 파일
        ]
        """

        # 환경 변수 설정 (pgfutter가 사용하는 PostgreSQL 환경 변수)
        os.environ["PGUSER"] = DB_CONFIG["user"]
        os.environ["PGPASSWORD"] = DB_CONFIG["password"]
        os.environ["PGHOST"] = DB_CONFIG["host"]
        os.environ["PGPORT"] = str(DB_CONFIG["port"])
        os.environ["PGDATABASE"] = DB_CONFIG["dbname"]

        # pgfutter 실행 명령어
        command = [
            "pgfutter", "csv",
            fixed_csv_file  # 삽입할 CSV 파일
        ]

        try:
            # result = subprocess.run(command, check=True, env=env, capture_output=True, text=True)
            result = subprocess.run(command, check=True, capture_output=True, text=True)

            print(f"[INFO] pgfutter 실행 완료 (stdout):\n{result.stdout}")  # stdout 전체 출력
            print(f"[INFO] pgfutter 오류 로그 (stderr):\n{result.stderr}")  # stderr 전체 출력

            cur.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public';")
            tables = cur.fetchall()

            print("[INFO] 현재 존재하는 테이블 목록:")
            for table in tables:
                print(f" - {table[0]}")

        except subprocess.CalledProcessError as e:
            print(f"[ERROR] pgfutter 실행 실패: {e}")
            log_to_db(start_time, date_formatted, ticker, f"LOAD_TO_DB", "ERROR",
                      f"{date_formatted}.{ticker} PGFUTTER EXECUTION ERROR", 0)

            return False

        # ✅ (2) 중복 데이터 제거 후, target_table로 이동
        cur.execute(f"""
                DELETE FROM {table_name} 
                WHERE (ticker, date::TEXT) IN (SELECT ticker, date::TEXT FROM {target_table});
            """)
        conn.commit()

        cur.execute(f"""
                INSERT INTO {target_table} (date, open, high, low, close, volume, dividends, stock_splits, ticker)
                SELECT 
                    date::DATE, 
                    NULLIF(REPLACE(open, '\r', ''), '')::NUMERIC, 
                    NULLIF(REPLACE(high, '\r', ''), '')::NUMERIC, 
                    NULLIF(REPLACE(low, '\r', ''), '')::NUMERIC, 
                    NULLIF(REPLACE(close, '\r', ''), '')::NUMERIC, 
                    NULLIF(REPLACE(volume, '\r', ''), '')::NUMERIC, 
                    REPLACE(ticker, '\r', '')
                FROM {table_name};
            """)
        conn.commit()

        print(f"[INFO] 데이터 `{target_table}`로 이동 완료")
        duration_seconds = (datetime.now() - start_time).total_seconds()
        log_to_db(start_time, date_formatted, ticker, f"LOAD_TO_DB", "SUCCESS", f"{date_formatted}.{ticker} LOAD TO DB SUCCESS", duration_seconds)

        # ✅ (3) 원본 테이블 삭제
        cur.execute(f"DROP TABLE {table_name};")
        conn.commit()
        # print(f"[INFO] 자동 생성된 테이블 `{table_name}` 삭제 완료")

        return True

    except subprocess.CalledProcessError as e:
        print(f"[Error] pgfutter 실행 실패: {e}")
        return False

    except Exception as e:
        print(f"[Error] 데이터베이스 작업 중 오류 발생: {e}")
        return False

    finally:
        if conn:
            conn.close()


def process_csv_files():
    """ 📂 로그 파일에서 CSV 파일 목록을 읽어 하나씩 처리한 후, 로그 파일 삭제 """

    # 1️⃣ CSV 로그 파일 존재 여부 확인
    if not os.path.exists(CSV_LOG_FILE):
        print(f"❌ CSV 로그 파일({CSV_LOG_FILE})이 존재하지 않습니다.")
        return

    # 2️⃣ 로그 파일에서 CSV 파일 목록 읽기
    with open(CSV_LOG_FILE, "r") as file:
        csv_files = [line.strip() for line in file.readlines() if line.strip()]

    if not csv_files:
        print("📂 적재할 CSV 파일이 없습니다.")
        return

    print(f"📂 총 {len(csv_files)}개의 CSV 파일을 처리합니다.")

    # 3️⃣ CSV 파일을 하나씩 데이터베이스에 적재
    for csv_file_path in csv_files:
        if os.path.exists(csv_file_path):
            # print(f"📄 처리 중: {csv_file_path}")
            csv_to_db_pgfutter(csv_file_path)
        else:
            print(f"⚠️ 파일을 찾을 수 없음: {csv_file_path}")

    # 4️⃣ 모든 CSV 파일 처리 후 로그 파일 삭제
    try:
        # os.remove(CSV_LOG_FILE)
        print(f"🗑️ 로그 파일 삭제 완료: {CSV_LOG_FILE}")
    except Exception as e:
        print(f"❌ 로그 파일 삭제 실패: {e}")

    print("✅ 모든 CSV 파일 처리 및 로그 파일 삭제 완료")


if __name__ == "__main__":
    create_stock_data_table()
    process_csv_files()
