import argparse
import subprocess
import os
from datetime import datetime
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



def csv_to_db_pgfutter(csv_file_path, table_name="stock_data"):
    """ 📥 pgfutter를 이용하여 CSV 데이터를 PostgreSQL에 적재하는 함수 """
    try:
        start_time = datetime.now()
        schema = "public"
        temp_table = f"{table_name}_temp"

        # PostgreSQL 연결
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # 1️⃣ 임시 테이블 생성 (기존 stock_data 테이블과 동일한 구조)
        create_temp_table_query = sql.SQL(f"""
                    CREATE TABLE IF NOT EXISTS {temp_table} (LIKE {table_name} INCLUDING ALL);
                """)
        cur.execute(create_temp_table_query)
        conn.commit()

        # pgfutter 실행 명령어
        command = ["pgfutter", "csv", csv_file_path]
        result = subprocess.run(command, capture_output=True, text=True)

        if result.returncode != 0:
            raise Exception(f"pgfutter 적재 실패: {result.stderr}")

        # 3️⃣ 데이터 검증
        cur.execute(f"SELECT COUNT(*) FROM {temp_table};")
        temp_count = cur.fetchone()[0]

        if temp_count == 0:
            raise Exception("임시 테이블에 적재된 데이터가 없습니다.")

        # 4️⃣ 검증된 데이터를 stock_data 테이블로 이동
        insert_query = sql.SQL(f"""
                    INSERT INTO {table_name} SELECT * FROM {temp_table}
                    ON CONFLICT (ticker, date) DO NOTHING;
                """)
        cur.execute(insert_query)
        conn.commit()

        # 5️⃣ 임시 테이블 삭제
        cur.execute(f"DROP TABLE IF EXISTS {temp_table};")
        conn.commit()

        print(f"✅ {csv_file_path} 적재 성공")
        return True

    except Exception as e:
        # ❌ 실패 시 임시 테이블 삭제
        conn.rollback()
        cur.execute(f"DROP TABLE IF EXISTS {temp_table};")
        conn.commit()
        print(f"❌ {csv_file_path} 적재 실패: {e}")
        return False

    finally:
        if cur:
            cur.close()
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
            print(f"📄 처리 중: {csv_file_path}")
            csv_to_db_pgfutter(csv_file_path)
        else:
            print(f"⚠️ 파일을 찾을 수 없음: {csv_file_path}")

    # 4️⃣ 모든 CSV 파일 처리 후 로그 파일 삭제
    try:
        os.remove(CSV_LOG_FILE)
        print(f"🗑️ 로그 파일 삭제 완료: {CSV_LOG_FILE}")
    except Exception as e:
        print(f"❌ 로그 파일 삭제 실패: {e}")

    print("✅ 모든 CSV 파일 처리 및 로그 파일 삭제 완료")


if __name__ == "__main__":
    create_stock_data_table()
    process_csv_files()
