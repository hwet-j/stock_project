import argparse
import subprocess
import os
from datetime import datetime, timedelta
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

CSV_LOG_DIR = os.getenv("CSV_LOG_DIR")



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
    """ CSV_LOG_DIR에 있는 모든 CSV 파일을 처리한 후, 로그 파일 삭제 """

    CSV_LOG_DIR_FILES = CSV_LOG_DIR + "/csv_files.log"


    # CSV_LOG_DIR에서 파일 목록 가져오기
    csv_files = sorted([f for f in os.listdir(CSV_LOG_DIR_FILES) if f.endswith(".csv")])

    if not csv_files:
        print("📂 적재할 CSV 파일이 없습니다.")
        return

    print(f"📂 총 {len(csv_files)}개의 CSV 파일을 처리합니다.")

    for csv_file in csv_files:
        csv_file_path = os.path.join(CSV_LOG_DIR, csv_file)
        print(f"📄 처리 중: {csv_file_path}")
        csv_to_db_pgfutter(csv_file_path)

    # 모든 CSV 파일 처리 후 로그 파일 삭제
    for csv_file in csv_files:
        csv_file_path = os.path.join(CSV_LOG_DIR, csv_file)
        try:
            os.remove(csv_file_path)
            print(f"🗑️ 삭제 완료: {csv_file_path}")
        except Exception as e:
            print(f"❌ 파일 삭제 실패: {csv_file_path} - {e}")

    print("✅ 모든 CSV 파일 처리 및 로그 파일 삭제 완료")


if __name__ == "__main__":
    process_csv_files()