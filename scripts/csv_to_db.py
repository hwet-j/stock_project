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

        # 환경 변수 설정
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
            csv_file_path  # 삽입할 CSV 파일
        ]

        command = ["pgfutter", "csv", csv_file_path]
        result = subprocess.run(command, capture_output=True, text=True)

        if result.returncode != 0:
            raise Exception(f"pgfutter 적재 실패: {result.stderr}")

        # 3️⃣ 데이터 검증 (중복 제거, NULL 값 체크 등)
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

        # ✅ 성공 로그 저장
        """
        log_to_db(
            execution_time=datetime.now(),
            extraction_date=datetime.strptime(csv_file_path.split("_")[-1].split(".")[0], "%Y%m%d"),
            tickers=table_name,
            step="PGFUTTER_IMPORT",
            status="SUCCESS",
            message=f"CSV 파일 {csv_file_path} -> {table_name} 적재 완료",
            duration_seconds=(datetime.now() - start_time).total_seconds()
        )
        """
        return True

    except Exception as e:
        # ❌ 실패 시 임시 테이블 삭제
        conn.rollback()
        cur.execute(f"DROP TABLE IF EXISTS {temp_table};")
        conn.commit()
        """
        log_to_db(
            execution_time=datetime.now(),
            extraction_date=datetime.strptime(csv_file_path.split("_")[-1].split(".")[0], "%Y%m%d"),
            tickers=table_name,
            step="PGFUTTER_IMPORT",
            status="FAIL",
            message=f"pgfutter 실행 중 오류 발생: {e}",
            duration_seconds=(datetime.now() - start_time).total_seconds()
        )
        """
        return False

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
