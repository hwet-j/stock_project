import argparse
import subprocess
import os
import io
from dotenv import load_dotenv
import psycopg2
from hdfs import InsecureClient

# .env 파일 로드
load_dotenv()

# PostgreSQL 연결 정보
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASS"),
}

CSV_LOG_FILE = os.getenv("HDFS_CSV_LOG_DIR")  # HDFS 로그 파일 경로
# HDFS 연결 정보 설정
HDFS_URL = os.getenv("HDFS_URL")
HDFS_USER = os.getenv("HDFS_USER")
HDFS_DIR = os.getenv("HDFS_DIR")
HDFS_CSV_LOG_DIR = os.getenv("HDFS_CSV_LOG_DIR")

client = InsecureClient(HDFS_URL, user=HDFS_USER)

# 환경 변수 확인

def create_stock_data_table():
    """📊 stock_data 테이블 생성 (없으면 생성)"""
    try:
        with psycopg2.connect(**DB_CONFIG) as conn, conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS stock_data (
                    id BIGSERIAL PRIMARY KEY,
                    ticker TEXT NOT NULL,
                    date DATE NOT NULL,
                    open NUMERIC,
                    high NUMERIC,
                    low NUMERIC,
                    close NUMERIC,
                    volume BIGINT,
                    UNIQUE (ticker, date)
                );
            """)
            conn.commit()
    except Exception as e:
        print(f"❌ 테이블 생성 오류: {e}")


def create_temp_table():
    """📌 stock_data_temp 테이블 생성 (없으면 생성)"""
    try:
        with psycopg2.connect(**DB_CONFIG) as conn, conn.cursor() as cur:
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
    except Exception as e:
        print(f"❌ 임시 테이블 생성 오류: {e}")


def csv_to_temp_table(hdfs_csv_file, target_table="stock_data_temp"):
    """📥 HDFS에서 CSV 데이터를 읽어 PostgreSQL에 적재"""
    try:
        # HDFS에서 CSV 읽기
        with client.read(hdfs_csv_file, encoding='utf-8') as reader:
            csv_data = io.StringIO(reader.read())

        # PostgreSQL 연결 및 적재
        with psycopg2.connect(**DB_CONFIG) as conn, conn.cursor() as cur:
            create_temp_table()
            copy_query = f"""
            COPY {target_table} (date, ticker, close, high, low, open, volume)
            FROM STDIN WITH CSV HEADER DELIMITER ',' QUOTE '"';
            """
            cur.copy_expert(sql=copy_query, file=csv_data)
            conn.commit()

        print(f"✅ HDFS 파일 적재 성공: {hdfs_csv_file}")
        return True

    except Exception as e:
        print(f"❌ CSV 적재 실패: {e}")
        import traceback
        traceback.print_exc()  # 전체 에러 스택 출력
        return False



def move_data_from_temp_to_main():
    """📤 stock_data_temp 테이블에서 stock_data 테이블로 데이터 이동"""
    try:
        with psycopg2.connect(**DB_CONFIG) as conn, conn.cursor() as cur:
            cur.execute("""
                INSERT INTO stock_data (ticker, date, open, high, low, close, volume)
                SELECT ticker, date, open, high, low, close, volume
                FROM stock_data_temp
                ON CONFLICT (ticker, date) DO NOTHING;
            """)
            conn.commit()
    except Exception as e:
        print(f"❌ 데이터 이동 실패: {e}")


def drop_temp_table():
    """📂 stock_data_temp 테이블 삭제"""
    try:
        with psycopg2.connect(**DB_CONFIG) as conn, conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS stock_data_temp;")
            conn.commit()
    except Exception as e:
        print(f"❌ 임시 테이블 삭제 실패: {e}")


def hdfs_file_exists(hdfs_path):
    """HDFS 파일 존재 여부 확인"""
    try:
        if client.status(hdfs_path, strict=False):
            print(f"✅ 파일 존재함: {hdfs_path}")
            return 1
        else:
            print(f"❌ 파일 없음: {hdfs_path}")
            return 0
    except Exception as e:
        print("오류 발생:", e)
        return 0




def process_csv_files(csv_file_path=None):
    """📂 로그 파일에서 CSV 목록을 읽어 처리"""
    if csv_file_path:
        # 단일 CSV 파일 처리
        if hdfs_file_exists(csv_file_path):
            success = csv_to_temp_table(csv_file_path)
            if success:
                move_data_from_temp_to_main()
                drop_temp_table()
        else:
            print(f"⚠️ HDFS 파일을 찾을 수 없음1: {csv_file_path}")
    else:
        if not os.path.exists(CSV_LOG_FILE):
            print("📂 CSV 로그 파일이 없습니다.")
            return

        with open(CSV_LOG_FILE, "r") as file:
            csv_files = [line.strip() for line in file.readlines() if line.strip()]

        if not csv_files:
            print("📂 적재할 CSV 파일이 없습니다.")
            return

        print(f"📂 총 {len(csv_files)}개의 CSV 파일을 처리합니다.")

        for csv_file in csv_files:
            if hdfs_file_exists(csv_file):
                success = csv_to_temp_table(csv_file)
                if success:
                    move_data_from_temp_to_main()
                    drop_temp_table()
            else:
                print(f"⚠️ HDFS 파일을 찾을 수 없음2: {csv_file}")

        print("✅ 모든 CSV 파일 처리 완료")

        try:
            os.remove(CSV_LOG_FILE)
            print("🗑️ 로그 파일 삭제 완료")
        except Exception as e:
            print(f"⚠️ 로그 파일 삭제 실패: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="HDFS에서 CSV 파일을 PostgreSQL에 적재하는 스크립트")
    parser.add_argument("csv_file", type=str, help="처리할 HDFS CSV 파일 경로", nargs="?")

    args = parser.parse_args()

    create_stock_data_table()

    if args.csv_file:
        process_csv_files(args.csv_file)
    else:
        process_csv_files()
