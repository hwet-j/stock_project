import os
import subprocess
import psycopg2
from dotenv import load_dotenv

# 환경 변수 로드
load_dotenv()

# PostgreSQL 연결 정보
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASS")
}

# HDFS 로그 파일 디렉터리
HDFS_CSV_LOG_DIR = os.getenv("HDFS_CSV_LOG_DIR", "logs")
HDFS_CSV_LOG_FILE = os.path.join(HDFS_CSV_LOG_DIR, "hdfs_csv_paths.log")


def read_hdfs_csv_log():
    """📂 HDFS CSV 로그 파일에서 파일 경로 목록을 읽어옴"""
    if not os.path.exists(HDFS_CSV_LOG_FILE):
        print(f"⚠️ 로그 파일이 존재하지 않습니다: {HDFS_CSV_LOG_FILE}")
        return []

    with open(HDFS_CSV_LOG_FILE, "r") as file:
        return [line.strip() for line in file if line.strip()]


def update_hdfs_csv_log(remaining_paths):
    """📝 처리 후 남은 HDFS CSV 경로 목록을 로그 파일에 다시 저장"""
    with open(HDFS_CSV_LOG_FILE, "w") as file:
        file.writelines(f"{path}\n" for path in remaining_paths)


def create_stock_data_table():
    """📊 stock_data 테이블 생성"""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
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
    cur.close()
    conn.close()


def create_temp_table():
    """📌 stock_data_temp 테이블 생성"""
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
    cur.close()
    conn.close()


def hdfs_to_temp_table(hdfs_path):
    """📥 HDFS에서 CSV 파일을 읽어 PostgreSQL 임시 테이블에 적재"""
    create_temp_table()

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    try:
        # HDFS에서 파일 내용을 직접 읽어 COPY 실행
        hdfs_cmd = ["hdfs", "dfs", "-cat", hdfs_path]
        process = subprocess.Popen(hdfs_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        copy_query = """
        COPY stock_data_temp (ticker, date, open, high, low, close, volume)
        FROM STDIN WITH CSV HEADER DELIMITER ',' QUOTE '"';
        """
        cur.copy_expert(sql=copy_query, file=process.stdout)

        conn.commit()
        print(f"✅ HDFS 데이터 적재 완료: {hdfs_path}")
        return True

    except Exception as e:
        print(f"❌ HDFS 데이터 적재 실패: {e}")
        return False

    finally:
        cur.close()
        conn.close()


def move_data_from_temp_to_main():
    """📤 stock_data_temp → stock_data 테이블로 데이터 이동"""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute("""
    INSERT INTO stock_data (ticker, date, open, high, low, close, volume)
    SELECT ticker, date, open, high, low, close, volume
    FROM stock_data_temp
    ON CONFLICT (ticker, date) DO NOTHING;
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("✅ 데이터 이동 완료")


def drop_temp_table():
    """📂 stock_data_temp 테이블 삭제"""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS stock_data_temp;")
    conn.commit()
    cur.close()
    conn.close()


def process_hdfs_csv_files():
    """📂 HDFS CSV 로그 파일을 읽어 모든 CSV를 처리"""
    hdfs_paths = read_hdfs_csv_log()
    if not hdfs_paths:
        print("⚠️ 처리할 HDFS CSV 파일이 없습니다.")
        return

    processed_paths = []

    for hdfs_path in hdfs_paths:
        print(f"📂 HDFS 파일 처리 중: {hdfs_path}")
        success = hdfs_to_temp_table(hdfs_path)
        if success:
            move_data_from_temp_to_main()
            drop_temp_table()
            processed_paths.append(hdfs_path)

    # 로그 파일 업데이트 (처리된 파일 제거)
    remaining_paths = [path for path in hdfs_paths if path not in processed_paths]
    update_hdfs_csv_log(remaining_paths)

    print("✅ 모든 HDFS CSV 파일 처리 완료")


if __name__ == "__main__":
    create_stock_data_table()
    process_hdfs_csv_files()
