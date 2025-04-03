import subprocess
import psycopg2
import os
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


def create_stock_data_table():
    """📊 stock_data 테이블 생성"""
    conn = None
    cur = None
    try:
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
    except Exception as e:
        print(f"❌ 테이블 생성 오류: {e}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def create_temp_table():
    """📌 stock_data_temp 테이블 생성"""
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
    except Exception as e:
        print(f"❌ 임시 테이블 생성 오류: {e}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def hdfs_to_temp_table(hdfs_path, target_table="stock_data_temp"):
    """📥 HDFS에서 CSV 파일을 읽어 PostgreSQL 임시 테이블에 적재"""
    create_temp_table()

    conn = None
    cur = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # HDFS에서 파일 내용을 직접 읽어 COPY 실행
        hdfs_cmd = ["hdfs", "dfs", "-cat", hdfs_path]
        process = subprocess.Popen(hdfs_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        copy_query = f"""
        COPY {target_table} (ticker, date, open, high, low, close, volume)
        FROM STDIN WITH CSV HEADER DELIMITER ',' QUOTE '"';
        """
        cur.copy_expert(sql=copy_query, file=process.stdout)

        conn.commit()
        print(f"✅ HDFS 데이터 적재 완료: {hdfs_path}")

    except Exception as e:
        print(f"❌ HDFS 데이터 적재 실패: {e}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def move_data_from_temp_to_main():
    """📤 stock_data_temp → stock_data 테이블로 데이터 이동"""
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute("""
        INSERT INTO stock_data (ticker, date, open, high, low, close, volume)
        SELECT ticker, date, open, high, low, close, volume
        FROM stock_data_temp
        ON CONFLICT (ticker, date) DO NOTHING;
        """)
        conn.commit()
        print("✅ 데이터 이동 완료")
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
        cur.execute("DROP TABLE IF EXISTS stock_data_temp;")
        conn.commit()
    except Exception as e:
        print(f"❌ 임시 테이블 삭제 실패: {e}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def process_hdfs_csv(hdfs_file_path):
    """📂 HDFS CSV 파일을 PostgreSQL에 적재"""
    if not hdfs_file_path:
        print("⚠️ HDFS 파일 경로가 필요합니다.")
        return

    print(f"📂 HDFS 파일 처리 중: {hdfs_file_path}")
    success = hdfs_to_temp_table(hdfs_file_path)
    if success:
        move_data_from_temp_to_main()
        drop_temp_table()
        print("✅ HDFS CSV 처리 완료")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="HDFS CSV 파일을 PostgreSQL에 적재하는 스크립트")
    parser.add_argument("hdfs_file", type=str, help="HDFS 파일 경로")
    args = parser.parse_args()

    create_stock_data_table()
    process_hdfs_csv(args.hdfs_file)
