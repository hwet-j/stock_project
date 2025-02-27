-- stock_data 테이블 생성 예시
CREATE DATABASE IF NOT EXISTS stock_db;
USE stock_db;

CREATE TABLE IF NOT EXISTS test_stock_data (
    ticker STRING,
    date STRING,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume INT
) STORED AS PARQUET;

