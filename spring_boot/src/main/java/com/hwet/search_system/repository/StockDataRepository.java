package com.hwet.search_system.repository;

import com.hwet.search_system.model.StockData;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.util.List;

@Repository
public interface StockDataRepository extends JpaRepository<StockData, Long> {

    // ✅ DISTINCT 값을 가져오는 올바른 방법
    @Query("SELECT DISTINCT s.ticker FROM StockData s")
    List<String> findDistinctTickers();

    List<StockData> findByTicker(String ticker);
    List<StockData> findByDate(LocalDate date);
    List<StockData> findByTickerAndDate(String ticker, LocalDate date);
}
