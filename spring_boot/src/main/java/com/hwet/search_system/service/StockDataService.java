package com.hwet.search_system.service;

import com.hwet.search_system.model.StockData;
import com.hwet.search_system.repository.StockDataRepository;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.List;

@Service
public class StockDataService {
    private final StockDataRepository stockDataRepository;

    public StockDataService(StockDataRepository stockDataRepository) {
        this.stockDataRepository = stockDataRepository;
    }

    public List<StockData> findByTickerAndDate(String ticker, LocalDate date) {
        return stockDataRepository.findByTickerAndDate(ticker, date);
    }

    public List<StockData> findByTicker(String ticker) {
        return stockDataRepository.findByTicker(ticker);
    }

    public List<StockData> findByDate(LocalDate date) {
        return stockDataRepository.findByDate(date);
    }

    public List<String> getAllTickers() {
        return stockDataRepository.findDistinctTickers();
    }
}
