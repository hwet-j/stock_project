package com.hwet.search_system.controller;

import com.hwet.search_system.model.StockData;
import com.hwet.search_system.service.StockDataService;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;
import java.util.List;

@Controller
@RequestMapping("/stock")
public class StockDataController {
    private final StockDataService stockDataService;

    public StockDataController(StockDataService stockDataService) {
        this.stockDataService = stockDataService;
    }

    // ✅ 1. 검색 페이지 렌더링 (초기 화면)
    @GetMapping("/search")
    public String showSearchPage(Model model) {
        // 드롭다운에 사용할 ticker 목록을 전달
        List<String> tickers = stockDataService.getAllTickers();
        model.addAttribute("tickers", tickers);
        return "search";  // search.html 템플릿 반환
    }

    // ✅ 2. 검색 요청 처리
    @PostMapping("/search")
    public String searchStockData(
            @RequestParam(value="ticker", required = false) String ticker,
            @RequestParam(value="date", required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate date,
            Model model) {

//        System.out.println("=== 검색 요청 ===");
//        System.out.println("ticker: " + ticker);
//        System.out.println("date: " + date);

        List<StockData> searchResults;

        if (ticker != null && !ticker.isEmpty() && date != null) {
            searchResults = stockDataService.findByTickerAndDate(ticker, date);
        } else if (ticker != null && !ticker.isEmpty()) {
            searchResults = stockDataService.findByTicker(ticker);
        } else if (date != null) {
            searchResults = stockDataService.findByDate(date);
        } else {
            model.addAttribute("error", "검색어를 입력하거나 날짜를 선택하세요.");
            return "search";
        }

        // 🔥 검색 결과 디버깅
//        System.out.println("검색 결과 개수: " + searchResults.size());
//        for (StockData stock : searchResults) {
//            System.out.println("검색 결과: " + stock.getTicker() + " | " + stock.getDate());
//        }

        model.addAttribute("searchResults", searchResults);
        model.addAttribute("tickers", stockDataService.getAllTickers());

        return "search";  // search.html 템플릿 반환
    }

    // ✅ 3. 드롭다운에서 사용할 ticker 목록을 JSON으로 반환 (AJAX 요청 처리)
    @GetMapping("/tickers")
    @ResponseBody
    public List<String> getTickers() {
        return stockDataService.getAllTickers();
    }
}
