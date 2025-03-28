package com.hwet.search_system.controller;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;

import java.time.LocalDateTime;

@Controller
public class MainController {

    @GetMapping("/")
    public String mainPage(Model model) {
        // 현재 날짜와 시간을 모델에 추가
        model.addAttribute("currentDate", LocalDateTime.now());
        return "index";  // index.html 템플릿을 반환
    }
}
