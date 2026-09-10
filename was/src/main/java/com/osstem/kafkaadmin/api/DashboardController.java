package com.osstem.kafkaadmin.api;

import com.osstem.kafkaadmin.monitor.DashboardService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class DashboardController {
    private final DashboardService dashboard;
    public DashboardController(DashboardService dashboard) { this.dashboard = dashboard; }
    @GetMapping("/api/dashboard")
    public DashboardService.Dashboard dashboard() { return dashboard.snapshot(); }
}
