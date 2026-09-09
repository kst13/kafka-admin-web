package com.osstem.kafkaadmin.metrics;

import java.time.Duration;

// 시계열 범위: 창·step·rate 창은 스펙 표 그대로
public enum SeriesRange {
    H1("1h", Duration.ofHours(1), Duration.ofSeconds(30), "2m"),
    H6("6h", Duration.ofHours(6), Duration.ofMinutes(1), "5m"),
    H24("24h", Duration.ofHours(24), Duration.ofMinutes(5), "10m"),
    D7("7d", Duration.ofDays(7), Duration.ofMinutes(30), "1h");

    private final String label;
    private final Duration window;
    private final Duration step;
    private final String rateWindow;

    SeriesRange(String label, Duration window, Duration step, String rateWindow) {
        this.label = label; this.window = window; this.step = step; this.rateWindow = rateWindow;
    }

    public String label() { return label; }
    public Duration window() { return window; }
    public Duration step() { return step; }
    public String rateWindow() { return rateWindow; }

    public static SeriesRange parse(String s) {
        for (SeriesRange r : values()) if (r.label.equals(s)) return r;
        throw new IllegalArgumentException("지원하지 않는 range 입니다: " + s);
    }
}
