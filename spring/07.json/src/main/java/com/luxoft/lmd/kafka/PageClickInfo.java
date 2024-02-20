package com.luxoft.lmd.kafka;

import java.time.LocalDateTime;

public record PageClickInfo(String url, LocalDateTime date, String username) {
}
