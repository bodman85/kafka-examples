package com.luxoft.lmd.kafka.model;

import java.time.LocalDateTime;

public record ShoppingCartCreated(String id, LocalDateTime dateTime, String userName) {
}
