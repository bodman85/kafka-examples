package com.luxoft.lmd.kafka.model;

public record ProductRemoved(String cartId, String productId, int quantity) {

}
