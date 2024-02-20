package com.luxoft.lmd.kafka.model;

public record ProductAdded(String cartId, String productId, int quantity, double price) {

}
