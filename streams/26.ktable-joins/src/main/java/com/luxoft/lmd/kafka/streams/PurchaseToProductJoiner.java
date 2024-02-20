package com.luxoft.lmd.kafka.streams;

import org.apache.kafka.streams.kstream.ValueJoinerWithKey;

public class PurchaseToProductJoiner implements ValueJoinerWithKey<String, Purchase, Product, PurchaseEnriched> {
	@Override public PurchaseEnriched apply(String readOnlyKey, Purchase purchase, Product product) {
		return PurchaseEnriched.newBuilder()
			.setCustomerId(purchase.getCustomerId())
			.setDate(purchase.getDate())
			.setPrice(purchase.getPrice())
			.setQuantity(purchase.getQuantity())
			.setProductId(purchase.getProductId())
			.setProductType(product.getType()) // !!!
			.build();
	}
}
