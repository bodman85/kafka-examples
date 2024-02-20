package com.luxoft.lmd.kafka.streams;

import org.apache.kafka.streams.kstream.Aggregator;

class ProductPurchaseTotalsAggregator
	implements Aggregator<String, Purchase, ProductPurchaseTotals> {
	@Override
	public ProductPurchaseTotals apply(String key,
	                                   Purchase currentPurchase,
	                                   ProductPurchaseTotals aggregate) {
		var quantity =
			aggregate.getTotalQuantity() + currentPurchase.getQuantity();
		var totalValue =
			aggregate.getTotalValue() + currentPurchase.getPrice() * currentPurchase.getQuantity();

		return
			ProductPurchaseTotals.newBuilder()
				.setProductId(aggregate.getProductId())
				.setTotalQuantity(quantity)
				.setTotalValue(totalValue)
				.build();
	}
}
