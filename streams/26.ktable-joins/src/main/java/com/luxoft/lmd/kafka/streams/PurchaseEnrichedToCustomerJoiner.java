package com.luxoft.lmd.kafka.streams;

import org.apache.kafka.streams.kstream.ValueJoinerWithKey;

public class PurchaseEnrichedToCustomerJoiner implements ValueJoinerWithKey<String, PurchaseEnriched, Customer, PurchaseEnriched> {
	@Override public PurchaseEnriched apply(String readOnlyKey, PurchaseEnriched purchase, Customer contractor) {
		return PurchaseEnriched.newBuilder(purchase)
			.setCountryCode(contractor.getCountryCode())
			.build();
	}
}
