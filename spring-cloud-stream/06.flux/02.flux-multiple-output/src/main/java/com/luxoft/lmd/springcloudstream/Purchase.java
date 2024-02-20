package com.luxoft.lmd.springcloudstream;

public record Purchase(
	String id,
	String customerId,
	Type type,
	String productName,
	double amount
) {
	public enum Type {
		COFFEE, CLOTHING, ELECTRONICS
	}
}
