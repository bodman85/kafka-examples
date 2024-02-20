package com.luxoft.lmd.springcloudstream.model;

import java.util.Date;

public record CreditCardTransaction(
	String id,
	String accountId,
	String customerName,
	String cardNumber,
	String cardType,
	Date date,
	Double amount
) {
}
