package com.luxoft.lmd.springcloudstream.service;

import com.google.common.util.concurrent.AtomicDouble;
import com.luxoft.lmd.springcloudstream.model.CreditCardTransaction;

import java.util.HashMap;
import java.util.Map;

public class ContractorSpendingCalculatorImpl implements ContractorSpendingCalculator {
	private final Map<String, AtomicDouble> spendings = new HashMap<>();

	@Override
	public Number fetchSpendings(String contractorId) {
		return spendings.get(contractorId);
	}

	@Override
	public Map<String, ? extends Number> fetchAll() {
		return spendings;
	}

	@Override
	public void registerSpending(CreditCardTransaction transaction) {
		AtomicDouble currentSpending =
			spendings.computeIfAbsent(transaction.accountId(), s -> new AtomicDouble(0));
		currentSpending.addAndGet(transaction.amount());
	}
}
