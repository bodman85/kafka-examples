package com.luxoft.lmd.springcloudstream.service;

import com.luxoft.lmd.springcloudstream.model.CreditCardTransaction;

import java.util.Map;

public interface ContractorSpendingCalculator {
	Number fetchSpendings(String contractorId);

	Map<String, ? extends Number> fetchAll();

	void registerSpending(CreditCardTransaction transaction);
}
