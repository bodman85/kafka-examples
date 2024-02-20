package com.luxoft.lmd.springcloudstream;

import com.luxoft.lmd.springcloudstream.model.CreditCardTransaction;
import com.luxoft.lmd.springcloudstream.service.ContractorSpendingCalculator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.function.Consumer;

@Component
public class TransactionConsumer implements Consumer<CreditCardTransaction> {
	@Autowired
	private ContractorSpendingCalculator contractorSpendingCalculator;
	private final Logger logger = LoggerFactory.getLogger(getClass());

	@Override
	public void accept(CreditCardTransaction creditCardTransaction) {
		try (MDC.MDCCloseable ignore = MDC.putCloseable("id", creditCardTransaction.id())) {
			logger.info("<- analytics consumer: {}", creditCardTransaction);
			contractorSpendingCalculator.registerSpending(creditCardTransaction);
		}
	}
}
