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
	private final Logger logger = LoggerFactory.getLogger(getClass());
	@Autowired
	private ContractorSpendingCalculator contractorSpendingCalculator;

	@Override
	public void accept(CreditCardTransaction tx) {
		try (MDC.MDCCloseable ignore = MDC.putCloseable("tx", tx.id())) {
			logger.info("<- {}", tx);
			contractorSpendingCalculator.registerSpending(tx);
		}
	}
}
