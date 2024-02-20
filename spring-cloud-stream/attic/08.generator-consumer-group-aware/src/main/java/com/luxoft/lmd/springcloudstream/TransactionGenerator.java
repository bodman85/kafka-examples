package com.luxoft.lmd.springcloudstream;

import com.luxoft.lmd.springcloudstream.model.CreditCardTransaction;
import com.luxoft.lmd.springcloudstream.util.CreditCardTransactionBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.function.Supplier;

@Component
public class TransactionGenerator implements Supplier<CreditCardTransaction> {
	private final Logger logger = LoggerFactory.getLogger(getClass());
	private final CreditCardTransactionBuilder creditCardTransactionBuilder = new CreditCardTransactionBuilder();

	@Override
	public CreditCardTransaction get() {
		CreditCardTransaction creditCardTransaction =
			creditCardTransactionBuilder.createFakeTransaction();

		logger.info("-> {}", creditCardTransaction);
		return creditCardTransaction;
	}
}
