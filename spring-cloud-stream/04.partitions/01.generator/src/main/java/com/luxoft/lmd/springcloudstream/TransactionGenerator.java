package com.luxoft.lmd.springcloudstream;

import com.luxoft.lmd.springcloudstream.model.CreditCardTransaction;
import com.luxoft.lmd.springcloudstream.util.CreditCardTransactionBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.function.Supplier;

@Component
public class TransactionGenerator implements Supplier<Message<CreditCardTransaction>> {
	private final Logger logger = LoggerFactory.getLogger(getClass());
	private final CreditCardTransactionBuilder creditCardTransactionBuilder = new CreditCardTransactionBuilder();

	@Override
	public Message<CreditCardTransaction> get() {
		CreditCardTransaction creditCardTransaction =
			creditCardTransactionBuilder.createFakeTransaction();

		Message<CreditCardTransaction> msg = MessageBuilder.createMessage(
			creditCardTransaction,
			new MessageHeaders(
				Map.of("partitionKey", creditCardTransaction.accountId())
			)
		);

		logger.info("-> {}", msg);
		return msg;
	}
}
