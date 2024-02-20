package com.luxoft.lmd.springcloudstream;

import com.luxoft.lmd.springcloudstream.model.CreditCardTransaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import java.util.function.Consumer;

@Component
public class TransactionConsumer implements Consumer<Message<CreditCardTransaction>> {
	private final Logger logger = LoggerFactory.getLogger(getClass());

	@Override
	public void accept(Message<CreditCardTransaction> message) {
		// notice a lot of custom headers!
		logger.info("<- {}", message);
	}
}
