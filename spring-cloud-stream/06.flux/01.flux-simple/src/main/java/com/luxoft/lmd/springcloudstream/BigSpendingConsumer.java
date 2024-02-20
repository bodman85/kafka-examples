package com.luxoft.lmd.springcloudstream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.function.Consumer;

@Component
public class BigSpendingConsumer implements Consumer<CustomerSpending> {
	private final Logger logger = LoggerFactory.getLogger(getClass());

	@Override
	public void accept(CustomerSpending bigCustomerSpending) {
		logger.info("<- {}", bigCustomerSpending);
	}
}
