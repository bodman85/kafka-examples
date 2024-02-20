package com.luxoft.lmd.springcloudstream;

import com.luxoft.lmd.springcloudstream.model.CreditCardTransaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;

import java.util.function.Consumer;

@Component
public class TransactionConsumer implements Consumer<CreditCardTransaction> {
	private final Logger logger = LoggerFactory.getLogger(getClass());

	@Override public void accept(CreditCardTransaction creditCardTransaction) {
		try (MDC.MDCCloseable ignore = MDC.putCloseable("id", creditCardTransaction.id())) {
			logger.info("<- long processing {}", creditCardTransaction);
			Thread.sleep(1000);
			logger.info("processing finished");
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}
}
