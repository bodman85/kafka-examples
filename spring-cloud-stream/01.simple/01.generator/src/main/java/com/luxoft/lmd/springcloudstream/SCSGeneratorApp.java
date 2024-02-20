package com.luxoft.lmd.springcloudstream;

import com.luxoft.lmd.springcloudstream.model.CreditCardTransaction;
import com.luxoft.lmd.springcloudstream.util.CreditCardTransactionBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.function.Supplier;

@SpringBootApplication
public class SCSGeneratorApp {
	private final Logger logger = LoggerFactory.getLogger(getClass());

	private final CreditCardTransactionBuilder creditCardTransactionBuilder =
		new CreditCardTransactionBuilder();

	public static void main(String[] args) {
		SpringApplication.run(SCSGeneratorApp.class, args);
	}

	@Bean
	public Supplier<CreditCardTransaction> transactionGenerator() {
		return () -> {
			CreditCardTransaction creditCardTransaction =
				creditCardTransactionBuilder.createFakeTransaction();

			logger.info("-> {}", creditCardTransaction);
			return creditCardTransaction;
		};
	}
}
