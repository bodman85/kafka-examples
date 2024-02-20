package com.luxoft.lmd.springcloudstream;

import com.luxoft.lmd.springcloudstream.model.CreditCardTransaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.function.Consumer;

@SpringBootApplication
public class SCSConsumerApp {
	private final Logger logger = LoggerFactory.getLogger(getClass());

	@Bean
	public Consumer<CreditCardTransaction> transactionConsumer() {
		return transaction -> logger.info("<- {}", transaction);
	}

	public static void main(String[] args) {
		SpringApplication.run(SCSConsumerApp.class, args);
	}
}
