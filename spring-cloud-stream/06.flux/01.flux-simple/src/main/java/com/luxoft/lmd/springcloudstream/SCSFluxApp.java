package com.luxoft.lmd.springcloudstream;

import com.luxoft.lmd.springcloudstream.model.CreditCardTransaction;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import reactor.core.publisher.Flux;

import java.util.function.Function;

@SpringBootApplication
public class SCSFluxApp {
	public static void main(String[] args) {
		SpringApplication.run(SCSFluxApp.class, args);
	}

	@Bean
	public Function<Flux<CreditCardTransaction>, Flux<CreditCardTransaction>> filterLargeSpendings() {
		return creditCardTransactionFlux ->
			creditCardTransactionFlux
				.filter(tx -> tx.amount() > 1000.0)
				.log();
	}

	@Bean
	public Function<Flux<CreditCardTransaction>, Flux<CustomerSpending>> transform() {
		return creditCardTransactionFlux ->
			creditCardTransactionFlux
				.map(tx -> new CustomerSpending(tx.customerName(), tx.amount()))
				.log();
	}
}
