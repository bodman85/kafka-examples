package com.luxoft.lmd.springcloudstream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.util.function.Consumer;
import java.util.function.Function;

@SpringBootApplication
public class SCSMultipleOutputApp {
	private final static Logger logger = LoggerFactory.getLogger("main");

	public static void main(String[] args) {
		SpringApplication.run(SCSMultipleOutputApp.class, args);
	}

	@Bean
	public Function<Flux<Purchase>, Tuple2<Flux<Purchase>, Flux<Purchase>>> splitter() {
		return purchaseFlux -> {
			Flux<Purchase> coffeePurchasesFlux =
				purchaseFlux.filter(purchase -> purchase.type() == Purchase.Type.COFFEE);

			Flux<Purchase> electronicsPurchasesFlux =
				purchaseFlux.filter(purchase -> purchase.type() == Purchase.Type.ELECTRONICS);

			return Tuples.of(coffeePurchasesFlux, electronicsPurchasesFlux);
		};
	}

	@Bean
	public Consumer<Purchase> electronicsPurchasesConsumer() {
		return purchase -> logger.info("<- got electronics purchase: {}", purchase);
	}
}
