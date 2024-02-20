package com.luxoft.lmd.springcloudstream;

import net.datafaker.Faker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

@SpringBootApplication
public class SCSFunctionPipingApp {
	public final static Logger LOGGER = LoggerFactory.getLogger("main");
	private final static Faker faker = new Faker();

	public static void main(String[] args) {
		SpringApplication.run(SCSFunctionPipingApp.class, args);
	}

	@Bean
	public Supplier<String> namesGenerator() {
		return () -> faker.funnyName().name();
	}

	@Bean
	public Function<String, String> uppercase() {
		return name -> name.toUpperCase();
	}

	@Bean
	public Function<String, String> reverse() {
		return name -> new StringBuilder(name).reverse().toString();
	}

	@Bean
	public Consumer<String> namesConsumer() {
		return name -> LOGGER.info("<- {}", name);
	}
}
