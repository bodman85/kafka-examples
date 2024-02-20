package com.luxoft.lmd.springcloudstream;

import net.datafaker.Faker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.function.Supplier;

@Component
public class DataProducer implements Supplier<String> {
	private int idx = 0;
	private final Faker faker = new Faker();

	private final Logger logger = LoggerFactory.getLogger(getClass());

	@Override public String get() {
		String payload = "[%d] %s".formatted(idx++, faker.funnyName().name());
		//logger.info("-> {}", payload);
		return payload;
	}
}
