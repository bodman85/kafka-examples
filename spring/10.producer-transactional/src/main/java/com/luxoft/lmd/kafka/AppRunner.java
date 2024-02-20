package com.luxoft.lmd.kafka;

import lombok.Getter;
import lombok.Setter;
import net.datafaker.Faker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.Random;
import java.util.UUID;

@Component
@Getter @Setter
public class AppRunner implements ApplicationRunner {
	@Autowired private KafkaTemplate<String, String> kafkaTemplate;

	private final Random random = new Random();
	private final Logger logger = LoggerFactory.getLogger(getClass());
	private final Faker faker = new Faker();

	@Override
	@Transactional // !!!
	public void run(ApplicationArguments args) throws Exception {
		var key = UUID.randomUUID().toString();
		kafkaTemplate.send("names", key, faker.funnyName().name()).get();

/*
		if (1 == 1)
			throw new RuntimeException("processing error");
*/

		kafkaTemplate.send("addresses", key, faker.address().fullAddress()).get();
	}
}
