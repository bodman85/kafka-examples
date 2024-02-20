package com.luxoft.lmd.kafka;

import net.datafaker.Faker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Component
public class DataGenerator implements Runnable {
	@Autowired private KafkaTemplate<String, PageClickInfo> kafkaTemplate;

	private final Logger logger = LoggerFactory.getLogger(getClass());
	private final Faker faker = new Faker();

	@Scheduled(fixedDelay = 1000)
	public void run() {
		PageClickInfo info = new PageClickInfo(
			faker.internet().url(),
			LocalDateTime.now(),
			faker.superhero().name()
		);

		logger.info("-> {}", info);
		kafkaTemplate.sendDefault(info);
	}
}
