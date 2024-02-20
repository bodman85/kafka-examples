package com.luxoft.lmd.kafka;

import net.datafaker.Faker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;


@Component
public class DataGenerator implements Runnable {
	@Autowired private KafkaTemplate<Object, Object> kafkaTemplate;
	private final Faker faker = new Faker();
	private final Logger logger = LoggerFactory.getLogger(getClass());

	@Scheduled(fixedDelay = 1000)
	@Override public void run() {
		String name = faker.name().name();
		logger.info("-> {}", name);
		kafkaTemplate.send(KafkaConfiguration.TOPIC_NAME, name);
	}
}
