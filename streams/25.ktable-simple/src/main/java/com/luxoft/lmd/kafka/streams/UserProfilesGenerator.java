package com.luxoft.lmd.kafka.streams;

import net.datafaker.Faker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Random;

@Component
public class UserProfilesGenerator {
	@Autowired private KafkaTemplate<Object, Object> kafkaTemplate;
	private final Logger logger = LoggerFactory.getLogger(getClass());

	private final Random random = new Random();
	private final Faker faker = new Faker();

	@Scheduled(fixedDelay = 1000)
	public void generate() {
		var id = String.valueOf(random.nextInt(10));

		var profile =
			UserProfile.newBuilder()
				.setId(id)
				.setEmail(faker.internet().emailAddress())
				.build();

		logger.info("-> {}", profile);
		kafkaTemplate.send("user.profiles.01", id, profile);
	}
}
