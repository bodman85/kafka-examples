package com.luxoft.lmd.kafka;

import com.luxoft.lmd.kafka.domain.UserRegistration;
import net.datafaker.Faker;
import org.apache.avro.specific.SpecificRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class DataGenerator implements Runnable {
	@Autowired private KafkaTemplate<String, SpecificRecord> kafkaTemplate;
	@Autowired private AppConfig config;
	private final Faker faker = new Faker();

	@Scheduled(fixedDelay = 1000)
	public void run() {
		UserRegistration registration = UserRegistration.newBuilder()
			.setId(UUID.randomUUID().toString())
			.setFirstName(faker.name().firstName())
			.setLastName(faker.name().lastName())
			.build();

		kafkaTemplate.send(config.getTopicName(), registration.getId(), registration);
	}
}
