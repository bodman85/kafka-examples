package com.luxoft.lmd.kafka.shared;

import lombok.Getter;
import lombok.Setter;
import net.datafaker.Faker;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;

@Getter @Setter
public class NamesGenerator {
	private final Faker faker = new Faker();

	@Autowired private KafkaTemplate<String, String> kafkaTemplate;

	@Scheduled(fixedDelay = 1000)
	public void generate() {
		kafkaTemplate.send("names", null, faker.name().firstName());
	}
}
