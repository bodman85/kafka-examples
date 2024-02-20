package com.luxoft.lmd.kafka;

import net.datafaker.Faker;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class DataGenerator implements Runnable {
	@Autowired private KafkaTemplate<String, Object> kafkaTemplate;
	private final Faker faker = new Faker();

	@Scheduled(fixedDelay = 1000)
	@Transactional
	@Override public void run() {
		MovieQuote movieQuote = new MovieQuote(faker.backToTheFuture().quote());

		kafkaTemplate.send("quotes.01", movieQuote);
	}
}
