package com.luxoft.lmd.kafka.streams;

import lombok.Getter;
import lombok.Setter;
import net.datafaker.Faker;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Random;

@Component
@Getter @Setter
public class PageClickGenerator {
	private final Faker faker = new Faker();
	private final Random random = new Random();
	@Autowired public KafkaTemplate<Object, Object> kafkaTemplate;
	private List<String> urls =
		faker.collection(() -> faker.internet().url()).len(100).generate();

	@Scheduled(fixedDelay = 1000)
	public void run() {
		PageClick click =
			PageClick.newBuilder()
				.setUrl(randomUrl())
				.setDate(LocalDateTime.now())
				.setSourceIP(faker.internet().publicIpV4Address())
				.setUserAgent(faker.internet().userAgent())
				.build();

		kafkaTemplate.send("pageclicks.01", null, click);
	}

	private String randomUrl() {
		return urls.get(random.nextInt(urls.size()));
	}
}
