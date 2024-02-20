package com.luxoft.lmd.kafka.streams;

import lombok.Getter;
import lombok.Setter;
import net.datafaker.Faker;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Random;

@Component
@Getter @Setter
public class PageClickGenerator {
	private final Faker faker = new Faker();
	private final Random random = new Random();
	@Autowired public KafkaTemplate<Object, Object> kafkaTemplate;
	private List<String> urls =
		faker.collection(() -> faker.internet().url()).len(10).generate();

	private LocalDateTime date =
		LocalDateTime.now()
			.minusDays(700)
			.truncatedTo(ChronoUnit.MINUTES);


	@Scheduled(fixedDelay = 200)
	public void run() {
		PageClick click =
			PageClick.newBuilder()
				.setUrl(randomUrl())
				.setDate(date)
				.setSourceIP(faker.internet().publicIpV4Address())
				.setUserAgent(faker.internet().userAgent())
				.build();

		kafkaTemplate.send("pageclicks.02", null, click);

		date = date.plus(random.nextInt(100), ChronoUnit.MINUTES);
	}

	private String randomUrl() {
		return urls.get(random.nextInt(urls.size()));
	}
}
