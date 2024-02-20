package com.luxoft.lmd.kafka.streams;

import lombok.Getter;
import lombok.Setter;
import net.datafaker.Faker;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;

@Getter @Setter
public class UserGenerator {
	@Autowired public KafkaTemplate<Object, Object> kafkaTemplate;
	private final Faker faker = new Faker();

	@Scheduled(fixedDelay = 1000)
	public void generateUser() {

		UserId id =
			UserId.newBuilder()
				.setCountry(faker.country().countryCode2().toUpperCase())
				.setPesel(faker.idNumber().peselNumber())
				.build();

		User user =
			User.newBuilder()
				.setFirstName(faker.name().firstName())
				.setLastName(faker.name().lastName())
				.build();

		kafkaTemplate.send("users", id, user);
	}
}
