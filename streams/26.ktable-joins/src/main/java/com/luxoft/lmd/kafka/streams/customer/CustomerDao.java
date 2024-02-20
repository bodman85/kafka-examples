package com.luxoft.lmd.kafka.streams.customer;

import jakarta.annotation.PostConstruct;
import lombok.Getter;
import net.datafaker.Faker;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Random;

@Component
public class CustomerDao {
	private Faker faker = new Faker();
	private Random random = new Random();
	@Getter private List<CustomerEntity> customers;

	@PostConstruct
	public void init() {
		this.customers =
			faker
				.collection(
					() ->
						new CustomerEntity(
							faker.idNumber().peselNumber(),
							faker.country().countryCode2(),
							faker.funnyName().name())
				).len(100)
				.generate();
	}

	public CustomerEntity randomCustomer() {
		return customers.get(random.nextInt(customers.size()));
	}
}
