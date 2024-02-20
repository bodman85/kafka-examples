package com.luxoft.lmd.kafka.streams.customer;

import com.luxoft.lmd.kafka.streams.Customer;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class CustomersPopulator implements ApplicationRunner {
	private final CustomerDao customers;
	private final KafkaTemplate<Object, Object> kafkaTemplate;

	public CustomersPopulator(CustomerDao customers, KafkaTemplate<Object, Object> kafkaTemplate) {
		this.customers = customers;
		this.kafkaTemplate = kafkaTemplate;
	}

	@Override public void run(ApplicationArguments args) {
		customers.getCustomers().forEach(customer -> {
			var payload =
				Customer.newBuilder()
					.setId(customer.id())
					.setCountryCode(customer.countryCode())
					.setName(customer.name())
					.build();

			kafkaTemplate.send("customers", payload.getId(), payload);
		});
	}
}
