package com.luxoft.lmd.kafka.streams;

import com.luxoft.lmd.kafka.streams.customer.CustomerDao;
import com.luxoft.lmd.kafka.streams.product.ProductDao;
import net.datafaker.Faker;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Component
public class PurchaseGenerator {
	@Autowired private KafkaTemplate<Object, Object> kafkaTemplate;
	@Autowired private CustomerDao customers;
	@Autowired private ProductDao products;
	private Faker faker = new Faker();

	@Scheduled(fixedDelay = 2000)
	public void run() {
		Purchase purchase =
			Purchase.newBuilder()
				.setCustomerId(customers.randomCustomer().id())
				.setProductId(products.randomProduct().id())
				.setDate(LocalDateTime.now())
				.setQuantity(faker.random().nextInt(10, 20))
				.setPrice(faker.number().randomDouble(2, 50, 200))
				.build();

		kafkaTemplate.send("purchases.02", null, purchase);
	}
}
