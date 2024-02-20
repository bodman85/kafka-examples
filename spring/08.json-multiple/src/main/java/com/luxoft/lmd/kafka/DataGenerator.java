package com.luxoft.lmd.kafka;

import com.luxoft.lmd.kafka.model.ProductAdded;
import com.luxoft.lmd.kafka.model.ProductRemoved;
import com.luxoft.lmd.kafka.model.ShoppingCartClosed;
import com.luxoft.lmd.kafka.model.ShoppingCartCreated;
import net.datafaker.Faker;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.UUID;

@Component
public class DataGenerator implements ApplicationRunner {
	@Autowired private KafkaTemplate<String, Object> kafkaTemplate;
	private final Faker faker = new Faker();

	@Override public void run(ApplicationArguments args) throws Exception {
		String topicName = "shopping.cart.events";

		String cartId = UUID.randomUUID().toString();

		kafkaTemplate.send(
			topicName,
			cartId,
			new ShoppingCartCreated(cartId, LocalDateTime.now(), faker.funnyName().name())
		);

		kafkaTemplate.send(
			topicName,
			cartId,
			new ProductAdded(cartId, faker.commerce().productName(), 10, 24.99)
		);

		String productId = faker.commerce().productName();
		kafkaTemplate.send(
			topicName,
			cartId,
			new ProductAdded(cartId, productId, 3, 3.99)
		);
		kafkaTemplate.send(
			topicName,
			cartId,
			new ProductRemoved(cartId, productId, 1)
		);

		kafkaTemplate.send(
			topicName,
			cartId,
			new ShoppingCartClosed(cartId, LocalDateTime.now())
		);
	}
}
