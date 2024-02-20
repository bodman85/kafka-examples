package com.luxoft.lmd.kafka;

import com.luxoft.lmd.kafka.model.ProductAdded;
import com.luxoft.lmd.kafka.model.ProductRemoved;
import com.luxoft.lmd.kafka.model.ShoppingCartCreated;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@KafkaListener(
	topics = "shopping.cart.events",
	containerFactory = "multiTypeListenerContainerFactory"
)
public class DataListener {
	private final Logger logger = LoggerFactory.getLogger(getClass());

	@KafkaHandler
	public void onCartCreated(ShoppingCartCreated event) {
		logger.info("<- onCartCreated: {}", event);
	}

	@KafkaHandler
	public void onProductAdded(ProductAdded event) {
		logger.info("<- productAdded: {}", event);
	}

	@KafkaHandler
	public void onProductRemoved(ProductRemoved event) {
		logger.info("<- productRemoved: {}", event);
	}

	@KafkaHandler(isDefault = true)
	public void fallback(Object payload) {
		logger.info("got some other payload: {}", payload);
	}
}
