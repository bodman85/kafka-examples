package com.luxoft.lmd.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class DataListener {
	private final Logger logger = LoggerFactory.getLogger(getClass());

	@KafkaListener(topics = KafkaConfiguration.TOPIC_NAME)
	public void onMessage(String message) {
		logger.info("consuming {}, but throwing...", message);
		throw new RuntimeException("unexpected");
	}
}
