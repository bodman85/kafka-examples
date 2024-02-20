package com.luxoft.lmd.kafka;

import com.luxoft.lmd.kafka.domain.UserRegistration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class DataReader {
	private final Logger logger = LoggerFactory.getLogger(getClass());

	@KafkaListener(
		groupId = "avro.reader",
		topics = "#{@appConfig.topicName}"
	)
	public void onRegistration(UserRegistration registration) {
		logger.info("got a registration: {}", registration);
	}
}
