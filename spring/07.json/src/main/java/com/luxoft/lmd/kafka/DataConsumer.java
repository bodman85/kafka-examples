package com.luxoft.lmd.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
public class DataConsumer {
	private final Logger logger = LoggerFactory.getLogger(getClass());

	@KafkaListener(
		topics = "page.click",
		containerFactory = "pageClickListenerContainerFactory"
	)
	public void onClick(PageClickInfo click, @Header(KafkaHeaders.RECEIVED_PARTITION) int partition) {
		logger.info("<- {}:{}", partition, click);
	}
}
