package com.luxoft.lmd.kafka;

import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.adapter.ConsumerRecordMetadata;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
public class DataListener {
	private final Logger logger = LoggerFactory.getLogger(getClass());

	@KafkaListener(
		groupId = "laggingConsumer",
		topics = "concurrency.test"
		//, concurrency = "2"
	)
	public void onQuote(
		String quote,
		@Header(KafkaHeaders.RECORD_METADATA) ConsumerRecordMetadata metadata
	) {
		logger.info("got quote: [{}:{}] {}, emulating long processing...",
			metadata.topic(),
			metadata.partition(),
			quote
		);

		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			throw Throwables.propagate(e);
		}
	}
}
