package com.luxoft.lmd.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class QuoteSizeListener {
	private final Logger logger = LoggerFactory.getLogger(getClass());

	@KafkaListener(topics = "quotes.length", groupId = "quotes")
	public void run(MovieQuoteLength quoteLength) {
		logger.info("got record: {}", quoteLength);
	}
}
