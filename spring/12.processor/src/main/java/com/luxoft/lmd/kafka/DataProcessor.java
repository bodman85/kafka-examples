package com.luxoft.lmd.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class DataProcessor {
	private final Logger logger = LoggerFactory.getLogger(getClass());

	@KafkaListener(
		topics = "quotes.01",
		groupId = "quotes"
	)
	@SendTo("quotes.length")
	@Transactional
	public MovieQuoteLength process(MovieQuote quote) {
		MovieQuoteLength result = new MovieQuoteLength(quote.message().length());
		logger.info("processing: '{}' -> {}", quote, result);
		return result;
	}
}
