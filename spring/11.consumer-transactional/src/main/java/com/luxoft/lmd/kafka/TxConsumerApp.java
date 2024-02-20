package com.luxoft.lmd.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.adapter.ConsumerRecordMetadata;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;

@EnableKafka
@SpringBootApplication
public class TxConsumerApp {
	private final Logger logger = LoggerFactory.getLogger(getClass());

	public static void main(String[] args) {
		SpringApplication.run(TxConsumerApp.class, args);
	}

	@KafkaListener(
		topics = {"names", "addresses"}
	)
	public void onData(
		ConsumerRecord<String, String> record,
		@Header(KafkaHeaders.RECORD_METADATA) ConsumerRecordMetadata meta) {
		logger.info("got data:[{}] {} ## {}", meta.topic(), record.key(), record.value());
	}
}
