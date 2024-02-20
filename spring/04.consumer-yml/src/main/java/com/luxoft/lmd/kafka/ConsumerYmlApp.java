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
public class ConsumerYmlApp {
	private final Logger logger = LoggerFactory.getLogger(getClass());

	public static void main(String[] args) {
		SpringApplication.run(ConsumerYmlApp.class, args);
	}

	// notice no "groupId" here
	@KafkaListener(topics = "spring.keyed.names")
	public void onNameAccessingFullRecordWithFullMetadata(
		ConsumerRecord<Long, String> record,
		@Header(KafkaHeaders.RECORD_METADATA) ConsumerRecordMetadata meta) {
		logger.info("onNameAccessingFullRecordWithFullMetadata:[{}] {}", record.key(), record.value());
	}
}
