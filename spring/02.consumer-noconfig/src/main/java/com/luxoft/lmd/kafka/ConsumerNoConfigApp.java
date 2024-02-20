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
public class ConsumerNoConfigApp {
	private final Logger logger = LoggerFactory.getLogger(getClass());

	public static void main(String[] args) {
		SpringApplication.run(ConsumerNoConfigApp.class, args);
	}

	@KafkaListener(
		groupId = "springConsumerGroup01",
		topics = "spring.names"
	)
	public void onName(String name) {
		logger.info("onName:{}", name);
	}

	@KafkaListener(
		groupId = "springConsumerGroup02",
		topics = "spring.names"
	)
	public void onNameAccessingFullRecordWithFullMetadata(
		ConsumerRecord<String, String> record,
		@Header(KafkaHeaders.RECORD_METADATA) ConsumerRecordMetadata meta) {
		logger.info("onNameAccessingFullRecordWithFullMetadata:[{}] {}", meta.topic(), record);
	}
}
