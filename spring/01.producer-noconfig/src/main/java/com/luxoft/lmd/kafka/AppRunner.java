package com.luxoft.lmd.kafka;

import lombok.Getter;
import lombok.Setter;
import net.datafaker.Faker;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
@Getter
@Setter
public class AppRunner implements ApplicationRunner {
	@Autowired private KafkaTemplate<String, String> kafkaTemplate;
	private final Logger logger = LoggerFactory.getLogger(getClass());
	private final Faker faker = new Faker();

	@Override
	public void run(ApplicationArguments args) throws Exception {
		// CompletableFutures! Finally!!!
		CompletableFuture<SendResult<String, String>> result =
			kafkaTemplate.send("spring.names", faker.funnyName().name());

		SendResult<String, String> sendResult = result.get();

		RecordMetadata recordMetadata = sendResult.getRecordMetadata();
		logger.info("record: {} sent to {}:{}:{}",
			sendResult.getProducerRecord(),
			recordMetadata.topic(),
			recordMetadata.partition(),
			recordMetadata.offset());
	}
}
