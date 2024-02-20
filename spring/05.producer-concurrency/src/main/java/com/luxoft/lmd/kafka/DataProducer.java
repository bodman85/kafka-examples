package com.luxoft.lmd.kafka;

import lombok.SneakyThrows;
import net.datafaker.Faker;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Component
public class DataProducer implements Runnable {
	@Autowired private KafkaTemplate<String, String> kafkaTemplate;

	private final Faker faker = new Faker();
	private final Logger logger = LoggerFactory.getLogger(getClass());

	@SneakyThrows
	@Scheduled(fixedDelay = 500)
	@Override
	public void run() {
		String quote = faker.backToTheFuture().quote();
		CompletableFuture<SendResult<String, String>> result =
			kafkaTemplate.send(
				"concurrency.test",
				UUID.randomUUID().toString(),
				quote
			);

		SendResult<String, String> sendResult = result.get();

		RecordMetadata recordMetadata = sendResult.getRecordMetadata();
		logger.info("[{}:{}:{}] '{}'",
			recordMetadata.topic(),
			recordMetadata.partition(),
			recordMetadata.offset(),
			sendResult.getProducerRecord().value());
	}
}
