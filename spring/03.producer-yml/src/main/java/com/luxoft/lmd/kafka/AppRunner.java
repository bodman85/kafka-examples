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

import java.util.Random;
import java.util.concurrent.CompletableFuture;

@Component
@Getter
@Setter
public class AppRunner implements ApplicationRunner {
	// match the generic interface to your serializers,
	// it might look like the app starts, but it will throw a lot of ClassCastExceptions
	// if you mismatch key/value types.
	@Autowired private KafkaTemplate<Long, String> kafkaTemplate;

	private Random random = new Random();
	private Logger logger = LoggerFactory.getLogger(getClass());
	private Faker faker = new Faker();

	@Override
	public void run(ApplicationArguments args) throws Exception {
		CompletableFuture<SendResult<Long, String>> result =
			kafkaTemplate.send(
				"spring.keyed.names",
				random.nextLong(10000L),
				faker.funnyName().name()
			);

		SendResult<Long, String> sendResult = result.get();

		RecordMetadata recordMetadata = sendResult.getRecordMetadata();
		logger.info("record: {} sent to {}:{}:{}",
			sendResult.getProducerRecord(),
			recordMetadata.topic(),
			recordMetadata.partition(),
			recordMetadata.offset());
	}
}
