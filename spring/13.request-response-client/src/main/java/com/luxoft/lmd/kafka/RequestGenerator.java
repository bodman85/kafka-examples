package com.luxoft.lmd.kafka;

import com.luxoft.lmd.kafka.model.Request;
import com.luxoft.lmd.kafka.model.Response;
import net.datafaker.Faker;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Component
public class RequestGenerator {
	@Autowired
	private ReplyingKafkaTemplate<String, Request, Response> replyingKafkaTemplate;
	private final Logger logger = LoggerFactory.getLogger(getClass());
	private final Faker faker = new Faker();

	@Scheduled(fixedDelay = 1000)
	public void run() {
		Request payload = new Request(faker.name().name());
		ProducerRecord<String, Request> producerRecord =
			new ProducerRecord<>("request", UUID.randomUUID().toString(), payload);

		CompletableFuture<Response> requestReplyFuture =
			replyingKafkaTemplate
				.sendAndReceive(producerRecord, Duration.ofSeconds(10))
				.thenApply(responseRecord -> {
						logger.info("got async response: {}", responseRecord);
						return responseRecord.value();
					}
				);
	}
}
