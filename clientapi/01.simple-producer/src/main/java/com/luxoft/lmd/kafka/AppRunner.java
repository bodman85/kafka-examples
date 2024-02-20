package com.luxoft.lmd.kafka;

import net.datafaker.Faker;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@Component
public class AppRunner implements ApplicationRunner {
	private final Logger logger = LoggerFactory.getLogger(getClass());
	private final Faker faker = new Faker();

	@Override
	public void run(ApplicationArguments args) throws Exception {
		// keeping things in one place and simple - not using dependency injection
		// you would do that for any kind of production code, but you wouldn't be using
		// Kafka Java API directly anyway - use Spring for Kafka
		// and get this automatically set up

		Map<String, Object> config =
			Map.of(
				ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
				ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
				ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
			);

		try (var producer = new KafkaProducer<String, String>(config)) {
			int counter = 0;
			while (true) {
				sendMessage(producer, ++counter);

				Thread.sleep(1000);
			}
		}
	}

	private void sendMessage(KafkaProducer<String, String> producer, int counter)
		throws ExecutionException, InterruptedException {
		var key = counter + "--" + UUID.randomUUID();
		var name = faker.funnyName().name();

		var record = new ProducerRecord<>("names", key, name);

		Future<RecordMetadata> result = producer.send(record);
		RecordMetadata metadata = result.get();

		logger.info("\n{}\nhas been sent to:\n{}:{}:{} at {}\n",
			record,
			metadata.topic(),
			metadata.partition(),
			metadata.offset(),
			new Date(metadata.timestamp()));
	}
}
