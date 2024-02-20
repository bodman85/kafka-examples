package com.luxoft.lmd.kafka.throughput;

import net.datafaker.Faker;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class LargeThroughputProducer {
	private final String bootstrapServers = "localhost:9092, localhost:9093";

	private final Logger logger = LoggerFactory.getLogger(getClass());
	private final Faker faker = new Faker();

	public static void main(String[] args) throws ExecutionException, InterruptedException {
		new LargeThroughputProducer().run();
	}

	public void run() throws ExecutionException, InterruptedException {
		// keeping things in one place and simple - not using dependency injection
		// you would do that for any kind of production code, but you wouldn't be using
		// Kafka Java API directly anyway - use Spring for Kafka
		// and get this automatically set up

		Map<String, Object> config =
			Map.of(
				ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
				ProducerConfig.BATCH_SIZE_CONFIG, 200_000,
				ProducerConfig.LINGER_MS_CONFIG, 3000,
				ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4"
			);

		try (
			var producer =
				new KafkaProducer<>(
					config,
					new StringSerializer(),
					new StringSerializer()
				)
		) {
			int counter = 0;
			while (true) {
				sendMessage(producer, ++counter);
				Thread.sleep(100);
			}
		}
	}


	private void sendMessage(KafkaProducer<String, String> producer, int counter) {
		var key = counter + "--" + UUID.randomUUID();
		var name = faker.funnyName().name();

		var record = new ProducerRecord<>("throughput", key, name);

		Future<RecordMetadata> result = producer.send(record);
		// we should wait for result but let's emulate multiple simultanenous request

		logger.info("-> {}", record);
	}
}
