package com.luxoft.lmd.kafka.simple;

import net.datafaker.Faker;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class BasicProducer {
	private final String bootstrapServers = "localhost:9092, localhost:9093";

	private final Logger logger = LoggerFactory.getLogger(getClass());
	private final Faker faker = new Faker();

	public static void main(String[] args) throws ExecutionException, InterruptedException {
		new BasicProducer().run();
	}

	public void run() throws ExecutionException, InterruptedException {
		// keeping things in one place and simple - not using dependency injection
		// you would do that for any kind of production code, but you wouldn't be using
		// Kafka Java API directly anyway - use Spring for Kafka
		// and get this automatically set up

		Map<String, Object> config =
			Map.of(
				ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
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
		record.headers().add("textHeader", "hello!".getBytes(StandardCharsets.UTF_8));
		record.headers().add("intHeader", ByteBuffer.allocate(4).putInt(counter).array());

		Future<RecordMetadata> result = producer.send(record);
		RecordMetadata metadata = result.get();

		logger.info("\n-> {}\n   has been sent to:\n   {}:{}:{} at {}\n",
			record,
			metadata.topic(),
			metadata.partition(),
			metadata.offset(),
			new Date(metadata.timestamp()));
	}
}
