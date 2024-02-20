package com.luxoft.lmd.kafka.partitioner;

import net.datafaker.Faker;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class PartitioningProducer {
	private final String bootstrapServers = "localhost:9092, localhost:9093";

	private final Logger logger = LoggerFactory.getLogger(getClass());
	private final Faker faker = new Faker();

	public static void main(String[] args) throws ExecutionException, InterruptedException {
		new PartitioningProducer().run();
	}

	public void run() throws ExecutionException, InterruptedException {
		String topicName = createTopic();

		Map<String, Object> config =
			Map.of(
				ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
				ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.luxoft.lmd.kafka.partitioner.MyPartitioner"
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
				sendMessage(producer, ++counter, topicName);
				Thread.sleep(1000);
			}
		}
	}

	private String createTopic() throws InterruptedException, ExecutionException {
		AdminClient adminClient =
			AdminClient.create(
				Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
			);

		String topicName = "names-" + faker.internet().uuid();

		ListTopicsResult listTopicsResult = adminClient.listTopics();
		Set<String> existingTopics = listTopicsResult.names().get();

		if (existingTopics.contains(topicName))
			return topicName;

		// trying to create a topic that already exists resuts in exception
		// we would have to check if topic exists, or ... we cheat
		CreateTopicsResult createTopicsResult =
			adminClient.createTopics(
				List.of(
					new NewTopic(topicName, 10, (short) 1))
			);

		createTopicsResult.all().get();

		adminClient.close();
		return topicName;
	}

	private void sendMessage(KafkaProducer<String, String> producer, int counter, String topicName)
		throws ExecutionException, InterruptedException {
		var key = counter + "--" + UUID.randomUUID();
		var name = faker.funnyName().name();

		var record = new ProducerRecord<>(topicName, key, name);

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
