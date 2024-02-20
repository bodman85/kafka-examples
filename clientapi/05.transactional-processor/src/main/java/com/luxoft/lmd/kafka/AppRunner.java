package com.luxoft.lmd.kafka;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Component
public class AppRunner implements ApplicationRunner {
	private static final String bootstrapServers = "localhost:9092";

	private final Logger logger = LoggerFactory.getLogger(getClass());
	private final Random random = new Random();

	@Override
	public void run(ApplicationArguments args) throws Exception {
		Map<String, Object> producerConfig = buildProducerConfig();
		Map<String, Object> consumerConfig = buildConsumerConfig();

		try (var producer = new KafkaProducer<String, String>(producerConfig)) {
			producer.initTransactions();

			try (var consumer = new KafkaConsumer<String, String>(consumerConfig)) {
				consumer.subscribe(List.of("names"));

				while (true) {
					ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));

					producer.beginTransaction();

					try {
						records.forEach(record -> {
								logger.info("<- got record [{}] {}:{}", record.topic(), record.key(), record.value());

								if (random.nextBoolean()) throw new RuntimeException("emulating a problem");

								var transformedValue = record.value().toUpperCase();
								var outputRecord = new ProducerRecord("names.uppercased", record.key(), transformedValue);

								try {
									producer.send(outputRecord).get();
								} catch (InterruptedException | ExecutionException e) {
									throw new RuntimeException(e);
								}
							}
						);

						Map<TopicPartition, OffsetAndMetadata> offsets = extractOffsets(records);
						producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata());
						producer.commitTransaction();
					} catch (Exception e) {
						logger.error("processing error occured", e);
						producer.abortTransaction();
					}
				}
			}
		}
	}

	private Map<TopicPartition, OffsetAndMetadata> extractOffsets(ConsumerRecords<String, String> records) {
		return records.partitions().stream()
			.map(topicPartition -> {
				long lastOffsetForPartition = Iterables.getLast(records.records(topicPartition)).offset();
				return Pair.of(topicPartition, new OffsetAndMetadata(lastOffsetForPartition + 1));
			}).collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
	}

	private Map<String, Object> buildConsumerConfig() {
		return ImmutableMap.<String, Object>builder()
			.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
			.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
			.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
			.put(ConsumerConfig.GROUP_ID_CONFIG, "processor01")
			.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
			.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
			.build();
	}

	private Map<String, Object> buildProducerConfig() {
		return ImmutableMap.<String, Object>builder()
			.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
			.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
			.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
			.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "processor01-" + UUID.randomUUID())
			.build();
	}
}
