package com.luxoft.lmd.kafka.offsetcommit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class CommittingConsumer {
	private final String bootstrapServers = "localhost:9092, localhost:9093";

	private final Logger logger = LoggerFactory.getLogger(getClass());

	public static void main(String[] args) throws Exception {
		new CommittingConsumer().run();
	}

	public void run() throws Exception {
		Map<String, Object> config =
			Map.of(
				ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
				ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
				ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
				ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
				ConsumerConfig.GROUP_ID_CONFIG, "group.01",
				//
				ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false
			);

		try (var consumer = new KafkaConsumer<>(
			config,
			new StringDeserializer(),
			new StringDeserializer())
		) {
			consumer.subscribe(List.of("names"));

			boolean keepRunning = true;

			while (keepRunning) {
				ConsumerRecords<String, String> records =
					consumer.poll(Duration.ofSeconds(2));

				records.forEach(record ->
					logger.info("<- got record [{}] key='{}', value='{}'",
						record.topic(),
						record.key(),
						record.value())
				);
				consumer.commitAsync();
			}

			consumer.commitSync();
		}
	}
}
