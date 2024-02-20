package com.luxoft.lmd.kafka.tx;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class TransactionalConsumer {
	private final String bootstrapServers = "localhost:9092, localhost:9093";

	private final Logger logger = LoggerFactory.getLogger(getClass());

	public static void main(String[] args) throws Exception {
		new TransactionalConsumer().run();
	}

	public void run() throws Exception {
		Map<String, Object> config =
			Map.of(
				ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
				ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
				ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
				ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
				ConsumerConfig.GROUP_ID_CONFIG, "consumer01",
				// this is REALLY important
				ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed"
			);

		try (var consumer = new KafkaConsumer<String, String>(config)) {
			consumer.subscribe(List.of("names", "addresses"));

			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
				records.forEach(record ->
					logger.info("<- got record [{}] {}:{}", record.topic(), record.key(), record.value())
				);
			}
		}
	}
}
