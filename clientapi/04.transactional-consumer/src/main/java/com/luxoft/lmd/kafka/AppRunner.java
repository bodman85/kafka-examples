package com.luxoft.lmd.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.Map;

@Component
public class AppRunner implements ApplicationRunner {
	private final Logger logger = LoggerFactory.getLogger(getClass());

	@Override
	public void run(ApplicationArguments args) throws Exception {
		Map<String, Object> config =
			Map.of(
				ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
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
