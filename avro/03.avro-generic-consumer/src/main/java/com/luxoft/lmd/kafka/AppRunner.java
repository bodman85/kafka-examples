package com.luxoft.lmd.kafka;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.generic.GenericRecord;
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

@Component
public class AppRunner implements ApplicationRunner {
	private final Logger logger = LoggerFactory.getLogger(getClass());

	@Override
	public void run(ApplicationArguments args) throws Exception {
		var config =
			ImmutableMap.<String, Object>builder()
				.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
				.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
				.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class)
				.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
				.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8085")
				.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false)
				.put(ConsumerConfig.GROUP_ID_CONFIG, "avro-consumer-03")
				.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
				.build();

		try (var consumer = new KafkaConsumer<String, GenericRecord>(config)) {
			consumer.subscribe(List.of("users"));

			while (true) {
				ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofSeconds(2));
				records.forEach(record -> {
						GenericRecord value = record.value();

						logger.info("<- got record [{}] {}:{}", record.topic(), record.key(), record.value());
						// you need to introspect the GenericRecord for particular fields

						logger.info("parsing user: {} {}", value.get("firstName"), value.get("lastName"));
					}
				);
			}
		}
	}
}
