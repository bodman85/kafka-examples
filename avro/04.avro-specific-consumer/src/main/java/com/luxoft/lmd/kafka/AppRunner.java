package com.luxoft.lmd.kafka;

import com.google.common.collect.ImmutableMap;
import com.luxoft.lmd.domain.User;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.specific.SpecificRecord;
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
				// !!!!
				.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true)
				.put(ConsumerConfig.GROUP_ID_CONFIG, "avro-specific-consumer-01")
				.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
				.build();

		try (var consumer = new KafkaConsumer<String, SpecificRecord>(config)) {
			consumer.subscribe(List.of("users"));

			while (true) {
				ConsumerRecords<String, SpecificRecord> records = consumer.poll(Duration.ofSeconds(2));
				records.forEach(record -> {
						SpecificRecord value = record.value();
						logger.info("<- got record [{}] {}:{}", record.topic(), record.key(), record.value());
						logger.info("record value is of type: " + value.getClass().getName());

						// this cast is totally safe, but let's check anyway
						if (value instanceof User user) {
							logger.info("parsing user: {}", user.getNickname());
						}
					}
				);
			}
		}
	}
}
