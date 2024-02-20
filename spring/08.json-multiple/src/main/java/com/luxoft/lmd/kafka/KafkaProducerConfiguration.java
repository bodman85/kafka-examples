package com.luxoft.lmd.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.Map;

@Configuration
public class KafkaProducerConfiguration {
	@Autowired private KafkaProperties kafkaProperties;
	@Autowired private ObjectMapper objectMapper;

	@Bean public ProducerFactory<String, Object> producerFactory() {
		Map<String, Object> config = kafkaProperties.buildProducerProperties();
		return new DefaultKafkaProducerFactory<>(
			config,
			new StringSerializer(),
			new JsonSerializer<>(objectMapper)
		);
	}

	@Bean public KafkaTemplate<String, Object> kafkaTemplate() {
		return new KafkaTemplate<>(producerFactory());
	}
}
