package com.luxoft.lmd.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.Map;

@Configuration
public class KafkaConsumerConfiguration {
	@Autowired private KafkaProperties kafkaProperties;
	@Autowired private ObjectMapper objectMapper;

	@Bean public ConsumerFactory<String, Object> multiTypeConsumerFactory() {
		Map<String, Object> consumerProperties = kafkaProperties.buildConsumerProperties();
		return new DefaultKafkaConsumerFactory<>(
			consumerProperties,
			new StringDeserializer(),
			createDeserializer()
		);
	}

	private JsonDeserializer createDeserializer() {
		JsonDeserializer deserializer = new JsonDeserializer(objectMapper);
		deserializer.addTrustedPackages("com.luxoft.*");
		return deserializer;

	}

	@Bean public ConcurrentKafkaListenerContainerFactory<String, Object> multiTypeListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(multiTypeConsumerFactory());
		return factory;
	}
}
