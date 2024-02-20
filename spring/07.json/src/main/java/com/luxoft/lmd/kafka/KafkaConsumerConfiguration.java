package com.luxoft.lmd.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
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

	@Bean public ConsumerFactory<String, PageClickInfo> consumerFactory() {
		Map<String, Object> consumerProperties = kafkaProperties.buildConsumerProperties();
		consumerProperties.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 500);

		JsonDeserializer<PageClickInfo> clickDeserializer = new JsonDeserializer<>(objectMapper);
		clickDeserializer.addTrustedPackages("com.luxoft.*");
		return new DefaultKafkaConsumerFactory<>(
			consumerProperties,
			new StringDeserializer(),
			clickDeserializer
		);
	}

	@Bean public ConcurrentKafkaListenerContainerFactory<String, PageClickInfo> pageClickListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, PageClickInfo> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory());
		factory.setConcurrency(3);
		return factory;
	}
}
