package com.luxoft.lmd.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.Map;

@Configuration
public class KafkaProducerConfiguration {
	@Autowired private KafkaProperties kafkaProperties;
	@Autowired private ObjectMapper objectMapper;

	@Bean public NewTopic pageClickTopic() {
		return TopicBuilder.name("page.click").partitions(6).build();
	}

	@Bean public ProducerFactory<String, PageClickInfo> producerFactory() {
		// base configuration taken from yaml file
		Map<String, Object> producerProperties = kafkaProperties.buildProducerProperties();

		// customized further to our liking
		producerProperties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

		JsonSerializer valueSerializer = new JsonSerializer<>(objectMapper.constructType(PageClickInfo.class), objectMapper);
		//valueSerializer.setAddTypeInfo(false);
		return new DefaultKafkaProducerFactory<>(producerProperties, new StringSerializer(), valueSerializer);
	}

	@Bean public KafkaTemplate<String, PageClickInfo> kafkaTemplate() {
		KafkaTemplate<String, PageClickInfo> kafkaTemplate = new KafkaTemplate<>(producerFactory());
		kafkaTemplate.setDefaultTopic("page.click");
		return kafkaTemplate;
	}
}
