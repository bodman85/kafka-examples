package com.luxoft.lmd.kafka.streams;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@Configuration
public class DataGeneratorConfiguration {
	@Bean public KafkaAdmin.NewTopics appTopics() {
		return new KafkaAdmin.NewTopics(
			TopicBuilder.name("users").build()
		);
	}

	@Bean public UserGenerator userGenerator() {
		return new UserGenerator();
	}
}
