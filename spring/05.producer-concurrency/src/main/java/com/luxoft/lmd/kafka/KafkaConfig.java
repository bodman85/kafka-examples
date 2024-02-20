package com.luxoft.lmd.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfig {
	@Bean
	public NewTopic concurrencyTopic() {
		return TopicBuilder.name("concurrency.test")
			.partitions(12)
			.replicas(3)
			.config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
			.build();
	}
}
