package com.luxoft.lmd.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin.NewTopics;

import java.time.Duration;
import java.util.Optional;

@Configuration
public class KafkaConfig {
	@Bean
	public NewTopic topic1() {
		return new NewTopic("topic1", 12, (short) 3);
	}

	@Bean
	public NewTopic topic2() {
		return new NewTopic("topic1.1", Optional.of(12), Optional.empty());
	}

	@Bean
	public NewTopic topic3() {
		return TopicBuilder.name("topic3").partitions(10).build();
	}

	@Bean
	public NewTopic topic4() {
		return TopicBuilder.name("topic4")
			.partitions(8)
			.replicas(3)
			.compact()
			.build();
	}

	@Bean
	public NewTopic topic5() {
		return TopicBuilder.name("topic5")
			.partitions(10)
			.replicas(3)
			.config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
			.config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(Duration.ofDays(3).toMillis()))
			.build();
	}

	@Bean
	NewTopics multipleTopics() {
		return new NewTopics(
			TopicBuilder.name("multiple1").partitions(3).build(),
			TopicBuilder.name("multiple2").partitions(11).build(),
			TopicBuilder.name("multiple3").partitions(7).build()
		);
	}
}
