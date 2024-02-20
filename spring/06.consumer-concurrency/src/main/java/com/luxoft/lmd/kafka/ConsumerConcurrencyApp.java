package com.luxoft.lmd.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableKafka
@EnableScheduling
@SpringBootApplication
public class ConsumerConcurrencyApp {
	@Bean
	public NewTopic concurrencyTopic() {
		return TopicBuilder.name("concurrency.test")
			.partitions(12)
			.replicas(3)
			.config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
			.build();
	}

	public static void main(String[] args) {
		SpringApplication.run(ConsumerConcurrencyApp.class, args);
	}
}
