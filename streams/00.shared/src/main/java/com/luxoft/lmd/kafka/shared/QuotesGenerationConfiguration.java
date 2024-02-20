package com.luxoft.lmd.kafka.shared;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableKafka
@EnableScheduling
@Configuration
public class QuotesGenerationConfiguration {
	@Bean public NewTopic namesTopic() {
		return TopicBuilder.name("quotes").partitions(6).build();
	}

	@Bean public QuotesGenerator quotesGenerator() {
		return new QuotesGenerator();
	}
}
