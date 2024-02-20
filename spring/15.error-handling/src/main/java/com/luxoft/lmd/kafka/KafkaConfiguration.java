package com.luxoft.lmd.kafka;

import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConfiguration {
	public final static String TOPIC_NAME = "error.handling";
	public final static String STANDARD_DLT_TOPIC_NAME = "error.handling.DLT";
	public final static String CUSTOM_DLT_TOPIC_NAME = "custom.dlt";

	@Bean
	public KafkaAdmin.NewTopics topics() {
		return new KafkaAdmin.NewTopics(
			TopicBuilder.name(TOPIC_NAME).partitions(3).build(),
			TopicBuilder.name(STANDARD_DLT_TOPIC_NAME).partitions(3).build(),
			TopicBuilder.name(CUSTOM_DLT_TOPIC_NAME).partitions(1).build()
		);
	}

	// there can be only one DefaultErrorHandler registered or ... Spring will not choose any
	// if you need different errorHandlers - you need to set up all the factories from scratch

	//@Bean
	public DefaultErrorHandler exponentialBackOffErrorHandlerWithoutRecovery() {
		ExponentialBackOff backOff = new ExponentialBackOff(1000L, 1.5);
		// delays:
		// 1000 ms
		// 1000 * 1.5 = 1500 ms
		// 1000 * 1.5 * 1.5 = 2250 ms

		backOff.setMaxElapsedTime(10000);

		return new DefaultErrorHandler(backOff);
	}

	//@Bean
	public DefaultErrorHandler fixedBackOffErrorHandlerWithoutRecovery() {
		FixedBackOff backOff = new FixedBackOff(1500L, 2);
		return new DefaultErrorHandler(backOff);
	}

	//@Bean
	public DefaultErrorHandler simpleDltErrorHandler(KafkaTemplate<Object, Object> kafkaTemplate) {
		FixedBackOff backOff = new FixedBackOff(1000L, 5);
		DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate);
		return new DefaultErrorHandler(recoverer, backOff);
	}

	//@Bean
	public DefaultErrorHandler dltErrorHandler(KafkaTemplate<Object, Object> kafkaTemplate) {
		FixedBackOff backOff = new FixedBackOff(1000L, 5);

		DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
			kafkaTemplate,
			(consumerRecord, e) -> new TopicPartition(KafkaConfiguration.CUSTOM_DLT_TOPIC_NAME, 0)
		);

		recoverer.excludeHeader(DeadLetterPublishingRecoverer.HeaderNames.HeadersToAdd.EX_STACKTRACE);

		return new DefaultErrorHandler(recoverer, backOff);
	}
}
