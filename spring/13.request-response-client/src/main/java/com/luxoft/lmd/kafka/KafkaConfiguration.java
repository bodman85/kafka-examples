package com.luxoft.lmd.kafka;

import com.luxoft.lmd.kafka.model.Request;
import com.luxoft.lmd.kafka.model.Response;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;

@Configuration
public class KafkaConfiguration {

	@Bean public KafkaAdmin.NewTopics topics() {
		return new KafkaAdmin.NewTopics(
			TopicBuilder.name("request").build(),
			TopicBuilder.name("response").build()
		);
	}

	@Configuration
	public static class ClientConfig {
		@Autowired private ConcurrentKafkaListenerContainerFactory<String, Response> kafkaListenerContainerFactory;

		@Bean public ConcurrentMessageListenerContainer<String, Response> responseContainer() {
			ConcurrentMessageListenerContainer<String, Response> responseContainer =
				kafkaListenerContainerFactory.createContainer("response");

			responseContainer.getContainerProperties().setGroupId("responseProcessor");
			responseContainer.setAutoStartup(false);

			return responseContainer;
		}

		@Bean
		ReplyingKafkaTemplate<String, Request, Response> replyingKafkaTemplate(ProducerFactory<String, Request> producerFactory) {
			return new ReplyingKafkaTemplate<>(producerFactory, responseContainer());
		}
	}
}
