package com.luxoft.lmd.kafka;

import com.luxoft.lmd.kafka.model.Request;
import com.luxoft.lmd.kafka.model.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.handler.annotation.SendTo;

import java.util.HexFormat;

@Configuration
public class KafkaConfiguration {

	@Bean public KafkaAdmin.NewTopics topics() {
		return new KafkaAdmin.NewTopics(
			TopicBuilder.name("request").build(),
			TopicBuilder.name("response").build()
		);
	}

	@Configuration
	public static class ServerConfig {
		private final Logger logger = LoggerFactory.getLogger(getClass());

		@KafkaListener(
			topics = "request",
			groupId = "requestProcessor"
		)
		@SendTo
		public Response process(@Payload Request request,
		                        @Header(KafkaHeaders.REPLY_TOPIC) String replyTo,
		                        @Header(KafkaHeaders.CORRELATION_ID) byte[] correlationId) {
			logger.info("->: {}", request);

			logger.info("the response will be sent to: {}", replyTo);
			logger.info(
				"the client will match the response using correlation ID: {}",
				HexFormat.of().formatHex(correlationId));

			return new Response("Hello " + request.message().toUpperCase());
		}
	}
}
