package com.luxoft.lmd.springcloudstream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.binder.DefaultBinderFactory;
import org.springframework.cloud.stream.binder.kafka.KafkaMessageChannelBinder;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.messaging.MessageChannel;

import java.util.function.Consumer;

@EnableKafka
@SpringBootApplication
public class SCSTransactionalProducer {
	private final Logger logger = LoggerFactory.getLogger(getClass());

	public static void main(String[] args) {
		SpringApplication.run(SCSTransactionalProducer.class, args);
	}

	@Bean
	KafkaTransactionManager customKafkaTransactionManager(DefaultBinderFactory binderFactory) {
		KafkaMessageChannelBinder kafka = (KafkaMessageChannelBinder) binderFactory.getBinder("kafka", MessageChannel.class);
		ProducerFactory<byte[], byte[]> transactionalProducerFactory = kafka.getTransactionalProducerFactory();
		KafkaTransactionManager kafkaTransactionManager = new KafkaTransactionManager(transactionalProducerFactory);
		return kafkaTransactionManager;
	}

	@Bean public Consumer<String> wordConsumer() {
		return word -> logger.info("<- {}", word);
	}
}
