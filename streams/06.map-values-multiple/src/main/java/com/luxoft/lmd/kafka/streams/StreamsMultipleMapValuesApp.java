package com.luxoft.lmd.kafka.streams;

import com.luxoft.lmd.kafka.shared.NamesGenerationConfiguration;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.StreamsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.TopicBuilder;

@EnableKafkaStreams
@SpringBootApplication
@Import(NamesGenerationConfiguration.class)
public class StreamsMultipleMapValuesApp {
	public static Logger logger = LoggerFactory.getLogger(StreamsMultipleMapValuesApp.class);

	public static void main(String[] args) {
		SpringApplication.run(StreamsMultipleMapValuesApp.class, args);
	}

	@Bean public NewTopic namesProcessed() {
		return TopicBuilder.name("names.processed").partitions(6).build();
	}

	@Autowired
	public void buildTopology(StreamsBuilder builder) {
		builder
			.<String, String>stream("names")
			.mapValues((readOnlyKey, value) -> value.toUpperCase())
			.mapValues((readOnlyKey, value) -> new StringBuilder(value).reverse().toString())
			.peek((key, value) -> logger.info("after# {}", value))
			.to("names.processed");
	}
}
