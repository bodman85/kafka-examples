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
public class StreamsFilteringApp {
	public static Logger logger = LoggerFactory.getLogger(StreamsFilteringApp.class);

	public static void main(String[] args) {
		SpringApplication.run(StreamsFilteringApp.class, args);
	}

	@Bean public NewTopic aNames() {
		return TopicBuilder.name("names.a").partitions(6).build();
	}

	@Autowired
	public void buildTopology(StreamsBuilder builder) {
		builder
			.<String, String>stream("names")
			//.peek((key, value) -> logger.info("before filter# {}", value))
			.filter((key, value) -> value.startsWith("A"))
			.peek((key, value) -> logger.info("after filter# {}", value))
			.to("names.a");
	}
}
