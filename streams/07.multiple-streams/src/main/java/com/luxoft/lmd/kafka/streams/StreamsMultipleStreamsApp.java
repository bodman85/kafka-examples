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
public class StreamsMultipleStreamsApp {
	public static Logger logger = LoggerFactory.getLogger(StreamsMultipleStreamsApp.class);

	public static void main(String[] args) {
		SpringApplication.run(StreamsMultipleStreamsApp.class, args);
	}

	@Bean public NewTopic namesUppercased() {
		return TopicBuilder.name("names.uppercased").partitions(6).build();
	}

	@Bean public NewTopic namesProcessed() {
		return TopicBuilder.name("names.processed").partitions(6).build();
	}

	@Autowired
	public void buildTopology(StreamsBuilder builder) {
		builder
			.<String, String>stream("names")
			.mapValues((readOnlyKey, value) -> value.toUpperCase())
			.peek((key, value) -> logger.info("uppercase# {}", value))
			.to("names.uppercased");
	}

	@Autowired
	public void buildOther(StreamsBuilder builder) {
		// yes, you can just build that dual topology in one method
		// this only shows the code can be separate
		builder
			.<String, String>stream("names.uppercased")
			.mapValues((readOnlyKey, value) -> new StringBuilder(value).reverse().toString())
			.peek((key, value) -> logger.info("reverse# {}", value))
			.to("names.processed");
	}
}
