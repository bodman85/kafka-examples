package com.luxoft.lmd.kafka.streams;

import com.luxoft.lmd.kafka.shared.NamesGenerationConfiguration;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

@EnableKafkaStreams
@SpringBootApplication
@Import(NamesGenerationConfiguration.class)
public class StreamsNamedNodesApp {
	public static Logger logger = LoggerFactory.getLogger(StreamsNamedNodesApp.class);

	public static void main(String[] args) {
		SpringApplication.run(StreamsNamedNodesApp.class, args);
	}

	@Bean public KafkaAdmin.NewTopics appTopics() {
		return new KafkaAdmin.NewTopics(
			TopicBuilder.name("names.m.uppercased").partitions(6).build(),
			TopicBuilder.name("names.m.reversed").partitions(6).build(),
			TopicBuilder.name("names.long").partitions(6).build()
		);
	}

	@Autowired
	public void buildTopology(StreamsBuilder builder) {
		KStream<String, String> allNames = builder.stream("names");

		KStream<String, String> mNames =
			allNames.filter(
				(key, value) -> value.startsWith("M"),
				Named.as("only_m")
			);

		mNames.mapValues(
			(readOnlyKey, value) -> value.toUpperCase(),
			Named.as("uppercase")
		).to("names.m.uppercased");

		mNames
			.mapValues(
				(readOnlyKey, value) -> new StringBuilder(value).reverse().toString(),
				Named.as("reverse")
			).to("names.m.reversed");

		allNames
			.filter(
				(key, value) -> value.length() > 5,
				Named.as("over_5_chars")
			).to("names.long");
	}
}
