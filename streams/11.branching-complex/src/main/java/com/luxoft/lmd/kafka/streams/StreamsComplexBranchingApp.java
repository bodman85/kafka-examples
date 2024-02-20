package com.luxoft.lmd.kafka.streams;

import com.luxoft.lmd.kafka.shared.NamesGenerationConfiguration;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
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

import java.util.Map;

@EnableKafkaStreams
@SpringBootApplication
@Import(NamesGenerationConfiguration.class)
public class StreamsComplexBranchingApp {
	public static Logger logger = LoggerFactory.getLogger(StreamsComplexBranchingApp.class);

	public static void main(String[] args) {
		SpringApplication.run(StreamsComplexBranchingApp.class, args);
	}

	@Bean public KafkaAdmin.NewTopics appTopics() {
		return new KafkaAdmin.NewTopics(
			TopicBuilder.name("names.a.4").build(),
			TopicBuilder.name("names.h.6").build(),
			TopicBuilder.name("names.other").build()
		);
	}

	@Autowired
	public void buildTopology(StreamsBuilder builder) {
		KStream<String, String> namesStream = builder.stream("names");

		Map<String, KStream<String, String>> splitMap =
			namesStream
				.split(Named.as("starting_letters_"))
				.branch(
					(key, value) -> value.startsWith("A"),
					Branched.as("a")
				)
				.branch(
					(key, value) -> value.startsWith("H"),
					Branched.as("h")
				)
				.defaultBranch(
					Branched.as("fallback")
				);

		splitMap.get("starting_letters_a")
			.filter((key, value) -> value.length() > 4)
			.peek((key, value) -> logger.info("a > 4: {}", value))
			.to("names.a.4");

		splitMap.get("starting_letters_h")
			.filter((key, value) -> value.length() > 6)
			.peek((key, value) -> logger.info("h > 6: {}", value))
			.to("names.h.6");

		splitMap.get("starting_letters_fallback")
			.to("names.other");
	}
}
