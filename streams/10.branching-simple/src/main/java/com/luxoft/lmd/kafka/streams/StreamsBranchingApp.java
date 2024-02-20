package com.luxoft.lmd.kafka.streams;

import com.luxoft.lmd.kafka.shared.NamesGenerationConfiguration;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
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
public class StreamsBranchingApp {
	public static Logger logger = LoggerFactory.getLogger(StreamsBranchingApp.class);

	public static void main(String[] args) {
		SpringApplication.run(StreamsBranchingApp.class, args);
	}

	@Bean public KafkaAdmin.NewTopics appTopics() {
		return new KafkaAdmin.NewTopics(
			TopicBuilder.name("names.a.4").build(),
			TopicBuilder.name("names.h").build(),
			TopicBuilder.name("names.other").build()
		);
	}

	@Autowired
	public void buildTopology(StreamsBuilder builder) {
		builder
			.<String, String>stream("names")
			.split()
			.branch(
				(key, value) -> value.startsWith("H"),
				Branched.withConsumer(hNamesStream -> hNamesStream.to("names.h"))
			)
			.branch(
				(key, value) -> value.startsWith("A"),
				Branched.withConsumer(
					aNamesStream ->
						aNamesStream
							.filter((key, value) -> value.length() > 4)
							.to("names.a.4")
				)
			)
			// .noDefaultBranch() - also a possibility
			.defaultBranch(
				Branched.withConsumer(stream -> stream.to("names.other"))
			);
	}
}
