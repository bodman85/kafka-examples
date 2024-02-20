package com.luxoft.lmd.kafka.streams;

import com.luxoft.lmd.kafka.shared.FunnyNamesGenerationConfiguration;
import com.luxoft.lmd.kafka.shared.NamesGenerationConfiguration;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@EnableKafkaStreams
@SpringBootApplication
@Import({NamesGenerationConfiguration.class, FunnyNamesGenerationConfiguration.class})
public class StreamsMergingApp {
	public static Logger logger = LoggerFactory.getLogger(StreamsMergingApp.class);

	public static void main(String[] args) {
		SpringApplication.run(StreamsMergingApp.class, args);
	}

	@Autowired
	public void buildTopology(StreamsBuilder builder) {
		KStream<String, String> namesStream =
			builder.<String, String>stream("names")
				.mapValues((readOnlyKey, value) -> value.toLowerCase());

		KStream<String, String> funnyNamesStream =
			builder.<String, String>stream("funny_names")
				.mapValues((readOnlyKey, value) -> value.toUpperCase())
				.mapValues((readOnlyKey, value) -> "Funny:" + value);

		namesStream
			.merge(funnyNamesStream)
			.mapValues((readOnlyKey, value) -> "Hello '%s'".formatted(value))
			.peek((key, value) -> logger.info(value))
			.to("greetings");
	}

}
