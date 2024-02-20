package com.luxoft.lmd.kafka.streams;

import com.luxoft.lmd.kafka.shared.NamesGenerationConfiguration;
import org.apache.kafka.streams.StreamsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.EnableKafkaStreams;

import java.util.Arrays;

@EnableKafkaStreams
@SpringBootApplication
@Import({NamesGenerationConfiguration.class})
public class StreamsFlatMapValuesApp {
	public static Logger logger = LoggerFactory.getLogger(StreamsFlatMapValuesApp.class);

	public static void main(String[] args) {
		SpringApplication.run(StreamsFlatMapValuesApp.class, args);
	}

	@Autowired
	public void buildTopology(StreamsBuilder builder) {
		builder.<String, String>stream("names")
			.mapValues((readOnlyKey, value) -> value.toLowerCase())
			.peek((key, name) -> logger.info("<- {}", name))

			.flatMapValues((readOnlyKey, value) -> Arrays.stream(value.split("")).toList())

			.peek((key, letter) -> logger.info("-> {}", letter))
			.to("letters");
	}
}
