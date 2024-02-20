package com.luxoft.lmd.kafka.streams;

import com.luxoft.lmd.kafka.shared.FunnyNamesGenerationConfiguration;
import com.luxoft.lmd.kafka.shared.NamesGenerationConfiguration;
import org.apache.kafka.streams.StreamsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.EnableKafkaStreams;

import java.util.List;

@EnableKafkaStreams
@SpringBootApplication
@Import({NamesGenerationConfiguration.class, FunnyNamesGenerationConfiguration.class})
public class StreamsMergingSimpleApp {
	public static Logger logger = LoggerFactory.getLogger(StreamsMergingSimpleApp.class);

	public static void main(String[] args) {
		SpringApplication.run(StreamsMergingSimpleApp.class, args);
	}

	@Autowired
	public void multipleTopicsTopology(StreamsBuilder builder) {
		builder.stream(List.of("names", "funny_names"))
			.mapValues((readOnlyKey, value) -> "Hello " + value)
			.peek((key, value) -> logger.info(value))
			.to("greetings.basic");
	}
}
