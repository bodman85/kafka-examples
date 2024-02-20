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


@EnableKafkaStreams
@SpringBootApplication
@Import(NamesGenerationConfiguration.class)
public class StreamsTerminalActionApp {
	public static Logger logger = LoggerFactory.getLogger(StreamsTerminalActionApp.class);

	public static void main(String[] args) {
		SpringApplication.run(StreamsTerminalActionApp.class, args);
	}

	@Autowired
	public void buildTopology(StreamsBuilder builder) {
		builder
			.<String, String>stream("names")
			.foreach((key, value) -> logger.info("<- {}:{}", key, value));
	}
}
