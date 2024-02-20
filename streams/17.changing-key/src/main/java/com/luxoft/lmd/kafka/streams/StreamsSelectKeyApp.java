package com.luxoft.lmd.kafka.streams;

import com.luxoft.lmd.kafka.shared.NamesGenerationConfiguration;
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
import org.springframework.kafka.core.KafkaAdmin;

@EnableKafkaStreams
@SpringBootApplication
@Import({NamesGenerationConfiguration.class})
public class StreamsSelectKeyApp {
	public static Logger logger = LoggerFactory.getLogger(StreamsSelectKeyApp.class);

	public static void main(String[] args) {
		SpringApplication.run(StreamsSelectKeyApp.class, args);
	}

	@Bean public KafkaAdmin.NewTopics appTopics() {
		return new KafkaAdmin.NewTopics(
			TopicBuilder.name("names.keyed").partitions(6).build()
		);
	}

	@Autowired
	public void buildTopology(StreamsBuilder builder) {
		builder.<String, String>stream("names")
			.selectKey((key, value) -> String.valueOf(value.charAt(0)))
			.to("names.keyed");
	}
}
