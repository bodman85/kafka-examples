package com.luxoft.lmd.kafka.streams;

import com.luxoft.lmd.kafka.shared.NamesGenerationConfiguration;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Produced;
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
public class StreamsMapApp {
	public static Logger logger = LoggerFactory.getLogger(StreamsMapApp.class);

	public static void main(String[] args) {
		SpringApplication.run(StreamsMapApp.class, args);
	}

	@Bean public KafkaAdmin.NewTopics appTopics() {
		return new KafkaAdmin.NewTopics(
			TopicBuilder.name("names.by_length").partitions(6).build()
		);
	}

	@Autowired
	public void buildTopology(StreamsBuilder builder) {
		builder.<String, String>stream("names")
			.map((key, value) -> new KeyValue<>(value.length(), value.toUpperCase()))
			.peek((key, value) -> logger.info("K:{}, V:{}", key, value))
			.to("names.by_length", Produced.with(Serdes.Integer(), Serdes.String()));
	}
}
