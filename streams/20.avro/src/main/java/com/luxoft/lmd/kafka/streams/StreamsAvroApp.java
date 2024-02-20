package com.luxoft.lmd.kafka.streams;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.Map;

@EnableKafkaStreams
@SpringBootApplication
@EnableScheduling
public class StreamsAvroApp {
	public static Logger logger = LoggerFactory.getLogger(StreamsAvroApp.class);

	public static void main(String[] args) {
		SpringApplication.run(StreamsAvroApp.class, args);
	}

	@Autowired
	public void buildTopology(StreamsBuilder builder, KafkaProperties kafkaProperties) {
		Map<String, Object> streamProps = kafkaProperties.buildStreamsProperties();

		SpecificAvroSerde<UserId> idSerde = avroSerde(streamProps, true);
		SpecificAvroSerde<User> userSerde = avroSerde(streamProps, false);

		builder.stream("users", Consumed.with(idSerde, userSerde))
			.foreach((key, user) -> logger.info("{} -> {}", key, user));
	}

	private static <T extends SpecificRecord> SpecificAvroSerde<T> avroSerde(
		Map<String, Object> streamProps, boolean forKeys
	) {
		SpecificAvroSerde<T> serde = new SpecificAvroSerde<>();
		serde.configure(streamProps, forKeys);
		return serde;
	}
}
