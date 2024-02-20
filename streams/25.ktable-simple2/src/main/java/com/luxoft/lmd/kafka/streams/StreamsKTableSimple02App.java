package com.luxoft.lmd.kafka.streams;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.Map;

@EnableScheduling
@EnableKafkaStreams
@SpringBootApplication
public class StreamsKTableSimple02App {
	public static Logger logger = LoggerFactory.getLogger(StreamsKTableSimple02App.class);

	public static void main(String[] args) {
		SpringApplication.run(StreamsKTableSimple02App.class, args);
	}

	@Bean public KafkaAdmin.NewTopics appTopics() {
		return new KafkaAdmin.NewTopics(
			TopicBuilder.name("user.profiles.01").partitions(6).build()
		);
	}

	@Autowired
	public void buildTopology(StreamsBuilder builder, KafkaProperties kafkaProperties) {
		Serde<UserProfile> userProfileSerde =
			avroSerde(kafkaProperties.buildStreamsProperties(), false);

		builder.stream(
				"user.profiles.01",
				Consumed.with(Serdes.String(), userProfileSerde)
			).groupByKey(
				Grouped.with(
					"userProfile-modification-counts",
					Serdes.String(), userProfileSerde
				)
			)
			.count(Materialized.as("profiles-change-count-store"));
	}

	private static <T extends SpecificRecord> Serde<T> avroSerde(Map<String, Object> streamProps, boolean forKey) {
		Serde<T> purchaseSerde = new SpecificAvroSerde<T>();
		purchaseSerde.configure(streamProps, forKey);
		return purchaseSerde;
	}
}
