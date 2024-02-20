package com.luxoft.lmd.kafka.streams;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TimeWindows;
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

import java.time.Duration;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;

@EnableKafkaStreams
@SpringBootApplication
public class StreamsWindowingApp {
	public static Logger logger = LoggerFactory.getLogger(StreamsWindowingApp.class);

	public static void main(String[] args) {
		SpringApplication.run(StreamsWindowingApp.class, args);
	}

	@Bean public KafkaAdmin.NewTopics appTopics() {
		return new KafkaAdmin.NewTopics(
			TopicBuilder.name("pageclicks.01").partitions(6).build()
		);
	}

	@Autowired
	public void buildTopology(StreamsBuilder builder, KafkaProperties kafkaProperties) {
		DateTimeFormatter dateFormatter =
			DateTimeFormatter.ofPattern("yyyy-MM-dd")
				.withZone(ZoneId.systemDefault());

		Map<String, Object> streamProps = kafkaProperties.buildStreamsProperties();
		Serde<PageClick> pageClickSerde = avroSede(streamProps, false);

		KStream<String, PageClick> stream =
			builder.stream("pageclicks.01",
				Consumed.with(Serdes.String(), pageClickSerde)
			);

		stream
			//.peek((key, value) -> logger.info(value.toString()))
			.groupBy((
					key, value) -> value.getUrl(),
				Grouped.with(Serdes.String(), pageClickSerde)
			)
			.windowedBy(
				TimeWindows.ofSizeWithNoGrace(Duration.ofDays(1))
			)
			.count()
			.toStream()
			.foreach(
				(windowKey, count) ->
					logger.info("[{} - {}] {} -> {}",
						dateFormatter.format(windowKey.window().startTime()),
						dateFormatter.format(windowKey.window().endTime()),
						windowKey.key(),
						count
					)
			);
	}

	private static <T extends SpecificRecord> SpecificAvroSerde<T> avroSede(
		Map<String, Object> streamProps, boolean forKeys
	) {
		SpecificAvroSerde<T> serde = new SpecificAvroSerde<>();
		serde.configure(streamProps, forKeys);
		return serde;
	}
}
