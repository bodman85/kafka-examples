package com.luxoft.lmd.kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class Ex02PeekingApp {
	public static Logger logger = LoggerFactory.getLogger(Ex02PeekingApp.class);

	public static void main(String[] args) throws IOException {
		var properties =
			Map.of(
				StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
				StreamsConfig.APPLICATION_ID_CONFIG, "app01",
				StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName(),
				StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName()
			);

		// we will be creating both producers and consumers
		// potentially we need to set different settings for those
		// StreamsConfig allows for that with
		// StreamsConfig.consumerPrefix() and StreamsConfig.producerPrefix()
		var config = new StreamsConfig(properties);

		var topology = buildTopology();

		var kafkaStreams = new KafkaStreams(topology, config);

		// non blocking
		kafkaStreams.start();

		System.in.read();

		kafkaStreams.close();
	}

	private static Topology buildTopology() {
		var builder = new StreamsBuilder();

		builder.<String, String>stream("input")
			.peek((key, value) -> logger.info("<- {}:{}", key, value))
			.to("output");

		return builder.build();
	}
}
