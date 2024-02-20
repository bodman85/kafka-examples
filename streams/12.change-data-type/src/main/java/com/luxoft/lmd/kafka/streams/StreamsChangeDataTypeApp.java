package com.luxoft.lmd.kafka.streams;

import com.luxoft.lmd.kafka.shared.QuotesGenerationConfiguration;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Named;
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

@EnableKafkaStreams
@SpringBootApplication
@Import(QuotesGenerationConfiguration.class)
public class StreamsChangeDataTypeApp {
	public static Logger logger = LoggerFactory.getLogger(StreamsChangeDataTypeApp.class);

	public static void main(String[] args) {
		SpringApplication.run(StreamsChangeDataTypeApp.class, args);
	}

	@Bean public NewTopic quotesLength() {
		return TopicBuilder.name("quotes.length").partitions(6).build();
	}

	@Autowired
	public void buildTopology(StreamsBuilder builder) {
		builder
			.stream(
				"quotes",
				Consumed.with(Serdes.String(), Serdes.String())
					.withName("quotes_input")
			)

			.peek((key, value) -> logger.info(" quote: [{}]: {}", key, value))

			.mapValues(
				(readOnlyKey, quote) -> quote.length(),
				Named.as("to_charcount")
			)

			.peek((key, value) -> logger.info("length: [{}]: {}", key, value))

			.to(
				"quotes.length",
				Produced.with(Serdes.String(), Serdes.Integer())
			);
	}
}
