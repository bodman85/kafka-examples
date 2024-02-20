package com.luxoft.lmd.kafka.streams;

import com.luxoft.lmd.kafka.shared.NamesGenerationConfiguration;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@EnableKafkaStreams
@SpringBootApplication
@Import({NamesGenerationConfiguration.class})
public class StreamsGroupingApp {
	public static Logger logger = LoggerFactory.getLogger(StreamsGroupingApp.class);

	public static void main(String[] args) {
		SpringApplication.run(StreamsGroupingApp.class, args);
	}

	//@Autowired
	public void basicGroupingTopology(StreamsBuilder builder) {
		builder.<String, String>stream("names")
			// check how easy it is to break that topology
			//.filter((key, value) -> value.length() > 2)
			.selectKey((key, value) -> value.length())
			.groupByKey(Grouped.with(Serdes.Integer(), Serdes.String()))
			.count()
			.toStream()
			.peek(
				(nameLength, occurrences) ->
					logger.info("name of length {} occured up till now {} times", nameLength, occurrences)
			)
			.to(
				"names.lengths.counts",
				Produced.with(Serdes.Integer(), Serdes.Long())
			);
	}

	// @Autowired
	public void modificationSafeGroupingTopology(StreamsBuilder builder) {
		builder.<String, String>stream("names")
			.filter((key, value) -> value.length() > 2)

			.selectKey(
				(key, value) -> value.length()
			)

			.groupByKey(
				Grouped.with("names.by.length", Serdes.Integer(), Serdes.String())
			)

			.count(
				Named.as("name.counts"),
				Materialized.as("name.counts.store")
			)

			.toStream()

			.peek(
				(nameLength, occurrences) ->
					logger.info("name of length {} occured up till now {} times", nameLength, occurrences)
			)

			.to("names.lengths.counts", Produced.with(Serdes.Integer(), Serdes.Long()));
	}


}
