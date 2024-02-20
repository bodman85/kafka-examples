package com.luxoft.lmd.kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class MyTopology {
	@Autowired
	public void setupTopology(StreamsBuilder builder) {
		var stringSerde = Serdes.String();

		builder.stream("names", Consumed.with(stringSerde, stringSerde))
			.mapValues(value -> value.toLowerCase())
			.to("names.lowercased", Produced.with(stringSerde, stringSerde));
	}
}
