package com.luxoft.lmd.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.time.ZoneId;

class PageClickTimestampExtractor implements TimestampExtractor {
	@Override public long extract(
		ConsumerRecord<Object, Object> record,
		long partitionTime
	) {
		return ((PageClick) record.value())
			.getDate()
			.atZone(ZoneId.systemDefault())
			.toInstant()
			.toEpochMilli();
	}
}
