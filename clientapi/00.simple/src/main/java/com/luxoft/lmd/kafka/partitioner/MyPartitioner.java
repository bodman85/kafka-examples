package com.luxoft.lmd.kafka.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

public class MyPartitioner implements Partitioner {
	@Override public void configure(Map<String, ?> configs) {
		// initial setup
	}

	@Override
	public int partition(String topic,
	                     Object key,
	                     byte[] keyBytes,
	                     Object value,
	                     byte[] valueBytes,
	                     Cluster cluster) {
		// everything goes to partition 0 no matter what the key is
		// fill that in with your own implementation
		return 0;
	}

	@Override public void close() {
		// nothing to free up in this implementation
	}
}
