package com.luxoft.lmd.kafka.simple;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class RebalanceListeningConsumer {
	private final String bootstrapServers = "localhost:9092, localhost:9093";
	private final Logger logger = LoggerFactory.getLogger(getClass());

	public static void main(String[] args) throws Exception {
		new RebalanceListeningConsumer().run();
	}

	public void run() throws Exception {
		Map<String, Object> config =
			Map.of(
				ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
				ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
				ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
				// make sure you understand the meaning of these
				ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
				ConsumerConfig.GROUP_ID_CONFIG, "group.reb",
				ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5,
				ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 240_000
			);

		var rebalanceListener = new ConsumerRebalanceListener() {
			@Override public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
				logger.info("partitions revoked: {}", partitions);
				logger.info("WE HAVE A CHANCE TO SAVE OUR STATE BEFORE GIVING PARTITIONS AWAY");
			}

			@Override public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
				logger.info("partitions assigned: {}", partitions);
				logger.info("WE HAVE A CHANCE TO RESTORE OUR APP STATE BEFORE WE START WORKING WITH THOSE");
			}
		};

		try (var consumer = new KafkaConsumer<String, String>(config)) {
			consumer.subscribe(
				List.of("names"),
				rebalanceListener
			);

			while (true) {
				ConsumerRecords<String, String> records =
					consumer.poll(Duration.ofSeconds(2));

				records.forEach(record ->
					logger.info("<- got record [{}] key='{}', value='{}'",
						record.topic(),
						record.key(),
						record.value())
				);
			}
		}
	}
}
