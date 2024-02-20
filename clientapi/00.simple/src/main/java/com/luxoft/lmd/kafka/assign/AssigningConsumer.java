package com.luxoft.lmd.kafka.assign;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class AssigningConsumer {
	private final String bootstrapServers = "localhost:9092, localhost:9093";
	private final Logger logger = LoggerFactory.getLogger(getClass());

	public static void main(String[] args) throws Exception {
		new AssigningConsumer().run();
	}

	public void run() throws Exception {
		Map<String, Object> config =
			Map.of(
				ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
				// make sure you understand the meaning of these
				ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
				ConsumerConfig.GROUP_ID_CONFIG, "group.assigning"
			);

		try (
			var consumer =
				new KafkaConsumer<>(
					config,
					new StringDeserializer(),
					new StringDeserializer()
				)) {

			Map<String, List<PartitionInfo>> topology = consumer.listTopics();
			logger.info("topology: {}", topology);

			List<TopicPartition> selfAssignedPartitions =
				List.of(
					new TopicPartition("names", 0)
				);

			// we switch off automatic partition assignment and rebalance support
			consumer.assign(selfAssignedPartitions);

			while (true) {
				// we are still using __consumer_offsets to automatically keep track of processed messages
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
