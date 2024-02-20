package com.luxoft.lmd.kafka.seek;

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
import java.util.stream.Collectors;

public class SeekConsumer {
	private final String bootstrapServers = "localhost:9092, localhost:9093";
	private final Logger logger = LoggerFactory.getLogger(getClass());

	public static void main(String[] args) throws Exception {
		new SeekConsumer().run();
	}

	public void run() throws Exception {
		Map<String, Object> config =
			Map.of(
				ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers
			);

		try (
			var consumer =
				new KafkaConsumer<>(
					config,
					new StringDeserializer(),
					new StringDeserializer()
				)
		) {
			List<PartitionInfo> partitions = consumer.partitionsFor("names");

			List<TopicPartition> topicPartitions =
				partitions.stream()
					.map(partitionInfo ->
						new TopicPartition(
							partitionInfo.topic(),
							partitionInfo.partition()
						)
					).collect(Collectors.toList());

			consumer.assign(topicPartitions);

			Map<TopicPartition, Long> topicPartitionToEndOffsets =
				consumer.endOffsets(topicPartitions);

			// you have also these options:
			// consumer.seekToBeginning(topicPartitions);
			// consumer.seekToEnd(topicPartitions);

			topicPartitionToEndOffsets.forEach(
				(topicPartition, endOffset) -> {
					logger.info("end offset for {} -> {}", topicPartition, endOffset);
					long startOffset = Math.max(endOffset - 10, 0);
					consumer.seek(topicPartition, startOffset);
				}
			);

			while (true) {
				ConsumerRecords<String, String> records =
					consumer.poll(Duration.ofSeconds(2));

				records.forEach(record ->
					logger.info("<- got record [{}:{}:{}] key='{}', value='{}'",
						record.topic(),
						record.partition(),
						record.offset(),
						record.key(),
						record.value())
				);
			}
		}
	}
}
