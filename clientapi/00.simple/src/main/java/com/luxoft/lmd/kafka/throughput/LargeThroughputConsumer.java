package com.luxoft.lmd.kafka.throughput;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class LargeThroughputConsumer {
	private final String bootstrapServers = "localhost:9092, localhost:9093";
	private final Logger logger = LoggerFactory.getLogger(getClass());

	public static void main(String[] args) throws Exception {
		new LargeThroughputConsumer().run();
	}

	public void run() throws Exception {
		Map<String, Object> config =
			Map.of(
				ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
				// make sure you understand the meaning of these
				ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
				ConsumerConfig.GROUP_ID_CONFIG, "thpt",
				ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 10_000,
				ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 3000
			);

		try (
			var consumer =
				new KafkaConsumer<>(
					config,
					new StringDeserializer(),
					new StringDeserializer())) {

			consumer.subscribe(List.of("throughput"));

			while (true) {
				ConsumerRecords<String, String> records =
					consumer.poll(Duration.ofSeconds(2));

				logger.info("<- got {} records at once", records.count());

/*				records.forEach(record -> {
						logger.info("<- got record [{}] key='{}', value='{}'",
							record.topic(),
							record.key(),
							new String(record.value()));
					}
				);*/
			}

		}
	}
}
