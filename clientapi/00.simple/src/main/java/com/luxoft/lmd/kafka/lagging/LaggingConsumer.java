package com.luxoft.lmd.kafka.lagging;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class LaggingConsumer {
	private final String bootstrapServers = "localhost:9092, localhost:9093";
	private final Logger logger = LoggerFactory.getLogger(getClass());

	public static void main(String[] args) throws Exception {
		// run with BasicProducer and BasicConsumer
		// see BasicConsumer and LaggingConsumer entering a very frequent rebalance
		new LaggingConsumer().run();
	}

	public void run() throws Exception {
		Map<String, Object> config =
			Map.of(
				ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
				// make sure you understand the meaning of these
				ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
				ConsumerConfig.GROUP_ID_CONFIG, "group.01",
				ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 5000
			);

		try (
			var consumer = new KafkaConsumer<>(
				config,
				new StringDeserializer(),
				new StringDeserializer()
			)) {
			consumer.subscribe(List.of("names"));

			while (true) {
				ConsumerRecords<String, String> records =
					consumer.poll(Duration.ofSeconds(2));

				records.forEach(record ->
					logger.info("<- got record [{}] key='{}', value='{}'",
						record.topic(),
						record.key(),
						record.value())
				);

				logger.info("long processing...");
				Thread.sleep(10000);
				logger.info("done.");
			}

		}
	}
}
