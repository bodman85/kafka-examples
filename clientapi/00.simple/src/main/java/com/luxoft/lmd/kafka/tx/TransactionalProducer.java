package com.luxoft.lmd.kafka.tx;

import net.datafaker.Faker;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class TransactionalProducer {
	private final String bootstrapServers = "localhost:9092, localhost:9093";
	private final Logger logger = LoggerFactory.getLogger(getClass());
	private final Faker faker = new Faker();

	public static void main(String[] args) throws Exception {
		new TransactionalProducer().run();
	}

	public void run() throws Exception {
		// keeping things in one place and simple - not using dependency injection
		// you would do that for any kind of production code, but you wouldn't be using
		// Kafka Java API directly anyway - use Spring for Kafka
		// and get this automatically set up

		Map<String, Object> config =
			Map.of(
				ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
				// this is REALLY important
				ProducerConfig.TRANSACTIONAL_ID_CONFIG, "producer01-" + UUID.randomUUID()
				// "enable.idempotence" turned on by default in Kafka 3+
			);

		try (var producer =
			     new KafkaProducer<>(
				     config,
				     new StringSerializer(),
				     new StringSerializer())
		) {
			// called only once during producer init
			producer.initTransactions();

			int counter = 0;
			while (true) {
				sendMessage(producer, ++counter);
				Thread.sleep(1000);
			}
		}
	}

	private void sendMessage(KafkaProducer<String, String> producer, int counter)
		throws ExecutionException, InterruptedException {
		// sending messages in single transaction
		try {
			producer.beginTransaction();

			var key = counter + "--" + UUID.randomUUID();
			var name = faker.funnyName().name();
			var address = faker.address().fullAddress();

			logger.info("sending [id='{}', name='{}', address='{}']", key, name, address);
			var event1 = new ProducerRecord<>("names", key, name);
			var event2 = new ProducerRecord<>("addresses", key, address);

			// no CompletableFuture in Kafka API :(
			Future<RecordMetadata> result1 = producer.send(event1);
			// we could wait for both messages - that would be more performant,
			// but we might have a problem seeing what happens when an exception
			// occurs between sending those messages
			RecordMetadata metadata1 = result1.get();

			logger.info("\n{}\nhas been sent to:\n{}:{}:{} at {}\n",
				event1,
				metadata1.topic(),
				metadata1.partition(),
				metadata1.offset(),
				new Date(metadata1.timestamp()));

			//if (1 == 1) throw new RuntimeException("a processing exception ocurred");

			Future<RecordMetadata> result2 = producer.send(event2);
			RecordMetadata metadata2 = result2.get();

			logger.info("\n{}\nhas been sent to:\n{}:{}:{} at {}\n",
				event2,
				metadata2.topic(),
				metadata2.partition(),
				metadata2.offset(),
				new Date(metadata2.timestamp()));

			producer.commitTransaction();

		} catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
			throw new RuntimeException("unrecoverable kafka exception, need to close the producer and exit!", e);
		} catch (KafkaException anyOther) {
			producer.abortTransaction();
			// and you may try again
		}
	}
}
