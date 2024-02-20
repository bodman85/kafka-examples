package com.luxoft.lmd.kafka;

import com.google.common.collect.ImmutableMap;
import com.luxoft.lmd.domain.Gender;
import com.luxoft.lmd.domain.User;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import net.datafaker.Faker;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.Random;

@Component
public class AppRunner implements ApplicationRunner {
	private final Random random = new Random();
	private final Faker faker = new Faker();

	@Override
	public void run(ApplicationArguments args) throws Exception {
		var config =
			ImmutableMap.<String, Object>builder()
				.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
				.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
				.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class)
				.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8085")
				.build();

		try (var producer = new KafkaProducer<String, SpecificRecord>(config)) {
			var user = generateFakeUser();
			var record = new ProducerRecord<String, SpecificRecord>("users", user);
			producer.send(record).get();
		}
	}

	private User generateFakeUser() {
		return User.newBuilder()
			.setFirstName(faker.name().firstName())
			.setLastName(faker.name().lastName())
			.setNickname(faker.funnyName().name())
			.setAge(random.nextInt(100) + 1)
			.setGender(Gender.values()[random.nextInt(Gender.values().length)])
			.build();
	}
}
