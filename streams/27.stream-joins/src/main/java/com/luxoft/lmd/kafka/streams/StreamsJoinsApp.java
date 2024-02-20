package com.luxoft.lmd.kafka.streams;

import com.luxoft.lmd.kafka.stream.Coupon;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Map;

@EnableKafkaStreams
@SpringBootApplication
public class StreamsJoinsApp {
	public static Logger logger = LoggerFactory.getLogger(StreamsJoinsApp.class);

	public static void main(String[] args) {
		SpringApplication.run(StreamsJoinsApp.class, args);
	}

	@Bean public KafkaAdmin.NewTopics appTopics() {
		return new KafkaAdmin.NewTopics(
			TopicBuilder.name("coupon").partitions(3).build()
		);
	}

	@Autowired
	public void buildTopology(StreamsBuilder builder, KafkaProperties kafkaProperties) {
		Map<String, Object> streamProps = kafkaProperties.buildStreamsProperties();

		Serde<PurchaseEnriched> purchaseSerde = avroSerde(streamProps);
		Serde<Coupon> couponSerde = avroSerde(streamProps);

		Map<String, KStream<String, PurchaseEnriched>> branchedStreams =
			builder
				.stream(
					"purchases.02.enriched", Consumed.with(Serdes.String(), purchaseSerde)
				)
				.split(Named.as("productType-"))
				.branch(
					(key, value) -> value.getProductType() == ProductType.COFFEE,
					Branched.as("coffee")
				).branch(
					(key, value) -> value.getProductType() == ProductType.FOOD,
					Branched.as("food")
				).noDefaultBranch();

		KStream<String, PurchaseEnriched> coffeeStream = branchedStreams.get("productType-coffee");
		KStream<String, PurchaseEnriched> foodStream = branchedStreams.get("productType-food");

		coffeeStream.join(
				foodStream, (readOnlyKey, coffee, food) ->
					Coupon.newBuilder()
						.setCustomerId(readOnlyKey)
						.setExpires(LocalDateTime.now().plusDays(60))
						.setValue(calculateCouponValue(coffee, food))
						.build(),
				JoinWindows.ofTimeDifferenceAndGrace(Duration.ofMinutes(15), Duration.ofMinutes(3)),
				StreamJoined.with(Serdes.String(), purchaseSerde, purchaseSerde)
			).peek(
				(key, coupon) -> logger.info("coupon issued: {}", coupon)
			)
			.to("coupon", Produced.with(Serdes.String(), couponSerde));

	}

	private static double calculateCouponValue(PurchaseEnriched coffee, PurchaseEnriched food) {
		return 0.2 * (coffee.getQuantity() * coffee.getPrice() + food.getQuantity() * food.getPrice());
	}

	private static <T extends SpecificRecord> Serde<T> avroSerde(Map<String, Object> streamProps) {
		Serde<T> purchaseSerde = new SpecificAvroSerde<T>();
		purchaseSerde.configure(streamProps, false);
		return purchaseSerde;
	}
}
