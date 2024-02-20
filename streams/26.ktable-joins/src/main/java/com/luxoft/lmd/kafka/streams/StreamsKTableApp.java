package com.luxoft.lmd.kafka.streams;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
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
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.Map;

@EnableScheduling
@EnableKafkaStreams
@SpringBootApplication
public class StreamsKTableApp {
	public static Logger logger = LoggerFactory.getLogger(StreamsKTableApp.class);

	public static void main(String[] args) {
		SpringApplication.run(StreamsKTableApp.class, args);
	}

	@Bean public KafkaAdmin.NewTopics appTopics() {
		return new KafkaAdmin.NewTopics(
			TopicBuilder.name("purchases.02").partitions(6).build(),
			TopicBuilder.name("purchases.02.enriched").partitions(6).build(),
			TopicBuilder.name("customers").partitions(6).compact().build(),
			TopicBuilder.name("products").partitions(6).compact().build(),

			TopicBuilder.name("purchases.02.totals_by_country").compact().build()
		);
	}

	@Autowired
	public void buildEnrichingTopology(StreamsBuilder builder, KafkaProperties kafkaProperties) {
		Map<String, Object> streamProps = kafkaProperties.buildStreamsProperties();

		Serde<Purchase> purchaseSerde = avroSerde(streamProps);
		Serde<PurchaseEnriched> purchaseEnrichedSerdeSerde = avroSerde(streamProps);

		KStream<Void, Purchase> purchasesStream =
			builder.stream("purchases.02", Consumed.with(Serdes.Void(), purchaseSerde))
			//	.peek((key, value) -> logger.info("purchase: {}", value))
			;

		KTable<String, Customer> customersKTable = customersKTable(builder, kafkaProperties);
		KTable<String, Product> productsKTable = productsKTable(builder, kafkaProperties);

		purchasesStream
			.selectKey((key, purchase) -> purchase.getProductId())
			.repartition(Repartitioned.with(Serdes.String(), purchaseSerde))
			.join(productsKTable, new PurchaseToProductJoiner())

			.selectKey((key, purchase) -> purchase.getCustomerId())
			.repartition(Repartitioned.with(Serdes.String(), purchaseEnrichedSerdeSerde))
			.join(customersKTable, new PurchaseEnrichedToCustomerJoiner())

			.peek(
				(key, value) -> logger.info("purchase enriched: {} -> {}", key, value)
			)

			.to("purchases.02.enriched", Produced.with(Serdes.String(), purchaseEnrichedSerdeSerde));
	}

	@Autowired
	public void buildTotalsTopology(StreamsBuilder builder, KafkaProperties kafkaProperties) {
		Map<String, Object> streamProps = kafkaProperties.buildStreamsProperties();
		Serde<PurchaseEnriched> purchaseEnrichedSerdeSerde = avroSerde(streamProps);
		Serde<CountryPurchaseTotals> countryPurchaseTotalsSerde = avroSerde(streamProps);

		builder
			.stream(
				"purchases.02.enriched",
				Consumed.with(Serdes.String(), purchaseEnrichedSerdeSerde))
			.map(
				(key, value) ->
					new KeyValue<>(
						value.getCountryCode(),
						value.getQuantity() * value.getPrice()
					)
			)
			.groupByKey(Grouped.with(Serdes.String(), Serdes.Double()))
			.reduce(Double::sum)
			.toStream()

			.peek((countryCode, sales) -> logger.info("country sales update {}: {}", countryCode, sales))
			.mapValues(
				(readOnlyKey, value) ->
					CountryPurchaseTotals.newBuilder()
						.setCountryCode(readOnlyKey)
						.setTotalValue(value).build()
			)
			.to(
				"purchases.02.totals_by_country",
				Produced.with(Serdes.String(), countryPurchaseTotalsSerde)
			);
	}


	private KTable<String, Customer> customersKTable(StreamsBuilder builder, KafkaProperties kafkaProperties) {
		Map<String, Object> streamProps = kafkaProperties.buildStreamsProperties();
		Serde<Customer> customerSerde = avroSerde(streamProps);

		KTable<String, Customer> customers =
			builder.table("customers", Consumed.with(Serdes.String(), customerSerde));

		return customers;
	}

	private KTable<String, Product> productsKTable(StreamsBuilder builder, KafkaProperties kafkaProperties) {
		Map<String, Object> streamProps = kafkaProperties.buildStreamsProperties();
		Serde<Product> productSerde = avroSerde(streamProps);

		KTable<String, Product> products =
			builder.table("products", Consumed.with(Serdes.String(), productSerde));

		return products;
	}

	private static <T extends SpecificRecord> Serde<T> avroSerde(Map<String, Object> streamProps) {
		Serde<T> purchaseSerde = new SpecificAvroSerde<T>();
		purchaseSerde.configure(streamProps, false);
		return purchaseSerde;
	}
}
