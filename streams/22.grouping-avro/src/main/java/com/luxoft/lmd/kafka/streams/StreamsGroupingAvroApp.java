package com.luxoft.lmd.kafka.streams;

import com.luxoft.lmd.kafka.shared.NamesGenerationConfiguration;
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
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.EnableKafkaStreams;

import java.util.Map;

@EnableKafkaStreams
@SpringBootApplication
@Import({NamesGenerationConfiguration.class})
public class StreamsGroupingAvroApp {
	public static Logger logger = LoggerFactory.getLogger(StreamsGroupingAvroApp.class);

	public static void main(String[] args) {
		SpringApplication.run(StreamsGroupingAvroApp.class, args);
	}

	@Autowired
	public void buildTopology(StreamsBuilder builder, KafkaProperties kafkaProperties) {
		Map<String, Object> streamProps = kafkaProperties.buildStreamsProperties();

		Serde<Purchase> purchaseSerde = avroSerde(streamProps, false);

		KStream<String, Purchase> purchasesStream =
			builder.stream("purchases.01",
				Consumed.with(Serdes.String(), purchaseSerde)
			);

		// example01
		// count number of purchases per customerId
		purchasesStream
			.groupByKey()
			.count()
			.toStream()
			.to(
				"purchases.01.count_by_customerId",
				Produced.with(Serdes.String(), Serdes.Long()));

		// example02
		// count number of purchases by country
		purchasesStream
			.groupBy((key, purchase) -> purchase.getCountryCode())
			.count()
			.toStream()
			.to("purchases.02.count_by_countryCode",
				Produced.with(Serdes.String(), Serdes.Long())
			);

		// example03
		// purchases value by customerId
		purchasesStream
			.mapValues((readOnlyKey, purchase) -> purchase.getPrice() * purchase.getQuantity())
			.groupByKey()
			.reduce((quantity1, quantity2) -> quantity1 + quantity2)
			.toStream()
			.to(
				"purchases.01.total_value_by_customerId",
				Produced.with(Serdes.String(), Serdes.Double())
			);

		// example04
		// calculate total purchase value and quantity sold for each product
		Serde<ProductPurchaseTotals> purchaseTotalsSerde = avroSerde(streamProps, false);

		purchasesStream
			.groupBy((key, purchase) -> purchase.getProductId())
			.aggregate(
				() -> ProductPurchaseTotals.newBuilder().build(),
				new ProductPurchaseTotalsAggregator()
			)
			.toStream()
			.to(
				"purchases.01.totals_by_productId",
				Produced.with(Serdes.String(), purchaseTotalsSerde)
			);
	}

	//@Autowired
	public void buildNamedTopology(StreamsBuilder builder, KafkaProperties kafkaProperties) {
		Map<String, Object> streamProps = kafkaProperties.buildStreamsProperties();

		Serde<Purchase> purchaseSerde = avroSerde(streamProps, false);

		KStream<String, Purchase> purchasesStream =
			builder.stream(
				"purchases.01",
				Consumed.with(Serdes.String(), purchaseSerde).withName("purchases")
			);

		// example01
		// count number of purchases per customerId
		purchasesStream
			.groupByKey(Grouped.as("purchases_grouping"))
			.count(
				Named.as("purchase_count_by_customerId"),
				Materialized.as("purchases_count_by_customerId_store")
			)
			.toStream()
			.to(
				"purchases.01.count_by_customerId",
				Produced.with(Serdes.String(), Serdes.Long())
			);

		// example02
		// count number of purchases by country
		purchasesStream
			.groupBy((key, purchase) -> purchase.getCountryCode(),
				Grouped.as("purchases_by_countryCode"))
			.count(
				Named.as("counts_by_countryCode"),
				Materialized.as("counts_by_countryCodeStore"))
			.toStream()
			.to(
				"purchases.02.count_by_countryCode",
				Produced.with(Serdes.String(), Serdes.Long())
			);

		// example03
		// purchases value by customerId
		purchasesStream
			.mapValues(
				(readOnlyKey, purchase) -> purchase.getPrice() * purchase.getQuantity(),
				Named.as("map_purchase_to_value")
			)
			.groupByKey()
			.reduce(
				(value1, value2) -> value1 + value2,
				Named.as("total_purchase_value_reducer"),
				Materialized.as("total_purchase_value_store")
			)
			.toStream()
			.to(
				"purchases.01.total_value_by_customerId",
				Produced.with(Serdes.String(), Serdes.Double())
			);

		// example04
		// calculate total purchase value and quantity sold for each product

		Serde<ProductPurchaseTotals> purchaseTotalsSerde = avroSerde(streamProps, false);

		purchasesStream
			.groupBy(
				(key, purchase) -> purchase.getProductId(),
				Grouped.as("purchases_by_productId")
			)
			.aggregate(
				() -> ProductPurchaseTotals.newBuilder().build(),
				new ProductPurchaseTotalsAggregator(),
				Named.as("product_purchase_totals_aggregator"),
				Materialized.as("product_purchase_totals_store")
			)
			.toStream()
			.to(
				"purchases.01.totals_by_productId",
				Produced.with(Serdes.String(), purchaseTotalsSerde)
			);
	}

	private static <T extends SpecificRecord> SpecificAvroSerde<T> avroSerde(
		Map<String, Object> streamProps, boolean forKeys
	) {
		SpecificAvroSerde<T> serde = new SpecificAvroSerde<>();
		serde.configure(streamProps, forKeys);
		return serde;
	}
}
