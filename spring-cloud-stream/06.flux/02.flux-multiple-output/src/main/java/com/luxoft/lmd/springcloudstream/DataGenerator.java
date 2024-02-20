package com.luxoft.lmd.springcloudstream;

import net.datafaker.Faker;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.Stream;

@Component
public class DataGenerator implements Supplier<Flux<Purchase>> {
	private final Faker faker = new Faker();
	private final Random random = new Random();

	@Override
	public Flux<Purchase> get() {
		return Flux.interval(Duration.ofMillis(250))
			.zipWith(
				Flux.fromStream(
					Stream.generate(() ->
						new Purchase(
							faker.idNumber().valid(),
							faker.idNumber().validSvSeSsn(),
							Purchase.Type.values()[random.nextInt(Purchase.Type.values().length)],
							faker.commerce().productName(),
							faker.number().randomDouble(2, 5, 500)
						))
				)
			)
			.map(tuple -> tuple.getT2());
	}
}
