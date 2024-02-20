package com.luxoft.lmd.springcloudstream;

import com.luxoft.lmd.springcloudstream.model.CreditCardTransaction;
import com.luxoft.lmd.springcloudstream.util.CreditCardTransactionBuilder;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;

import java.time.Duration;
import java.util.function.Supplier;
import java.util.stream.Stream;

@Component
public class TransactionGenerator implements Supplier<Flux<CreditCardTransaction>> {
	private final CreditCardTransactionBuilder creditCardTransactionBuilder = new CreditCardTransactionBuilder();

	@Override
	public Flux<CreditCardTransaction> get() {
		return Flux.interval(Duration.ofMillis(250))
			.zipWith(
				Flux.fromStream(
					Stream.generate(creditCardTransactionBuilder::createFakeTransaction)
				)
			)
			.map(Tuple2::getT2);
	}
}
