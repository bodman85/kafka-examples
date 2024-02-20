package com.luxoft.lmd.springcloudstream.util;

import com.luxoft.lmd.springcloudstream.model.CreditCardTransaction;
import net.datafaker.Faker;

import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class CreditCardTransactionBuilder {
	private record ContractorData(String name, String accountId) {
	}

	private final List<ContractorData> contractors;
	private final Random random = new Random();
	private final Faker faker = new Faker();

	public CreditCardTransactionBuilder() {
		contractors =
			Stream.generate(
					() ->
						new ContractorData(
							faker.funnyName().name(),
							faker.idNumber().valid()
						)
				).limit(30)
				.toList();

	}

	public CreditCardTransaction createFakeTransaction() {
		ContractorData contractor = randomContractor();
		return new CreditCardTransaction(
			UUID.randomUUID().toString(),
			contractor.accountId(),
			contractor.name(),
			faker.business().creditCardNumber(),
			faker.business().creditCardType(),
			faker.date().past(5000, TimeUnit.DAYS),
			faker.number().randomDouble(2, 100, 2000)
		);
	}

	private ContractorData randomContractor() {
		return contractors.get(random.nextInt(contractors.size()));
	}
}
