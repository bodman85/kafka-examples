package com.luxoft.lmd.springcloudstream;

import com.luxoft.lmd.springcloudstream.model.CreditCardTransaction;
import com.luxoft.lmd.springcloudstream.model.EmailNotificationRequest;
import net.datafaker.Faker;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.Map;
import java.util.function.Function;

@Component
public class TransactionProcessor implements Function<CreditCardTransaction, Message<EmailNotificationRequest>> {
	private final Logger logger = LoggerFactory.getLogger(getClass());
	private final Faker faker = new Faker();

	@Transactional
	@Override public Message<EmailNotificationRequest> apply(CreditCardTransaction creditCardTransaction) {
		String messageContent = buildMailContent(creditCardTransaction);

		logger.info(
			"generating email request for account: {}",
			creditCardTransaction.accountId()
		);

		return MessageBuilder.createMessage(
			createRequest(messageContent),
			new MessageHeaders(
				Map.of(
					"source", "credit-card-transactions"
				)
			)
		);
	}

	@NotNull
	private EmailNotificationRequest createRequest(String messageContent) {
		return new EmailNotificationRequest(
			faker.internet().emailAddress(),
			"Thank you for using our services!",
			messageContent
		);
	}

	@NotNull
	private String buildMailContent(CreditCardTransaction creditCardTransaction) {
		return new StringBuilder()
			.append("You have deducted ")
			.append(creditCardTransaction.amount())
			.append(" from your account, current balance: ")
			.append(faker.number().randomDouble(2, 0, 5000))
			.toString();
	}
}
