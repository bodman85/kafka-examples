package com.luxoft.lmd.springcloudstream;

import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class WordService {
	private final StreamBridge streamBridge;

	public WordService(StreamBridge streamBridge) {
		this.streamBridge = streamBridge;
	}

	@Transactional
	public void publishTwoWordsInTransaction(String input) {
		String destinationName = "words";
		streamBridge.send(destinationName, input + ":1");

		if (input.equals("throw"))
			throw new RuntimeException("we have a processing exceptions, that Kafka transaction should be rolled back");

		streamBridge.send(destinationName, input + ":2");
	}
}
