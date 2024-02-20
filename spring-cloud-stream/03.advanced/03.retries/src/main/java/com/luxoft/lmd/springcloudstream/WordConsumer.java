package com.luxoft.lmd.springcloudstream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Random;
import java.util.function.Consumer;

@Component
public class WordConsumer implements Consumer<String> {
	private final Random random = new Random();
	private final Logger logger = LoggerFactory.getLogger(getClass());

	@Override public void accept(String word) {
		logger.info("<- {}", word);
		int exceptionPossibility = random.nextInt(100);
		if (exceptionPossibility < 10) {
			//logger.info("emulating fatal exception for {}", word);
			throw new IllegalArgumentException("fatal exception for " + word);
		}
		if (exceptionPossibility < 60) {
			//logger.info("emulating retryable exception for {}", word);
			throw new RuntimeException("retryable exception for " + word);
		}

		logger.info("!! successfully processed {}", word);
	}
}
