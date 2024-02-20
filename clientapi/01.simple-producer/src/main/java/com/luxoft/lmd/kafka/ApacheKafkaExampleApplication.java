package com.luxoft.lmd.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ApacheKafkaExampleApplication {
	public static void main(String[] args) {
		SpringApplication.run(ApacheKafkaExampleApplication.class, args);
		// search for ApplicationRunner interface implementations - the actual app logic is there
	}
}
