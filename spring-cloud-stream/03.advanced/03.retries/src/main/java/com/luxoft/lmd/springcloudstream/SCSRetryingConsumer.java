package com.luxoft.lmd.springcloudstream;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@EnableKafka
@SpringBootApplication
public class SCSRetryingConsumer {
	public static void main(String[] args) {
		SpringApplication.run(SCSRetryingConsumer.class, args);
	}
}
