package com.luxoft.lmd.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@EnableKafka
@SpringBootApplication
public class ProducerNoConfigApp {
	public static void main(String[] args) {
		SpringApplication.run(ProducerNoConfigApp.class, args);
	}
}
