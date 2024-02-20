package com.luxoft.lmd.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableKafka
@EnableScheduling
@SpringBootApplication
public class AvroApp {
	public static void main(String[] args) {
		SpringApplication.run(AvroApp.class, args);
	}
}
