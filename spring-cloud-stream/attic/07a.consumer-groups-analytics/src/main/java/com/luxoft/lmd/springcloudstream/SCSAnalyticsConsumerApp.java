package com.luxoft.lmd.springcloudstream;

import com.luxoft.lmd.springcloudstream.service.ContractorSpendingCalculator;
import com.luxoft.lmd.springcloudstream.service.ContractorSpendingCalculatorImpl;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class SCSAnalyticsConsumerApp {
	@Bean
	public ContractorSpendingCalculator contractorSpendingCalculator() {
		return new ContractorSpendingCalculatorImpl();
	}

	public static void main(String[] args) {
		SpringApplication.run(SCSAnalyticsConsumerApp.class, args);
	}
}
