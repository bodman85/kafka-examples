package com.luxoft.lmd.springcloudstream;

import com.luxoft.lmd.springcloudstream.service.ContractorSpendingCalculator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.util.Map;

@RestController
public class SpendingsEndpoint {
	@Autowired private ContractorSpendingCalculator contractorSpendingCalculator;

	@GetMapping
	public Mono<Map<String, ? extends Number>> fetchAll() {
		return Mono.just(contractorSpendingCalculator.fetchAll());
	}
}
