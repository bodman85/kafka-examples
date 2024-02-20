package com.luxoft.lmd.springcloudstream;

import com.luxoft.lmd.springcloudstream.model.CreditCardTransaction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TransactionsEndpoint {
	@Autowired private StreamBridge streamBridge;

	@PostMapping
	public void register(@RequestBody CreditCardTransaction transaction) {
		streamBridge.send("transactions", transaction);
	}
}
