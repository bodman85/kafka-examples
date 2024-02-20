package com.luxoft.lmd.springcloudstream;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class RestEndpoint {
	private final WordService wordService;

	public RestEndpoint(WordService wordService) {
		this.wordService = wordService;
	}

	@PostMapping
	public void publish(@RequestParam String word) {
		wordService.publishTwoWordsInTransaction(word);
	}
}
