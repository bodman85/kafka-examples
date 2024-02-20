package com.luxoft.lmd.kafka.streams.product;

import com.luxoft.lmd.kafka.streams.Product;
import com.luxoft.lmd.kafka.streams.ProductType;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class ProductsPopulator implements ApplicationRunner {
	private final ProductDao products;
	private final KafkaTemplate<Object, Object> kafkaTemplate;


	public ProductsPopulator(ProductDao products, KafkaTemplate<Object, Object> kafkaTemplate) {
		this.products = products;
		this.kafkaTemplate = kafkaTemplate;
	}

	@Override public void run(ApplicationArguments args) {
		products.getProducts().forEach(product -> {
			var payload =
				Product.newBuilder()
					.setId(product.id())
					.setEan(product.ean())
					.setType(ProductType.valueOf(product.type()))
					.setName(product.name())
					.build();

			kafkaTemplate.send("products", payload.getId(), payload);
		});
	}
}
