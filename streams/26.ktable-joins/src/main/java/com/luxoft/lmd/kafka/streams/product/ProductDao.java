package com.luxoft.lmd.kafka.streams.product;

import com.luxoft.lmd.kafka.streams.ProductType;
import jakarta.annotation.PostConstruct;
import lombok.Getter;
import net.datafaker.Faker;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Random;

@Component

public class ProductDao {
	private Faker faker = new Faker();
	private Random random = new Random();
	@Getter private List<ProductEntity> products;

	@PostConstruct
	public void init() {
		this.products =
			faker
				.collection(
					() ->
						new ProductEntity(
							faker.internet().uuid(),
							faker.code().ean13(),
							randomProductType(),
							faker.commerce().productName()
						)
				).len(100)
				.generate();
	}

	private String randomProductType() {
		return ProductType.values()[random.nextInt(ProductType.values().length)].name();
	}

	public ProductEntity randomProduct() {
		return products.get(random.nextInt(products.size()));
	}
}
