package com.luxoft.lmd.kafka.streams;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@RestController
public class UserProfilesChangeCountEndpoint {
	private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;
	private ReadOnlyKeyValueStore<String, Long> profilesChangeCountStore;

	UserProfilesChangeCountEndpoint(StreamsBuilderFactoryBean streamsBuilderFactoryBean) {
		this.streamsBuilderFactoryBean = streamsBuilderFactoryBean;
	}

	@EventListener(ContextRefreshedEvent.class)
	public void onContextRefreshed(ContextRefreshedEvent event) {
		KafkaStreams kafkaStreams = streamsBuilderFactoryBean.getKafkaStreams();

		this.profilesChangeCountStore =
			kafkaStreams.store(
				StoreQueryParameters.fromNameAndType(
					"profiles-change-count-store",
					QueryableStoreTypes.keyValueStore()
				)
			);
	}

	@GetMapping("{userId}/changeCount")
	public Long resolveChangeCount(@PathVariable String userId) {
		return profilesChangeCountStore.get(userId);
	}

	@GetMapping("all")
	public Map<String, Long> fetchAll() {
		Map<String, Long> map = new HashMap<>();

		try (var iterator = profilesChangeCountStore.all()) {
			iterator.forEachRemaining(stringLongKeyValue ->
				map.put(stringLongKeyValue.key, stringLongKeyValue.value)
			);
		}

		return map;
	}
}
