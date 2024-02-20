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

@RestController
public class UserProfilesEmailEndpoint {
	private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;
	private ReadOnlyKeyValueStore<String, UserProfile> profilesStore;

	UserProfilesEmailEndpoint(StreamsBuilderFactoryBean streamsBuilderFactoryBean) {
		this.streamsBuilderFactoryBean = streamsBuilderFactoryBean;
	}

	@EventListener(ContextRefreshedEvent.class)
	public void onContextRefreshed(ContextRefreshedEvent event) {
		KafkaStreams kafkaStreams = streamsBuilderFactoryBean.getKafkaStreams();
		this.profilesStore =
			kafkaStreams.store(
				StoreQueryParameters.fromNameAndType(
					"profiles-store", QueryableStoreTypes.keyValueStore()
				)
			);
	}

	@GetMapping("{userId}/email")
	public String resolveEmail(@PathVariable String userId) {
		UserProfile userProfile = profilesStore.get(userId);
		if (profilesStore == null)
			return null;

		return userProfile.getEmail();
	}
}
