package com.onlineinteract.workflow.domain.account.bus;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Component;

import com.onlineinteract.workflow.domain.account.AccountEvent;

import io.confluent.kafka.serializers.KafkaAvroSerializer;

@Component
public class Producer {
	private KafkaProducer<String, AccountEvent> producer;

	public Producer() {
		Properties producerProps = buildProducerProperties();
		producer = new KafkaProducer<String, AccountEvent>(producerProps);
	}

	public void publishRecord(String topic, AccountEvent value, String key)
			throws InterruptedException, ExecutionException {
		RecordMetadata metadata = producer.send(new ProducerRecord<>(topic, key, value)).get();
	}

	private Properties buildProducerProperties() {
		Properties properties = new Properties();
		properties.put("bootstrap.servers",
				"colossal.canadacentral.cloudapp.azure.com:29092,colossal.canadacentral.cloudapp.azure.com:29092,colossal.canadacentral.cloudapp.azure.com:39092,colossal.canadacentral.cloudapp.azure.com:49092,colossal.canadacentral.cloudapp.azure.com:49092");
		properties.put("key.serializer", StringSerializer.class);
		properties.put("value.serializer", KafkaAvroSerializer.class);
		properties.put("schema.registry.url", "http://colossal.canadacentral.cloudapp.azure.com:8081");
		return properties;
	}
}
