package com.onlineinteract.workflow.domain.account.bus;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
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

	public void publishRecord(String topic, AccountEvent value, String key) {
		producer.send(new ProducerRecord<>(topic, key, value));
	}

	private Properties buildProducerProperties() {
		Properties properties = new Properties();
		properties.put("bootstrap.servers", "tiny.canadacentral.cloudapp.azure.com:29092");
		properties.put("key.serializer", StringSerializer.class);
		properties.put("value.serializer", KafkaAvroSerializer.class);
		properties.put("schema.registry.url", "http://tiny.canadacentral.cloudapp.azure.com:8081");
		return properties;
	}
}
