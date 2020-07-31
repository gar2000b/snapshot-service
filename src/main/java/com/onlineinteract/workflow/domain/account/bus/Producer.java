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

	public void publishRecord(String topic, AccountEvent value, String key) throws InterruptedException, ExecutionException {
		RecordMetadata metadata = producer.send(new ProducerRecord<>(topic, key, value)).get();
		System.out.println("Record sent with key " + key + " to partition " + metadata.partition() + " with offset "
				+ metadata.offset());
	}

	private Properties buildProducerProperties() {
		Properties properties = new Properties();
		properties.put("bootstrap.servers", "localhost:29092,localhost:29092,localhost:39092,localhost:49092,localhost:49092");
		properties.put("key.serializer", StringSerializer.class);
		properties.put("value.serializer", KafkaAvroSerializer.class);
		properties.put("schema.registry.url", "http://localhost:8081");
		return properties;
	}
}
