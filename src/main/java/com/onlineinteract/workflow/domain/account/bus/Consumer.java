package com.onlineinteract.workflow.domain.account.bus;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import com.onlineinteract.workflow.domain.account.AccountEvent;
import com.onlineinteract.workflow.domain.account.repository.AccountRepository;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;

/**
 * TODO: Leaving this here as for illustrative purposes atm. Will have to think
 * about how I enable this for actual clients that want to use this once they
 * add the client-domain-lib as a dependency - maybe some spring property
 * directive. Regardless, it should never spin up from the producing apps point
 * of view.
 * 
 * @author gar20
 *
 */
//@Component
public class Consumer {

	private static final String ACCOUNT_EVENT_TOPIC = "account-event-topic";

	@Autowired
	AccountRepository accountRepository;

	private KafkaConsumer<String, AccountEvent> consumer;
	private boolean runningFlag = false;

	private List<Map<String, String>> schemas = new ArrayList<>();
	private String schemaVersion;
	private String schemaId;
	private String schema;

	@PostConstruct
	public void startConsumer() {
		/*
		 * fetchSchemas(); determineCurrentSchema();
		 */
		createConsumer();
		processRecords();
	}

	private void createConsumer() {
		Properties buildProperties = buildConsumerProperties();
		consumer = new KafkaConsumer<>(buildProperties);
		consumer.subscribe(Arrays.asList(ACCOUNT_EVENT_TOPIC));
	}

	private void processRecords() {
		consumer.poll(0);
		// consumer.seekToBeginning(consumer.assignment());
		runningFlag = true;
		System.out.println("Spinning up kafka account consumer");
		new Thread(() -> {
			while (runningFlag) {
				ConsumerRecords<String, AccountEvent> records = consumer.poll(100);
				for (ConsumerRecord<String, AccountEvent> consumerRecord : records) {
					System.out.println(
							"Consuming event from customer-event-topic with id/key of: " + consumerRecord.key());
					AccountEvent account = (AccountEvent) consumerRecord.value();
					if (account.getEventType().toString().contains("AccountCreatedEvent"))
						accountRepository.createAccount(account.getV1());
					if (account.getEventType().toString().contains("AccountUpdatedEvent"))
						accountRepository.updateAccount(account.getV1());
				}
			}
			shutdownConsumerProducer();
			System.out.println("Shutting down kafka account consumer");
		}).start();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void fetchSchemas() {
		RestTemplate restTemplate = new RestTemplate();
		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON);
		ResponseEntity<String[]> schemaVersionsResponse = restTemplate.getForEntity(
				"http://tiny.canadacentral.cloudapp.azure.com:8081/subjects/" + ACCOUNT_EVENT_TOPIC + "-value/versions",
				String[].class, headers);
		String[] schemaVersions = schemaVersionsResponse.getBody();

		for (String schemaVersion : schemaVersions) {
			System.out.println("schema version fetched: " + schemaVersion);
			ResponseEntity<HashMap> schemaResponse = restTemplate
					.getForEntity("http://tiny.canadacentral.cloudapp.azure.com:8081/subjects/" + ACCOUNT_EVENT_TOPIC
							+ "-value/versions/" + schemaVersion, HashMap.class, headers);
			HashMap<String, String> schema = schemaResponse.getBody();

			Map<String, String> schemaEntry = new HashMap<String, String>();
			schemaEntry.put("version", String.valueOf(schema.get("version")));
			schemaEntry.put("id", String.valueOf(schema.get("id")));
			schemaEntry.put("schema", schema.get("schema"));
			schemas.removeAll(schemas);
			schemas.add(schemaEntry);
			System.out.println("schema: " + schemaEntry);
		}
	}

	@SuppressWarnings("unused")
	private String determineSchema(String schema) {
		for (Map<String, String> schemaEntry : schemas) {
			if (schemaEntry.get("schema").equals(schema)) {
				return schemaEntry.get("id");
			}
		}

		fetchSchemas();

		for (Map<String, String> schemaEntry : schemas) {
			if (schemaEntry.get("schema").equals(schema)) {
				return schemaEntry.get("id");
			}
		}

		return "-1";
	}

	@SuppressWarnings("unused")
	private void determineCurrentSchema() {
		for (Map<String, String> schemaEntry : schemas) {
			String schema = schemaEntry.get("schema");
			if (schema.equals(AccountEvent.getClassSchema().toString())) {
				this.schemaVersion = schemaEntry.get("version");
				this.schemaId = schemaEntry.get("id");
				this.schema = schemaEntry.get("schema");
				System.out.println("Current schema version: " + this.schemaVersion + " - id: " + this.schemaId
						+ " - schema: " + this.schema);
				break;
			}
		}
	}

	@PreDestroy
	public void shutdownConsumerProducer() {
		System.out.println("*** consumer shutting down");
		consumer.close();
	}

	private Properties buildConsumerProperties() {
		Properties properties = new Properties();
		properties.put("bootstrap.servers", "tiny.canadacentral.cloudapp.azure.com:29092");
		properties.put("group.id", "account-event-topic-group2");
		properties.put("enable.auto.commit", "false");
		properties.put("max.poll.records", "200");
		properties.put("key.deserializer", StringDeserializer.class);
		properties.put("value.deserializer", KafkaAvroDeserializer.class);
		properties.put("schema.registry.url", "http://tiny.canadacentral.cloudapp.azure.com:8081");
		properties.put("specific.avro.reader", "true");
		return properties;
	}
}
