package com.onlineinteract.workflow.domain.account.bus;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import javax.annotation.PreDestroy;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.onlineinteract.workflow.dbclient.DbClient;
import com.onlineinteract.workflow.domain.account.AccountEvent;
import com.onlineinteract.workflow.domain.account.AccountV1;
import com.onlineinteract.workflow.domain.account.repository.AccountRepository;
import com.onlineinteract.workflow.model.SnapshotInfo;
import com.onlineinteract.workflow.model.SnapshotInfo.Domain;
import com.onlineinteract.workflow.model.SnapshotInfo.Version;
import com.onlineinteract.workflow.repository.SnapshotRepository;
import com.onlineinteract.workflow.utility.JsonParser;
import com.onlineinteract.workflow.utility.MongoUtility;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;

@Component
public class SnapshotConsumerV1 {

	private static final String ACCOUNT_EVENT_TOPIC = "account-event-topic";

	@Autowired
	AccountRepository accountRepository;

	@Autowired
	DbClient dbClient;

	@Autowired
	SnapshotRepository snapshotRepository;

	@Autowired
	private Producer producer;

	private KafkaConsumer<String, AccountEvent> consumer;
	private boolean runningFlag = false;
	private long beginSnapshotOffset;
	private long endSnapshotOffset;

	public void executeSnapshot() {
		createConsumer();
		reconstituteState();
		writeSnapshot();
		updateSnapshotInfo();
	}

	private void createConsumer() {
		Properties buildProperties = buildConsumerProperties();
		consumer = new KafkaConsumer<>(buildProperties);
		consumer.subscribe(Arrays.asList(ACCOUNT_EVENT_TOPIC));
	}

	private void reconstituteState() {
		accountRepository.removeAllDocuments();
		determineBeginSnapshotOffset();
		if (beginSnapshotOffset > 0)
			reconstitutePreviousSnapshot();

		consumer.poll(0);
		for (TopicPartition partition : consumer.assignment())
			consumer.seek(partition, beginSnapshotOffset);

		runningFlag = true;
		System.out.println("Spinning up kafka account consumer");
		while (runningFlag) {
			ConsumerRecords<String, AccountEvent> records = consumer.poll(1000);
			System.out.println("*** records count 2: " + records.count());
			for (ConsumerRecord<String, AccountEvent> consumerRecord : records) {
				System.out.println("Consuming event from account-event-topic with id/key of: " + consumerRecord.key());
				AccountEvent account = (AccountEvent) consumerRecord.value();
				if (account.getEventType().toString().contains("AccountCreatedEvent"))
					accountRepository.createAccount(account.getV1());
				if (account.getEventType().toString().contains("AccountUpdatedEvent"))
					accountRepository.updateAccount(account.getV1());
				if (!(account.getEventType().toString().contains("SnapshotBeginEvent")
						|| account.getEventType().toString().contains("SnapshotEvent")
						|| account.getEventType().toString().contains("SnapshotEndEvent"))) {
					endSnapshotOffset = consumerRecord.offset();
				}
			}
			if (records.count() == 0)
				runningFlag = false;
		}
		shutdownConsumer();
		System.out.println("Shutting down kafka account consumer");
	}

	private void reconstitutePreviousSnapshot() {
		consumer.poll(0);
		for (TopicPartition partition : consumer.assignment())
			consumer.seek(partition, beginSnapshotOffset);

		runningFlag = true;
		System.out.println("Spinning up kafka account consumer to reconstitute previous snapshot prior to "
				+ "replaying events on top to create new snapshot");
		while (runningFlag) {
			ConsumerRecords<String, AccountEvent> records = consumer.poll(1000);
			System.out.println("*** records count 1: " + records.count());
			for (ConsumerRecord<String, AccountEvent> consumerRecord : records) {
				System.out.println("Consuming event from account-event-topic with id/key of: " + consumerRecord.key());
				AccountEvent account = (AccountEvent) consumerRecord.value();
				if (account.getEventType().toString().contains("SnapshotBeginEvent"))
					System.out.println("Snapshot begin event detected");
				if (account.getEventType().toString().contains("SnapshotEvent"))
					accountRepository.createAccount(account.getV1());
				if (account.getEventType().toString().contains("SnapshotEndEvent")) {
					System.out.println("Snapshot end event detected");
					return;
				}
			}
			if (records.count() == 0)
				runningFlag = false;
		}
	}

	private void writeSnapshot() {
		MongoDatabase database = dbClient.getMongoClient().getDatabase(DbClient.DATABASE);
		MongoCollection<Document> accountsCollection = database.getCollection("accounts");
		FindIterable<Document> accountDocumentsIterable = accountsCollection.find();
		publishSnapshotMarkerEvent("SnapshotBeginEvent");
		for (Document accountDocument : accountDocumentsIterable) {
			MongoUtility.removeMongoId(accountDocument);
			AccountV1 account = JsonParser.fromJson(accountDocument.toJson(), AccountV1.class);
			publishSnapshotEvent("SnapshotEvent", account);
			System.out.println("AccountCreatedEvent Published to account-event-topic");
		}
		publishSnapshotMarkerEvent("SnapshotEndEvent");
	}

	private void publishSnapshotEvent(String eventType, AccountV1 account) {
		AccountEvent accountEvent = new AccountEvent();
		accountEvent.setCreated(new Date().getTime());
		accountEvent.setEventId(String.valueOf(accountEvent.getCreated()));
		accountEvent.setEventType(eventType);
		accountEvent.setV1(account);
		producer.publishRecord("account-event-topic", accountEvent, account.getId().toString());
	}

	private void publishSnapshotMarkerEvent(String eventType) {
		AccountEvent accountEvent = new AccountEvent();
		accountEvent.setCreated(new Date().getTime());
		accountEvent.setEventId(String.valueOf(accountEvent.getCreated()));
		accountEvent.setEventType(eventType);
		AccountV1 account = new AccountV1();
		account.setId("");
		account.setName("");
		account.setOpeningBalance("");
		account.setSavingsRate("");
		account.setType("");
		accountEvent.setV1(account);
		producer.publishRecord("account-event-topic", accountEvent, accountEvent.getEventId().toString());
	}

	private void updateSnapshotInfo() {
		SnapshotInfo snapshotInfo = snapshotRepository.getSnapshotInfo();
		Domain accountsDomain = snapshotInfo.getDomains().get("accounts");
		if (accountsDomain == null) {
			createAccountsDomain(snapshotInfo);
			return;
		}

		List<Version> versions = accountsDomain.getVersions();
		for (Version version : versions) {
			if (version.getVersion() == 1) {
				version.setBeginSnapshotOffset(beginSnapshotOffset);
				version.setEndSnapshotOffset(endSnapshotOffset);
				break;
			}
		}
		snapshotInfo.getDomains().get("accounts").setVersions(versions);
		snapshotRepository.updateSnapshotInfo(snapshotInfo);
	}

	private void createAccountsDomain(SnapshotInfo snapshotInfo) {
		Domain accountsDomain = new Domain();
		accountsDomain.setCollection("accounts");
		accountsDomain.setTopic("account-event-topic");
		Version version = new Version();
		version.setBeginSnapshotOffset(beginSnapshotOffset);
		version.setEndSnapshotOffset(endSnapshotOffset);
		version.setVersion(1);
		ArrayList<Version> versions = new ArrayList<Version>();
		versions.add(version);
		accountsDomain.setVersions(versions);
		snapshotInfo.getDomains().put("accounts", accountsDomain);
		snapshotRepository.updateSnapshotInfo(snapshotInfo);
	}

	private void determineBeginSnapshotOffset() {
		SnapshotInfo snapshotInfo = snapshotRepository.getSnapshotInfo();
		Domain accountsDomain = snapshotInfo.getDomains().get("accounts");
		if (accountsDomain == null) {
			beginSnapshotOffset = 0;
		} else {
			List<Version> versions = accountsDomain.getVersions();
			for (Version version : versions) {
				if (version.getVersion() == 1) {
					beginSnapshotOffset = version.getEndSnapshotOffset() + 1;
				}
			}
		}
	}

	@PreDestroy
	public void shutdownConsumer() {
		System.out.println("*** consumer shutting down");
		consumer.close();
	}

	private Properties buildConsumerProperties() {
		Properties properties = new Properties();
		properties.put("bootstrap.servers", "tiny.canadacentral.cloudapp.azure.com:29092");
		properties.put("group.id", "account-event-topic-snapshot");
		properties.put("enable.auto.commit", "false");
		properties.put("max.poll.records", "200");
		properties.put("key.deserializer", StringDeserializer.class);
		properties.put("value.deserializer", KafkaAvroDeserializer.class);
		properties.put("schema.registry.url", "http://tiny.canadacentral.cloudapp.azure.com:8081");
		properties.put("specific.avro.reader", "true");
		return properties;
	}
}