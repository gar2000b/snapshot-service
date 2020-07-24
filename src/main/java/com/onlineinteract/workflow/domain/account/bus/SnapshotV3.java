package com.onlineinteract.workflow.domain.account.bus;

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
import com.onlineinteract.workflow.domain.account.repository.AccountRepository;
import com.onlineinteract.workflow.domain.account.v3.AccountV3;
import com.onlineinteract.workflow.model.SnapshotInfo;
import com.onlineinteract.workflow.model.SnapshotInfo.Domain;
import com.onlineinteract.workflow.model.SnapshotInfo.Version;
import com.onlineinteract.workflow.repository.SnapshotRepository;
import com.onlineinteract.workflow.utility.JsonParser;
import com.onlineinteract.workflow.utility.MongoUtility;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;

@Component
public class SnapshotV3 {

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
	private long versionReconstitutedFrom;

	public void executeSnapshot() {
		createConsumer();
		reconstituteState();
		writeSnapshot();
		updateSnapshotInfo();
		resetSnapshotOffsets();
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
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		runningFlag = true;
		System.out.println("Spinning up kafka account consumer");
		while (runningFlag) {
			ConsumerRecords<String, AccountEvent> records = consumer.poll(100);
			System.out.println("*** records count 2: " + records.count());
			for (ConsumerRecord<String, AccountEvent> consumerRecord : records) {
				System.out.println("Consuming event from account-event-topic with id/key of: " + consumerRecord.key());
				AccountEvent accountEvent = (AccountEvent) consumerRecord.value();
				if (accountEvent.getEventType().toString().contains("AccountCreatedEvent")
						&& accountEvent.getVersion() == 2)
					accountRepository.createAccount(accountEvent.getV2());
				if (accountEvent.getEventType().toString().contains("AccountUpdatedEvent")
						&& accountEvent.getVersion() == 2)
					accountRepository.updateAccount(accountEvent.getV2());

				if (accountEvent.getEventType().toString().contains("AccountCreatedEvent")
						&& accountEvent.getVersion() >= 3)
					accountRepository.createAccount(accountEvent.getV3());
				if (accountEvent.getEventType().toString().contains("AccountUpdatedEvent")
						&& accountEvent.getVersion() >= 3)
					accountRepository.updateAccount(accountEvent.getV3());

				if (!(accountEvent.getEventType().toString().contains("SnapshotBeginEvent")
						|| accountEvent.getEventType().toString().contains("SnapshotEvent")
						|| accountEvent.getEventType().toString().contains("SnapshotEndEvent"))) {
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
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		runningFlag = true;
		System.out.println("Spinning up kafka account consumer to reconstitute previous snapshot prior to "
				+ "replaying events on top to create new snapshot");
		while (runningFlag) {
			ConsumerRecords<String, AccountEvent> records = consumer.poll(100);
			System.out.println("*** records count 1: " + records.count());
			for (ConsumerRecord<String, AccountEvent> consumerRecord : records) {
				System.out.println("Consuming event from account-event-topic with id/key of: " + consumerRecord.key());
				AccountEvent accountEvent = (AccountEvent) consumerRecord.value();
				if (versionReconstitutedFrom == 2) {
					if (accountEvent.getEventType().toString().contains("SnapshotBeginEvent")
							&& accountEvent.getVersion() == 2)
						System.out.println("Snapshot begin event detected");
					if (accountEvent.getEventType().toString().contains("SnapshotEvent")
							&& accountEvent.getVersion() == 2) {
						accountRepository.createAccount(accountEvent.getV2());
					}
					if (accountEvent.getEventType().toString().contains("SnapshotEndEvent")
							&& accountEvent.getVersion() == 2) {
						System.out.println("Snapshot end event detected");
						return;
					}
				}
				if (versionReconstitutedFrom == 3) {
					if (accountEvent.getEventType().toString().contains("SnapshotBeginEvent")
							&& accountEvent.getVersion() == 3)
						System.out.println("Snapshot begin event detected");
					if (accountEvent.getEventType().toString().contains("SnapshotEvent")
							&& accountEvent.getVersion() == 3) {
						accountRepository.createAccount(accountEvent.getV3());
					}
					if (accountEvent.getEventType().toString().contains("SnapshotEndEvent")
							&& accountEvent.getVersion() == 3) {
						System.out.println("Snapshot end event detected");
						return;
					}
				}
			}
			if (records.count() == 0)
				runningFlag = false;
		}
	}

	private void writeSnapshot() {
		if (endSnapshotOffset < beginSnapshotOffset) {
			System.out.println("**** No events have been published since last snapshot - snapshot not required ****");
			return;
		}

		MongoDatabase database = dbClient.getMongoClient().getDatabase(DbClient.DATABASE);
		MongoCollection<Document> accountsCollection = database.getCollection("accounts");
		FindIterable<Document> accountDocumentsIterable = accountsCollection.find();
		publishSnapshotMarkerEvent("SnapshotBeginEvent");
		for (Document accountDocument : accountDocumentsIterable) {
			MongoUtility.removeMongoId(accountDocument);
			AccountV3 accountV3 = JsonParser.fromJson(accountDocument.toJson(), AccountV3.class);
			if (versionReconstitutedFrom == 2) {
				if (accountV3.getAddr() == null) {
					System.out.println("addr is null");
					accountV3.setAddr("");
				}
				if (accountV3.getEnabled() == null) {
					System.out.println("enabled is null");
					accountV3.setEnabled(false);
				}
			}
			publishSnapshotEvent("SnapshotEvent", accountV3);
			System.out.println("AccountCreatedEvent Published to account-event-topic");
		}
		publishSnapshotMarkerEvent("SnapshotEndEvent");
	}

	private void publishSnapshotEvent(String eventType, AccountV3 accountV3) {
		AccountEvent accountEvent = new AccountEvent();
		accountEvent.setCreated(new Date().getTime());
		accountEvent.setEventId(String.valueOf(accountEvent.getCreated()));
		accountEvent.setEventType(eventType);
		accountEvent.setV3(accountV3);
		accountEvent.setVersion(3L);
		producer.publishRecord("account-event-topic", accountEvent, accountV3.getId().toString());
	}

	private void publishSnapshotMarkerEvent(String eventType) {
		AccountEvent accountEvent = new AccountEvent();
		accountEvent.setCreated(new Date().getTime());
		accountEvent.setEventId(String.valueOf(accountEvent.getCreated()));
		accountEvent.setEventType(eventType);
		accountEvent.setVersion(3L);
		producer.publishRecord("account-event-topic", accountEvent, accountEvent.getEventId().toString());
	}

	private void updateSnapshotInfo() {
		SnapshotInfo snapshotInfo = snapshotRepository.getSnapshotInfo();
		Domain accountsDomain = snapshotInfo.getDomains().get("accounts");

		List<Version> versions = accountsDomain.getVersions();
		for (Version version : versions) {
			if (version.getVersion() == 3) {
				version.setBeginSnapshotOffset(beginSnapshotOffset);
				version.setEndSnapshotOffset(endSnapshotOffset);
				snapshotInfo.getDomains().get("accounts").setVersions(versions);
				snapshotRepository.updateSnapshotInfo(snapshotInfo);
				return;
			}
		}

		for (Version version : versions) {
			if (version.getVersion() == 2)
				if (version.getEndSnapshotOffset() == endSnapshotOffset)
					return;
		}

		Version version = new Version();
		version.setBeginSnapshotOffset(beginSnapshotOffset);
		version.setEndSnapshotOffset(endSnapshotOffset);
		version.setVersion(3);
		versions.add(version);
		snapshotInfo.getDomains().get("accounts").setVersions(versions);
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
				if (version.getVersion() == 3) {
					beginSnapshotOffset = version.getEndSnapshotOffset() + 1;
					endSnapshotOffset = version.getEndSnapshotOffset();
					versionReconstitutedFrom = 3;
					return;
				}
			}
			for (Version version : versions) {
				if (version.getVersion() == 2) {
					beginSnapshotOffset = version.getEndSnapshotOffset() + 1;
					endSnapshotOffset = version.getEndSnapshotOffset();
					versionReconstitutedFrom = 2;
					return;
				}
			}
		}
	}

	private void resetSnapshotOffsets() {
		beginSnapshotOffset = 0;
		endSnapshotOffset = 0;
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