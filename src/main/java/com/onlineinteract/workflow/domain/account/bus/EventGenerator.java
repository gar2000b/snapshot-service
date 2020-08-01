package com.onlineinteract.workflow.domain.account.bus;

import java.util.Date;
import java.util.concurrent.ExecutionException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.onlineinteract.workflow.domain.account.AccountEvent;
import com.onlineinteract.workflow.domain.account.v2.AccountV2;

@Component
public class EventGenerator {

	@Autowired
	private Producer producer;

	public void createAccount(AccountV2 accountV2) {
		AccountEvent accountEvent = new AccountEvent();
		accountEvent.setCreated(new Date().getTime());
		accountEvent.setEventId(String.valueOf(accountEvent.getCreated()));
		accountEvent.setEventType("AccountCreatedEvent");
		accountEvent.setVersion(2L);
		accountEvent.setV2(accountV2);

		try {
			producer.publishRecord("account-event-topic", accountEvent, accountEvent.getV1().getId().toString());
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}
		System.out.println("AccountCreatedEvent Published to account-event-topic");
	}

	public void updateAccount(AccountV2 accountV2) {
		AccountEvent accountEvent = new AccountEvent();
		accountEvent.setCreated(new Date().getTime());
		accountEvent.setEventId(String.valueOf(accountEvent.getCreated()));
		accountEvent.setEventType("AccountUpdatedEvent");
		accountEvent.setVersion(2L);
		accountEvent.setV2(accountV2);

		try {
			producer.publishRecord("account-event-topic", accountEvent, accountEvent.getV1().getId().toString());
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}
		System.out.println("AccountUpdatedEvent Published to account-event-topic");
	}
}
