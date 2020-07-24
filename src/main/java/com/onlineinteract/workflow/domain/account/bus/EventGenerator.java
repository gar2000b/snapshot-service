package com.onlineinteract.workflow.domain.account.bus;

import java.util.Date;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.onlineinteract.workflow.domain.account.AccountEvent;
import com.onlineinteract.workflow.domain.account.v3.AccountV3;

@Component
public class EventGenerator {

	@Autowired
	private Producer producer;

	public void createAccount(AccountV3 accountV3) {
		AccountEvent accountEvent = new AccountEvent();
		accountEvent.setCreated(new Date().getTime());
		accountEvent.setEventId(String.valueOf(accountEvent.getCreated()));
		accountEvent.setEventType("AccountCreatedEvent");
		accountEvent.setVersion(2L);
		accountEvent.setV3(accountV3);

		producer.publishRecord("account-event-topic", accountEvent, accountEvent.getV3().getId().toString());
		System.out.println("AccountCreatedEvent Published to account-event-topic");
	}

	public void updateAccount(AccountV3 accountV3) {
		AccountEvent accountEvent = new AccountEvent();
		accountEvent.setCreated(new Date().getTime());
		accountEvent.setEventId(String.valueOf(accountEvent.getCreated()));
		accountEvent.setEventType("AccountUpdatedEvent");
		accountEvent.setVersion(2L);
		accountEvent.setV3(accountV3);

		producer.publishRecord("account-event-topic", accountEvent, accountEvent.getV3().getId().toString());
		System.out.println("AccountUpdatedEvent Published to account-event-topic");
	}
}
