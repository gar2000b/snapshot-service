package com.onlineinteract.workflow.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.onlineinteract.workflow.model.SnapshotInfo.Domain;
import com.onlineinteract.workflow.model.SnapshotInfo.Version;
import com.onlineinteract.workflow.utility.JsonParser;

public class SnapshotInfoTest {

	@Test
	public void test() {
		SnapshotInfo snapshotInfo = new SnapshotInfo();
		Map<String, Domain> domains = new HashMap<>();
		Domain domain1 = new Domain();
		domain1.setCollection("accounts");
		domain1.setTopic("account-event-topic");
		domain1.setCron("cron-expression");
		Version version = new Version();
		version.setVersion(1);
		version.setEndSnapshotOffset(103523);
		List<Version> versions = new ArrayList<>();
		versions.add(version);
		domain1.setVersions(versions);
		domains.put("accounts", domain1);
		snapshotInfo.setDomains(domains);
		System.out.println(JsonParser.toJson(snapshotInfo));
		SnapshotInfo si = JsonParser.fromJson("{\r\n" + "    \"_id\" : \"1\",\r\n" + "    \"domains\" : {\r\n"
				+ "        \"accounts\" : {\r\n" + "            \"collection\" : \"accounts\",\r\n"
				+ "            \"topic\" : \"account-event-topic\",\r\n" + "            \"cron\" : null,\r\n"
				+ "            \"versions\" : [ \r\n" + "                {\r\n"
				+ "                    \"version\" : 1,\r\n"
				+ "                    \"beginSnapshotOffset\" : 11,\r\n"
				+ "                    \"endSnapshotOffset\" : 10\r\n" + "                }\r\n" + "            ]\r\n"
				+ "        }\r\n" + "    },\r\n" + "    \"name\" : \"SnapshotInfo\"\r\n" + "}", SnapshotInfo.class);
		System.out.println("topic is: " + si.getDomains().get("accounts").getTopic());

	}
}
