package com.onlineinteract.workflow.rest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.onlineinteract.workflow.domain.account.bus.SnapshotV1;
import com.onlineinteract.workflow.model.SnapshotInfo;
import com.onlineinteract.workflow.repository.SnapshotRepository;
import com.onlineinteract.workflow.utility.JsonParser;

@Controller
@EnableAutoConfiguration
public class SnapshotController {

	@Autowired
	SnapshotRepository snapshotRepository;

	@Autowired
	SnapshotV1 snapshotConsumerV1;

	@RequestMapping(method = RequestMethod.POST, consumes = "application/json", produces = "application/json", value = "/snapshot-info")
	@ResponseBody
	public ResponseEntity<String> loadSnapshotInfo(@RequestBody SnapshotInfo snapshotInfo) {
		System.out.println("*** loadSnapshotInfo() called ***");
		snapshotRepository.loadInSnapshotInfo(snapshotInfo);
		return new ResponseEntity<>("loadSnapshotInfo(): " + JsonParser.toJson(snapshotInfo), HttpStatus.OK);
	}

	@RequestMapping(method = RequestMethod.PUT, consumes = "application/json", produces = "application/json", value = "/snapshot-info")
	@ResponseBody
	public ResponseEntity<String> updateSnapshotInfo(@RequestBody SnapshotInfo snapshotInfo) {
		System.out.println("*** updateSnapshotInfo() called ***");
		snapshotRepository.updateSnapshotInfo(snapshotInfo);
		return new ResponseEntity<>("updateSnapshotInfo(): " + JsonParser.toJson(snapshotInfo), HttpStatus.OK);
	}

	@RequestMapping(method = RequestMethod.POST, consumes = "application/json", produces = "application/json", value = "/snapshot/domain/{domain}/version/{version}")
	@ResponseBody
	public ResponseEntity<String> executeSnapshot(@PathVariable String domain, @PathVariable int version) {
		System.out.println("*** executeSnapshot() called ***");
		processSnapshot(domain, version);
		return new ResponseEntity<>(
				"executeSnapshot(): completed OK against domain: " + domain + " - version: " + version, HttpStatus.OK);
	}

	private void processSnapshot(String domain, int version) {
		if (domain.equals("accounts")) {
			if (version == 1)
				snapshotConsumerV1.executeSnapshot();
		}
	}
}
