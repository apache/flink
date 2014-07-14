package eu.stratosphere.streaming.api;

import java.util.Random;

import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;

//TODO: refactor access modifiers

public class StreamRecord {
	private Record record;
	private StringValue uid = new StringValue("");
	private String channelID = "";

	public StreamRecord(Record record) {
		this.record = record;
	}

	public StreamRecord(Record record, String channelID) {
		// TODO:get rid of copy
		this.record = record.createCopy();
		this.channelID = channelID;
	}

	public StreamRecord addId() {
		// uid.setValue(channelID+"-"+UUID.randomUUID().toString());
		Random rnd = new Random();
		uid.setValue(channelID + "-" + rnd.nextInt(1000));
		record.addField(uid);
		return this;
	}

	public String popId() {
		record.getFieldInto(record.getNumFields() - 1, uid);
		record.removeField(record.getNumFields() - 1);
		return uid.getValue();
	}

	public String getId() {
		record.getFieldInto(record.getNumFields() - 1, uid);
		return uid.getValue();
	}

	public Record getRecord() {
		return record;
	}

	// TODO:write proper toString
	@Override
	public String toString() {

		StringBuilder outputString = new StringBuilder();
		StringValue output = new StringValue("");

		for (int i = 0; i < this.record.getNumFields(); i++) {
			this.record.getFieldInto(i, output);
			outputString.append(output.getValue() + "*");
		}
		return outputString.toString();

	}
}
