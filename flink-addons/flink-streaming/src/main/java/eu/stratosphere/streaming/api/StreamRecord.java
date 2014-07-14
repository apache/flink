package eu.stratosphere.streaming.api;

import java.util.UUID;

import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;

//TODO: refactor access modifiers

public class StreamRecord {
	private Record record;
	private StringValue uid = new StringValue("");
	
	public StreamRecord(Record record) {
		this.record = record;
	}
	
	public StreamRecord addId() {
		uid.setValue(UUID.randomUUID().toString()); 
		record.addField(uid);
		return this;
	}
	
	public String popId() {
		record.getFieldInto(record.getNumFields() - 1, uid);
		record.removeField(record.getNumFields() - 1);
		return uid.getValue();
	}

	public Record getRecord() {
		return record;
	}
}
