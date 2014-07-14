package eu.stratosphere.streaming.api;

import java.util.Random;

import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.Value;

//TODO: refactor access modifiers

public class StreamRecord {
	private Record record;
	
	public StreamRecord(Record record) {
		this.record = record;
	}
	
	public StreamRecord addId() {
		Random rand = new Random();
		record.addField(new LongValue(rand.nextLong()));
		return this;
	}
	
	public Long popId() {
		LongValue id = new LongValue();
		record.getFieldInto(record.getNumFields() - 1, id);
		record.removeField(record.getNumFields() - 1);
		return id.getValue();
	}

	public Record getRecord() {
		return record;
	}
}
