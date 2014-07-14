package eu.stratosphere.streaming.api;

import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.Value;

public class FlatStreamRecord {

	private long id;
	private Record record;
	private int numberOfFields;
	
	public FlatStreamRecord() {
		id = 94;
	}
	
	public FlatStreamRecord(int numberOfFields) {
		this();
		
		this.numberOfFields = numberOfFields;
		record = new Record(numberOfFields);
		LongValue idValue = new LongValue(id);
		record.addField(idValue);
	}
	
	public FlatStreamRecord(Record record) {
		this();
		
		this.numberOfFields = record.getNumFields();
		this.record = record;
		LongValue idValue = new LongValue(id);
		record.addField(idValue);
	}
	
	public int getNumFields() {
		return this.numberOfFields;
	}
	
	public boolean getFieldInto(int fieldNum, Value target) {
		return record.getFieldInto(fieldNum, target);
	}
	
	public <T extends Value> T getField(final int fieldNum, final Class<T> type) {
		return record.getField(fieldNum, type);
	}

	//TODO: set to private or package private, this cannot be seen from outside!
	public Record getRecord() {
		return this.record;
	}
	
}
