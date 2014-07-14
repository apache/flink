package eu.stratosphere.streaming.api;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Random;

import eu.stratosphere.core.io.IOReadableWritable;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.types.Value;

public class SerializableStreamRecord implements IOReadableWritable, Serializable {
	private static final long serialVersionUID = 1L;

	private Value[] fields;
	private StringValue uid = new StringValue("");
	private String channelID = "";
	private int numOfFields;
		
	public SerializableStreamRecord(int length) {
		this.numOfFields = length;
		fields = new Value[length];
	}
	
	public SerializableStreamRecord(int length, String channelID) {
		this(length);
		this.channelID = channelID;
	}
	
	public int getNumOfFields() {
		return numOfFields;
	}	

	public SerializableStreamRecord setId() {
		Random rnd = new Random();
		uid.setValue(channelID + "-" + rnd.nextInt(1000));
		return this;
	}
	
	public String getId() {
		return uid.getValue();
	}
	
	public Value getField(int fieldNumber) {
		return fields[fieldNumber];
	}
	
	public void setField(int fieldNumber, Value value) {
		fields[fieldNumber] = value;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		uid.write(out);
		
		// Write the number of fields with an IntValue
		(new IntValue(numOfFields)).write(out);
		
		// Write the fields
		for (int i = 0; i < numOfFields; i++) {
			fields[i].write(out);
		}
	}

	@Override
	public void read(DataInput in) throws IOException {
		uid.read(in);
		
		// Get the number of fields
		IntValue numOfFieldsValue = new IntValue(0);
		numOfFieldsValue.read(in);
		numOfFields = numOfFieldsValue.getValue();
		
		// Make sure the fields have numOfFields elements
		fields = new Value[numOfFields];
		
		// Read the fields
		for (int i = 0; i < numOfFields; i++) {
			fields[i].read(in);
		}
	}
}
