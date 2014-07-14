package eu.stratosphere.streaming.api;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import eu.stratosphere.core.io.IOReadableWritable;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.types.Value;

public class StreamRecord implements IOReadableWritable, Serializable {
	private static final long serialVersionUID = 1L;

	private List<Value[]> recordBatch;
	private StringValue uid = new StringValue("");
	// it seems that we never use this variable.
	private String channelID = "";
	private int numOfFields;
	private int numOfRecords;

	public StreamRecord() {
		this.numOfFields = 1;
		recordBatch = new ArrayList<Value[]>();
		// setId();
	}

	public StreamRecord(int length) {
		this.numOfFields = length;
		recordBatch = new ArrayList<Value[]>();
		// setId();
	}

	public StreamRecord(int length, String channelID) {
		this(length);
		setChannelId(channelID);
	}
	
	public StreamRecord(AtomRecord record){
		Value[] fields=record.getFields();
		numOfFields = fields.length;
		recordBatch = new ArrayList<Value[]>();
		recordBatch.add(fields);
		numOfRecords=recordBatch.size();
	}

	public int getNumOfFields() {
		return numOfFields;
	}

	public int getNumOfRecords() {
		return numOfRecords;
	}

	public StreamRecord setId(String channelID) {
		Random rnd = new Random();
		uid.setValue(channelID + "-" + rnd.nextInt(1000));
		return this;
	}

	public String getId() {
		return uid.getValue();
	}

	public Value getField(int recordNumber, int fieldNumber) {
		return recordBatch.get(recordNumber)[fieldNumber];
	}
	
	public AtomRecord getRecord(int recordNumber){
		return new AtomRecord(recordBatch.get(recordNumber));
	}

	public void addRecord(AtomRecord record) {
		Value[] fields = record.getFields();
		if (fields.length == numOfFields) {
			recordBatch.add(fields);
			numOfRecords = recordBatch.size();
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		uid.write(out);

		// Write the number of fields with an IntValue
		(new IntValue(numOfFields)).write(out);

		// Write the number of records with an IntValue
		(new IntValue(numOfRecords)).write(out);

		// write the records
		for (Value[] record : recordBatch) {
			// Write the fields
			for (int i = 0; i < numOfFields; i++) {
				(new StringValue(record[i].getClass().getName())).write(out);
				record[i].write(out);
			}
		}
	}

	@Override
	public void read(DataInput in) throws IOException {
		uid.read(in);

		// Get the number of fields
		IntValue numOfFieldsValue = new IntValue(0);
		numOfFieldsValue.read(in);
		numOfFields = numOfFieldsValue.getValue();

		// Get the number of records
		IntValue numOfRecordsValue = new IntValue(0);
		numOfRecordsValue.read(in);
		numOfRecords = numOfRecordsValue.getValue();

		// Make sure the fields have numOfFields elements
		recordBatch = new ArrayList<Value[]>();

		for (int k = 0; k < numOfRecords; ++k) {
			Value[] record = new Value[numOfFields];
			// recordBatch=new Value[numOfFields];
			// Read the fields
			for (int i = 0; i < numOfFields; i++) {
				StringValue stringValue = new StringValue("");
				stringValue.read(in);
				try {
					record[i] = (Value) Class.forName(stringValue.getValue())
							.newInstance();
				} catch (InstantiationException e) {
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					e.printStackTrace();
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
				record[i].read(in);
			}
			recordBatch.add(record);
		}
	}

	public StreamRecord newInstance() {
		return new StreamRecord(0);
	}

	public StreamRecord setChannelId(String channelID) {
		this.channelID = channelID;
		return this;
	}

	// TODO: fix this method to work properly for non StringValue types
	public String toString() {
		StringBuilder outputString = new StringBuilder();
		StringValue output; // = new StringValue("");
		for (int k = 0; k < numOfRecords; ++k) {
			for (int i = 0; i < numOfFields; i++) {
				try {
					output = (StringValue) recordBatch.get(k)[i];
					outputString.append(output.getValue() + "*");
				} catch (ClassCastException e) {
					outputString.append("PRINT_ERROR*");
				}

			}
		}
		return outputString.toString();
	}

}
