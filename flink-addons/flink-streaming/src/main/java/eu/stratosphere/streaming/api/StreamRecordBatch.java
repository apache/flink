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

public class StreamRecordBatch extends StreamRecord implements
		IOReadableWritable, Serializable {
	private static final long serialVersionUID = 1L;

	private List<Value[]> fieldsBatch;
	private StringValue uid = new StringValue("");
	private String channelID = "";
	private int numOfFields;
	private int numOfRecords;

	public StreamRecordBatch() {
		this.numOfFields = 1;
		fieldsBatch = new ArrayList<Value[]>();
	}

	public StreamRecordBatch(int length) {
		this.numOfFields = length;
		fieldsBatch = new ArrayList<Value[]>();
	}

	public StreamRecordBatch(int length, String channelID) {
		this(length);
		setChannelId(channelID);
	}

	public int getNumOfFields() {
		return numOfFields;
	}

	public int getNumOfRecords() {
		return fieldsBatch.size();
	}

	public StreamRecordBatch setId(String channelID) {
		Random rnd = new Random();
		uid.setValue(channelID + "-" + rnd.nextInt(1000));
		return this;
	}

	public String getId() {
		return uid.getValue();
	}

	public void addRecord(StreamRecord record) {
		fieldsBatch.add(record.getFields());
		numOfRecords = fieldsBatch.size();
	}

	public Value getField(int recordNumber, int fieldNumber) {
		return fieldsBatch.get(recordNumber)[fieldNumber];
	}

	public StreamRecordBatch setChannelId(String channelID) {
		this.channelID = channelID;
		return this;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		uid.write(out);

		// Write the number of fields with an IntValue
		(new IntValue(numOfFields)).write(out);

		// Write the number of records with an IntValue
		(new IntValue(numOfRecords)).write(out);

		// write the records
		for (Value[] fields : fieldsBatch) {
			// Write the fields
			for (int i = 0; i < numOfFields; i++) {
				(new StringValue(fields[i].getClass().getName())).write(out);
				fields[i].write(out);
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

		fieldsBatch = new ArrayList<Value[]>();
		for (int k = 0; k < numOfRecords; ++k) {
			// Make sure the fields have numOfFields elements
			Value[] fields = new Value[numOfFields];

			// Read the fields
			for (int i = 0; i < numOfFields; i++) {
				StringValue stringValue = new StringValue("");
				stringValue.read(in);
				try {
					fields[i] = (Value) Class.forName(stringValue.getValue())
							.newInstance();
				} catch (InstantiationException e) {
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					e.printStackTrace();
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
				fields[i].read(in);
			}
			fieldsBatch.add(fields);
		}
	}
	
	public StreamRecordBatch newInstance() {
		return new StreamRecordBatch(0);
	}

	public String toString() {
		StringBuilder outputString = new StringBuilder();
		return outputString.toString();

	}
}
