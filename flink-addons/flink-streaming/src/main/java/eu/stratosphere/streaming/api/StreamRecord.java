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

public class StreamRecord implements IOReadableWritable, Serializable {
	private static final long serialVersionUID = 1L;

	private Value[] fields;
	private StringValue uid = new StringValue("");
	private String channelID = "";
	private int numOfFields;

	public StreamRecord() {
		this.numOfFields = 1;
		fields = new Value[1];
		//setId();
	}

	public StreamRecord(int length) {
		this.numOfFields = length;
		fields = new Value[length];
	//	setId();
	}

	public StreamRecord(int length, String channelID) {
		this(length);
		setChannelId(channelID);
	}

	public StreamRecord(Value value) {
		this(1);
		fields[0] = value;
	}

	public int getNumOfFields() {
		return numOfFields;
	}

	public StreamRecord setId(String channelID) {
		Random rnd = new Random();
		uid.setValue(channelID + "-" + rnd.nextInt());
		return this;
	}

	public String getId() {
		return uid.getValue();
	}

	public Value getField(int fieldNumber) {
		return fields[fieldNumber];
	}

	// public void getFieldInto(int fieldNumber, Value value) {
	// value = fields[fieldNumber];
	// }

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
			(new StringValue(fields[i].getClass().getName())).write(out);
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
			StringValue stringValue = new StringValue("");
			stringValue.read(in);
			try {
				fields[i] = (Value) Class.forName(stringValue.getValue()).newInstance();
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
			fields[i].read(in);
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

		for (int i = 0; i < this.getNumOfFields(); i++) {
			try {
				output = (StringValue) fields[i];
				outputString.append(output.getValue() + "*");
			} catch (ClassCastException e) {
				outputString.append("PRINT_ERROR*");
			}

		}
		return outputString.toString();
	}

}
