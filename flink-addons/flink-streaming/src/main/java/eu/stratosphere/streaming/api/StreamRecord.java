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

/**
 * Object for storing serializable records in batch(single records are
 * represented batches with one element) used for sending records between task
 * objects in Stratosphere stream processing. The elements of the batch are
 * Value arrays.
 * 
 */
public class StreamRecord implements IOReadableWritable, Serializable {
	private static final long serialVersionUID = 1L;

	private List<Value[]> recordBatch;
	private StringValue uid = new StringValue("");
	private int numOfFields;
	private int numOfRecords;

	/**
	 * Creates a new empty batch of records and sets the field number to one
	 */
	public StreamRecord() {
		this.numOfFields = 1;
		recordBatch = new ArrayList<Value[]>();
	}

	/**
	 * Creates a new empty batch of records and sets the field number to the
	 * given number
	 * 
	 * @param length
	 *            Number of fields in the records
	 */
	public StreamRecord(int length) {
		this.numOfFields = length;
		recordBatch = new ArrayList<Value[]>();
	}

	/**
	 * Create a new batch of records with one element from an AtomRecord, sets
	 * number of fields accordingly
	 * 
	 */
	public StreamRecord(AtomRecord record) {
		Value[] fields = record.getFields();
		numOfFields = fields.length;
		recordBatch = new ArrayList<Value[]>();
		recordBatch.add(fields);
		numOfRecords = recordBatch.size();
	}

	/**
	 * Given an array of Values, creates a new a record batch containing the
	 * array as its first element
	 * 
	 * @param values
	 *            Array containing the Values for the first record in the batch
	 */
	public StreamRecord(Value... values) {
		numOfFields = values.length;
		recordBatch = new ArrayList<Value[]>();
		recordBatch.add(values);
		numOfRecords = recordBatch.size();
	}

	/**
	 * 
	 * @return Number of fields in the records
	 */
	public int getNumOfFields() {
		return numOfFields;
	}

	/**
	 * 
	 * @return Number of records in the batch
	 */
	public int getNumOfRecords() {
		return numOfRecords;
	}

	/**
	 * Set the ID of the StreamRecord object
	 * 
	 * @param channelID
	 *            ID of the emitting task
	 * @return The StreamRecord object
	 */
	public StreamRecord setId(String channelID) {
		Random rnd = new Random();
		uid.setValue(channelID + "-" + rnd.nextInt(1000));
		return this;
	}

	/**
	 * 
	 * @return The ID of the object
	 */
	public String getId() {
		return uid.getValue();
	}

	/**
	 * Returns the Value of a field in the given position of a specific record
	 * in the batch
	 * 
	 * @param recordNumber
	 *            Position of the record in the batch
	 * @param fieldNumber
	 *            Position of the field in the record
	 * @return Value of the field
	 */
	public Value getField(int recordNumber, int fieldNumber) {
		return recordBatch.get(recordNumber)[fieldNumber];
	}

	// TODO: javadoc
	public void setRecord(int recordNumber, Value... values) {
		if (values.length == numOfRecords) {
			recordBatch.set(recordNumber, values);
		}
	}

	public void setRecord(Value... values) {
		// TODO: consider clearing the List
		recordBatch = new ArrayList<Value[]>(1);
		if (values.length == numOfRecords) {
			recordBatch.set(0, values);
		}
	}

	/**
	 * 
	 * @param recordNumber
	 *            Position of the record in the batch
	 * @return Value array containing the fields of the record
	 */
	public Value[] getRecord(int recordNumber) {
		return recordBatch.get(recordNumber);
	}

	/**
	 * Checks if the number of fields are equal to the batch field size then
	 * adds the AtomRecord to the end of the batch
	 * 
	 * @param record
	 *            Record to be added
	 */
	public void addRecord(AtomRecord record) {
		Value[] fields = record.getFields();
		if (fields.length == numOfFields) {
			recordBatch.add(fields);
			numOfRecords = recordBatch.size();
		}
	}

	/**
	 * Checks if the number of fields are equal to the batch field size then
	 * adds the Value array to the end of the batch
	 * 
	 * @param record
	 *            Value array to be added as the next record of the batch
	 */
	public void addRecord(Value[] fields) {
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
			// Read the fields
			for (int i = 0; i < numOfFields; i++) {
				StringValue stringValue = new StringValue("");
				stringValue.read(in);
				try {
					record[i] = (Value) Class.forName(stringValue.getValue()).newInstance();
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

	// TODO: fix this method to work properly for non StringValue types
	public String toString() {
		StringBuilder outputString = new StringBuilder();
		StringValue output;
		for (int k = 0; k < numOfRecords; ++k) {
			for (int i = 0; i < numOfFields; i++) {
				try {
					output = (StringValue) recordBatch.get(k)[i];
					outputString.append(output.getValue() + "*");
				} catch (ClassCastException e) {
					outputString.append("NON-STRING*");
				}
			}
		}
		return outputString.toString();
	}

}
