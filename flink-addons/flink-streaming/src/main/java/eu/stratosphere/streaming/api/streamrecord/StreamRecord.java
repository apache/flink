/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.streaming.api.streamrecord;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import eu.stratosphere.core.io.IOReadableWritable;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.types.Value;

/**
 * Object for storing serializable records in batch (single records are
 * represented batches with one element) used for sending records between task
 * objects in Stratosphere stream processing. The elements of the batch are
 * Value arrays.
 */
public class StreamRecord implements IOReadableWritable, Serializable {
	private static final long serialVersionUID = 1L;

	private List<Value[]> recordBatch;
	private StringValue uid = new StringValue("");
	private int numOfFields;
	private int numOfRecords;
//	private Random rnd = new Random();

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
		numOfFields = length;
		recordBatch = new ArrayList<Value[]>();
	}

	/**
	 * Creates a new empty batch of records and sets the field number to the
	 * given number, and the number of records to the given number. Setting
	 * batchSize is just for optimization, records need to be added.
	 * 
	 * @param length
	 *            Number of fields in the records
	 * @param batchSize
	 *            Number of records
	 */
	public StreamRecord(int length, int batchSize) {
		numOfFields = length;
		recordBatch = new ArrayList<Value[]>(batchSize);
	}

	/**
	 * Given an array of Values, creates a new a record batch containing the
	 * array as its first element
	 * 
	 * @param values
	 *            Array containing the Values for the first record in the batch
	 */
	public StreamRecord(Value... values) {
		this(values.length, 1);
		numOfRecords = 1;
		recordBatch.add(values);
	}

	/**
	 * @return Number of fields in the records
	 */
	public int getNumOfFields() {
		return numOfFields;
	}

	/**
	 * @return Number of records in the batch
	 */
	public int getNumOfRecords() {
		return numOfRecords;
	}

	// TODO: use UUID
	/**
	 * Set the ID of the StreamRecord object
	 * 
	 * @param channelID
	 *            ID of the emitting task
	 * @return The StreamRecord object
	 */
	public StreamRecord setId(String channelID) {
		UUID uuid = UUID.randomUUID();
		uid.setValue(channelID + "-" + uuid.toString());//rnd.nextInt(10));
		return this;
	}

	/**
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
		try {
			return recordBatch.get(recordNumber)[fieldNumber];
		} catch (IndexOutOfBoundsException e) {
			throw (new NoSuchRecordException());
		}
	}

	/**
	 * Returns the Value of a field in the given position of the first record in
	 * the batch
	 * 
	 * @param fieldNumber
	 *            Position of the field in the record
	 * @return Value of the field
	 */
	public Value getField(int fieldNumber) {
		try {
			return recordBatch.get(0)[fieldNumber];
		} catch (IndexOutOfBoundsException e) {
			throw (new NoSuchFieldException());
		}
	}

	/**
	 * Sets a field in the given position of a specific record in the batch
	 * 
	 * @param recordNumber
	 *            Position of record in batch
	 * @param fieldNumber
	 *            Position of field in record
	 * @param value
	 *            Value to set
	 */
	public void setField(int recordNumber, int fieldNumber, Value value) {
		try {
			recordBatch.get(recordNumber)[fieldNumber] = value;
		} catch (IndexOutOfBoundsException e) {
			throw (new NoSuchRecordException());
		}
	}

	/**
	 * Sets a field in the given position of the first record in the batch
	 * 
	 * @param fieldNumber
	 *            Position of the field in the record
	 * @param value
	 *            Value to set the given field to
	 */
	public void setField(int fieldNumber, Value value) {
		try {
			recordBatch.get(0)[fieldNumber] = value;
		} catch (IndexOutOfBoundsException e) {
			throw (new NoSuchFieldException());
		}
	}

	/**
	 * @param recordNumber
	 *            Position of the record in the batch
	 * @return Value array containing the fields of the record
	 */
	public Value[] getRecord(int recordNumber) {
		try {
			return recordBatch.get(recordNumber);
		} catch (IndexOutOfBoundsException e) {
			throw (new NoSuchRecordException());
		}
	}

	/**
	 * @return Value array containing the fields of first the record
	 */
	public Value[] getRecord() {
		return getRecord(0);
	}

	/**
	 * Sets a record at the given position in the batch
	 * 
	 * @param recordNumber
	 *            Position of record in the batch
	 * @param fields
	 *            Value to set
	 */
	public void setRecord(int recordNumber, Value... fields) {
		if (fields.length == numOfFields) {
			try {
				recordBatch.set(recordNumber, fields);
			} catch (IndexOutOfBoundsException e) {
				throw (new NoSuchRecordException());
			}
		} else {
			throw (new RecordSizeMismatchException());
		}
	}

	/**
	 * Sets the first record in the batch
	 * 
	 * @param fields
	 *            Value to set
	 */
	public void setRecord(Value... fields) {
		if (fields.length == numOfFields) {
			if (numOfRecords != 1) {
				recordBatch = new ArrayList<Value[]>(1);
				recordBatch.add(fields);
			} else {
				recordBatch.set(0, fields);
			}
		} else {
			throw (new RecordSizeMismatchException());
		}
	}

	/**
	 * Checks if the number of fields are equal to the batch field size then
	 * adds the Value array to the end of the batch
	 * 
	 * @param fields
	 *            Value array to be added as the next record of the batch
	 */
	public void addRecord(Value... fields) {
		if (fields.length == numOfFields) {
			recordBatch.add(fields);
			numOfRecords++;
		} else {
			throw new RecordSizeMismatchException();
		}
	}

	/**
	 * Creates a copy of the StreamRecord
	 * 
	 * @return Copy of the StreamRecord
	 * @throws IOException 
	 */
	public StreamRecord copy() {
		StreamRecord copiedRecord = new StreamRecord(this.numOfFields, this.numOfRecords);
		
		ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
		
		DataOutputStream out = new DataOutputStream(byteStream);
		
		try {
			this.write(out);
		} catch (IOException e) {
			e.printStackTrace();
		}
		DataInputStream in = new DataInputStream(new ByteArrayInputStream(byteStream.toByteArray()));
		
		try {
			copiedRecord.read(in);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return copiedRecord;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		uid.write(out);

		// Write the number of fields with an IntValue
		(new IntValue(numOfFields)).write(out);

		// Write the number of records with an IntValue
		(new IntValue(numOfRecords)).write(out);

		StringValue classNameValue = new StringValue("");
		// write the records
		for (Value[] record : recordBatch) {
			// Write the fields
			for (int i = 0; i < numOfFields; i++) {
				classNameValue.setValue(record[i].getClass().getName());
				classNameValue.write(out);
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

		StringValue stringValue = new StringValue("");

		for (int k = 0; k < numOfRecords; ++k) {
			Value[] record = new Value[numOfFields];
			// Read the fields
			for (int i = 0; i < numOfFields; i++) {
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
		StringBuilder outputString = new StringBuilder("(");
		StringValue output;
		for (int k = 0; k < numOfRecords; ++k) {
			for (int i = 0; i < numOfFields; i++) {
				try {
					output = (StringValue) recordBatch.get(k)[i];
					outputString.append(output.getValue() + ",");
				} catch (ClassCastException e) {
					outputString.append("NON-STRING,");
				}
			}
		}
		outputString.append(")");
		return outputString.toString();
	}

}
