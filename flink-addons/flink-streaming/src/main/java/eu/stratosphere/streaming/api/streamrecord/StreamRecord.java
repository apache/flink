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

import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.api.java.typeutils.TupleTypeInfo;
import eu.stratosphere.api.java.typeutils.TypeInformation;
import eu.stratosphere.api.java.typeutils.runtime.TupleSerializer;
import eu.stratosphere.core.io.IOReadableWritable;
import eu.stratosphere.pact.runtime.plugable.DeserializationDelegate;
import eu.stratosphere.pact.runtime.plugable.SerializationDelegate;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.StringValue;

/**
 * Object for storing serializable records in batch (single records are
 * represented batches with one element) used for sending records between task
 * objects in Stratosphere stream processing. The elements of the batch are
 * Value arrays.
 */
//TODO: update documentation
public class StreamRecord implements IOReadableWritable, Serializable {
	private static final long serialVersionUID = 1L;

	private List<Tuple> recordBatch;
	private StringValue uid = new StringValue("");
	private int numOfFields;
	private int numOfRecords;

	// TODO implement equals, clone
	/**
	 * Creates a new empty instance for read
	 */
	public StreamRecord() {
	}

	public StreamRecord(int numOfFields) {
		this.numOfFields = numOfFields;
		this.numOfRecords = 0;
		recordBatch = new ArrayList<Tuple>();

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
	public StreamRecord(Tuple tuple, int batchSize) {
		numOfFields = tuple.getArity();
		numOfRecords = 1;
		recordBatch = new ArrayList<Tuple>(batchSize);
		recordBatch.add(tuple);

	}

	/**
	 * Given an array of Values, creates a new a record batch containing the
	 * array as its first element
	 * 
	 * @param values
	 *            Array containing the Values for the first record in the batch
	 */
	public StreamRecord(Tuple tuple) {
		this(tuple, 1);
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
		uid.setValue(channelID + "-" + uuid.toString());// rnd.nextInt(10));
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
	public Object getField(int recordNumber, int fieldNumber) {
		try {
			return recordBatch.get(recordNumber).getField(fieldNumber);
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
	public Object getField(int fieldNumber) {
		try {
			return recordBatch.get(0).getField(fieldNumber);
		} catch (IndexOutOfBoundsException e) {
			throw (new NoSuchFieldException());
		}
	}

	public String getString(int fieldNumber) {
		try {
			return (String) recordBatch.get(0).getField(fieldNumber);
		} catch (IndexOutOfBoundsException e) {
			throw (new NoSuchFieldException());
		}
	}

	public Integer getInteger(int fieldNumber) {
		try {
			return (Integer) recordBatch.get(0).getField(fieldNumber);
		} catch (IndexOutOfBoundsException e) {
			throw (new NoSuchFieldException());
		}
	}

	public Long getLong(int fieldNumber) {
		try {
			return (Long) recordBatch.get(0).getField(fieldNumber);
		} catch (IndexOutOfBoundsException e) {
			throw (new NoSuchFieldException());
		}
	}

	public Boolean getBoolean(int fieldNumber) {
		try {
			return (Boolean) recordBatch.get(0).getField(fieldNumber);
		} catch (IndexOutOfBoundsException e) {
			throw (new NoSuchFieldException());
		}
	}

	public Double getDouble(int fieldNumber) {
		try {
			return (Double) recordBatch.get(0).getField(fieldNumber);
		} catch (IndexOutOfBoundsException e) {
			throw (new NoSuchFieldException());
		}
	}

	public String getString(int recordNumber, int fieldNumber) {
		try {
			return (String) recordBatch.get(recordNumber).getField(fieldNumber);
		} catch (IndexOutOfBoundsException e) {
			throw (new NoSuchFieldException());
		}
	}

	public Integer getInteger(int recordNumber, int fieldNumber) {
		try {
			return (Integer) recordBatch.get(recordNumber).getField(fieldNumber);
		} catch (IndexOutOfBoundsException e) {
			throw (new NoSuchFieldException());
		}
	}

	public Long getLong(int recordNumber, int fieldNumber) {
		try {
			return (Long) recordBatch.get(recordNumber).getField(fieldNumber);
		} catch (IndexOutOfBoundsException e) {
			throw (new NoSuchFieldException());
		}
	}

	public Boolean getBoolean(int recordNumber, int fieldNumber) {
		try {
			return (Boolean) recordBatch.get(recordNumber).getField(fieldNumber);
		} catch (IndexOutOfBoundsException e) {
			throw (new NoSuchFieldException());
		}
	}

	public Double getDouble(int recordNumber, int fieldNumber) {
		try {
			return (Double) recordBatch.get(recordNumber).getField(fieldNumber);
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
	public void setField(int recordNumber, int fieldNumber, Object o) {
		try {
			recordBatch.get(recordNumber).setField(o, fieldNumber);
		} catch (IndexOutOfBoundsException e) {
			throw (new NoSuchRecordException());
		}
	}

	public void setString(int recordNumber, int fieldNumber, String o) {
		try {
			recordBatch.get(recordNumber).setField(o, fieldNumber);
		} catch (IndexOutOfBoundsException e) {
			throw (new NoSuchRecordException());
		}
	}

	public void setInteger(int recordNumber, int fieldNumber, Integer o) {
		try {
			recordBatch.get(recordNumber).setField(o, fieldNumber);
		} catch (IndexOutOfBoundsException e) {
			throw (new NoSuchRecordException());
		}
	}

	public void setLong(int recordNumber, int fieldNumber, Long o) {
		try {
			recordBatch.get(recordNumber).setField(o, fieldNumber);
		} catch (IndexOutOfBoundsException e) {
			throw (new NoSuchRecordException());
		}
	}

	public void setDouble(int recordNumber, int fieldNumber, Double o) {
		try {
			recordBatch.get(recordNumber).setField(o, fieldNumber);
		} catch (IndexOutOfBoundsException e) {
			throw (new NoSuchRecordException());
		}
	}

	public void setBoolean(int recordNumber, int fieldNumber, Boolean o) {
		try {
			recordBatch.get(recordNumber).setField(o, fieldNumber);
		} catch (IndexOutOfBoundsException e) {
			throw (new NoSuchRecordException());
		}
	}

	public void setString(int fieldNumber, String o) {
		try {
			recordBatch.get(0).setField(o, fieldNumber);
		} catch (IndexOutOfBoundsException e) {
			throw (new NoSuchRecordException());
		}
	}

	public void setInteger(int fieldNumber, Integer o) {
		try {
			recordBatch.get(0).setField(o, fieldNumber);
		} catch (IndexOutOfBoundsException e) {
			throw (new NoSuchRecordException());
		}
	}

	public void setLong(int fieldNumber, Long o) {
		try {
			recordBatch.get(0).setField(o, fieldNumber);
		} catch (IndexOutOfBoundsException e) {
			throw (new NoSuchRecordException());
		}
	}

	public void setDouble(int fieldNumber, Double o) {
		try {
			recordBatch.get(0).setField(o, fieldNumber);
		} catch (IndexOutOfBoundsException e) {
			throw (new NoSuchRecordException());
		}
	}

	public void setBoolean(int fieldNumber, Boolean o) {
		try {
			recordBatch.get(0).setField(o, fieldNumber);
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
	public void setField(int fieldNumber, Object o) {
		try {
			recordBatch.get(0).setField(o, fieldNumber);
		} catch (IndexOutOfBoundsException e) {
			throw (new NoSuchFieldException());
		}
	}

	/**
	 * @param recordNumber
	 *            Position of the record in the batch
	 * @return Value array containing the fields of the record
	 */
	public Tuple getRecord(int recordNumber) {
		try {
			return recordBatch.get(recordNumber);
		} catch (IndexOutOfBoundsException e) {
			throw (new NoSuchRecordException());
		}
	}

	/**
	 * @return Value array containing the fields of first the record
	 */
	public Tuple getRecord() {
		return getRecord(0);
	}

	public void getTupleInto(Tuple tuple) {

		if (tuple.getArity() == numOfFields) {
			try {
				Tuple source = recordBatch.get(0);
				for (int i = 0; i < numOfFields; i++) {
					tuple.setField(source.getField(i), i);
				}
			} catch (IndexOutOfBoundsException e) {
				throw (new NoSuchRecordException());
			}
		} else {
			throw (new RecordSizeMismatchException());
		}

	}

	/**
	 * Sets a record at the given position in the batch
	 * 
	 * @param recordNumber
	 *            Position of record in the batch
	 * @param tuple
	 *            Value to set
	 */
	public void setRecord(int recordNumber, Tuple tuple) {
		if (tuple.getArity() == numOfFields) {
			try {
				recordBatch.set(recordNumber, tuple);
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
	 * @param tuple
	 *            Value to set
	 */
	public void setRecord(Tuple tuple) {
		if (tuple.getArity() == numOfFields) {
			if (numOfRecords != 1) {
				recordBatch = new ArrayList<Tuple>(1);
				recordBatch.add(tuple);
			} else {
				recordBatch.set(0, tuple);
			}
		} else {
			throw (new RecordSizeMismatchException());
		}
	}

	/**
	 * Checks if the number of fields are equal to the batch field size then
	 * adds the Value array to the end of the batch
	 * 
	 * @param tuple
	 *            Value array to be added as the next record of the batch
	 */
	public void addRecord(Tuple tuple) {
		if (tuple.getArity() == numOfFields) {
			recordBatch.add(tuple);
			numOfRecords++;
		} else {
			throw new RecordSizeMismatchException();
		}
	}

	/**
	 * Creates a copy of the StreamRecord
	 * 
	 * @return Copy of the StreamRecord
	 * 
	 */
	public StreamRecord copy() {

		ByteArrayOutputStream buff = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(buff);
		StreamRecord newRecord = new StreamRecord();
		try {
			this.write(out);
			DataInputStream in = new DataInputStream(new ByteArrayInputStream(buff.toByteArray()));

			newRecord.read(in);
		} catch (Exception e) {
		}

		return newRecord;
	}

	private void writeTuple(Tuple tuple, DataOutput out) {

		Class[] basicTypes = new Class[tuple.getArity()];
		StringBuilder basicTypeNames = new StringBuilder();

		// TODO: exception for empty record - no getField!
		for (int i = 0; i < basicTypes.length; i++) {
			basicTypes[i] = tuple.getField(i).getClass();
			basicTypeNames.append(basicTypes[i].getName() + ",");
		}
		TypeInformation<? extends Tuple> typeInfo = TupleTypeInfo.getBasicTupleTypeInfo(basicTypes);

		StringValue typeVal = new StringValue(basicTypeNames.toString());

		@SuppressWarnings("unchecked")
		TupleSerializer<Tuple> tupleSerializer = (TupleSerializer<Tuple>) typeInfo
				.createSerializer();
		SerializationDelegate<Tuple> serializationDelegate = new SerializationDelegate<Tuple>(
				tupleSerializer);
		serializationDelegate.setInstance(tuple);
		try {
			typeVal.write(out);
			serializationDelegate.write(out);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private Tuple readTuple(DataInput in) throws IOException {

		StringValue typeVal = new StringValue();
		typeVal.read(in);
		// TODO: use Tokenizer
		String[] types = typeVal.getValue().split(",");
		Class[] basicTypes = new Class[types.length];
		for (int i = 0; i < types.length; i++) {
			try {
				basicTypes[i] = Class.forName(types[i]);
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		TypeInformation<? extends Tuple> typeInfo = TupleTypeInfo.getBasicTupleTypeInfo(basicTypes);
		TupleSerializer<Tuple> tupleSerializer = (TupleSerializer<Tuple>) typeInfo
				.createSerializer();

		DeserializationDelegate<Tuple> dd = new DeserializationDelegate<Tuple>(tupleSerializer);
		dd.setInstance(tupleSerializer.createInstance());
		dd.read(in);
		return dd.getInstance();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		uid.write(out);

		// Write the number of fields with an IntValue
		(new IntValue(numOfFields)).write(out);

		// Write the number of records with an IntValue
		(new IntValue(numOfRecords)).write(out);

		for (Tuple tuple : recordBatch) {
			writeTuple(tuple, out);
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
		recordBatch = new ArrayList<Tuple>();

		for (int k = 0; k < numOfRecords; ++k) {
			recordBatch.add(readTuple(in));
		}
	}

	public String toString() {
		StringBuilder outputString = new StringBuilder("[");

		for (Tuple tuple : recordBatch) {
			outputString.append(tuple + ",");
		}
		outputString.append("]");
		return outputString.toString();
	}

}
