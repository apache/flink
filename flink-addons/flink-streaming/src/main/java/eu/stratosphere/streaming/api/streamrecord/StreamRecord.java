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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.api.java.tuple.Tuple10;
import eu.stratosphere.api.java.tuple.Tuple11;
import eu.stratosphere.api.java.tuple.Tuple12;
import eu.stratosphere.api.java.tuple.Tuple13;
import eu.stratosphere.api.java.tuple.Tuple14;
import eu.stratosphere.api.java.tuple.Tuple15;
import eu.stratosphere.api.java.tuple.Tuple16;
import eu.stratosphere.api.java.tuple.Tuple17;
import eu.stratosphere.api.java.tuple.Tuple18;
import eu.stratosphere.api.java.tuple.Tuple19;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.tuple.Tuple20;
import eu.stratosphere.api.java.tuple.Tuple21;
import eu.stratosphere.api.java.tuple.Tuple22;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.api.java.tuple.Tuple4;
import eu.stratosphere.api.java.tuple.Tuple5;
import eu.stratosphere.api.java.tuple.Tuple6;
import eu.stratosphere.api.java.tuple.Tuple7;
import eu.stratosphere.api.java.tuple.Tuple8;
import eu.stratosphere.api.java.tuple.Tuple9;
import eu.stratosphere.api.java.typeutils.TypeInformation;
import eu.stratosphere.api.java.typeutils.runtime.TupleSerializer;
import eu.stratosphere.core.io.IOReadableWritable;
import eu.stratosphere.pact.runtime.plugable.DeserializationDelegate;
import eu.stratosphere.pact.runtime.plugable.SerializationDelegate;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.types.Value;

/**
 * Object for storing serializable records in batch (single records are
 * represented batches with one element) used for sending records between task
 * objects in Stratosphere stream processing. The elements of the batch are
 * Value arrays.
 */
public class StreamRecord<T extends Tuple> implements IOReadableWritable, Serializable {
	private static final long serialVersionUID = 1L;

	private List<T> recordBatch;
	private StringValue uid = new StringValue("");
	private int numOfFields;
	private int numOfRecords;
	private Class<? extends Tuple> clazz = null;

	private static final Class<?>[] CLASSES = new Class<?>[] { Tuple1.class, Tuple2.class, Tuple3.class, Tuple4.class,
			Tuple5.class, Tuple6.class, Tuple7.class, Tuple8.class, Tuple9.class, Tuple10.class, Tuple11.class,
			Tuple12.class, Tuple13.class, Tuple14.class, Tuple15.class, Tuple16.class, Tuple17.class, Tuple18.class,
			Tuple19.class, Tuple20.class, Tuple21.class, Tuple22.class };

	// TODO implement equals, clone
	/**
	 * Creates a new empty batch of records and sets the field number to one
	 */
	public StreamRecord() {
		this.numOfFields = 1;
		recordBatch = new ArrayList<T>();
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
		recordBatch = new ArrayList<T>();
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
		recordBatch = new ArrayList<T>(batchSize);
	}

	/**
	 * Given an array of Values, creates a new a record batch containing the
	 * array as its first element
	 * 
	 * @param values
	 *            Array containing the Values for the first record in the batch
	 */
	public StreamRecord(T tuple) {
		this(tuple.getArity(), 0);
		addRecord(tuple);
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
	public Value getField(int recordNumber, int fieldNumber) {
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
	public Value getField(int fieldNumber) {
		try {
			return recordBatch.get(0).getField(fieldNumber);
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

	// TODO do not use this
	private void setClass(T tuple) {
		if (clazz == null) {
			clazz = tuple.getClass();
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
	public void setRecord(int recordNumber, T tuple) {
		if (tuple.getArity() == numOfFields) {
			try {
				setClass(tuple);
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
	public void setRecord(T tuple) {
		if (tuple.getArity() == numOfFields) {
			setClass(tuple);
			if (numOfRecords != 1) {
				recordBatch = new ArrayList<T>(1);
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
	public void addRecord(T tuple) {
		if (tuple.getArity() == numOfFields) {
			setClass(tuple);
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
		// TODO implement!
		return null;
	}

	private void writeTuple(Tuple tuple, DataOutput out) {
		TypeInformation<? extends Tuple> typeInfo = TypeInformation.getForObject(tuple);

		@SuppressWarnings("unchecked")
		TupleSerializer<Tuple> tupleSerializer = (TupleSerializer<Tuple>) typeInfo.createSerializer();
		SerializationDelegate<Tuple> serializationDelegate = new SerializationDelegate<Tuple>(tupleSerializer);
		serializationDelegate.setInstance(tuple);
	}

	private void readTuple(Tuple tuple, DataInput in, int arity) throws IOException {
		// TODO get the type somehow!s
		TypeInformation<? extends Tuple> typeInfo = null;
		TupleSerializer<Tuple> tupleSerializer = (TupleSerializer<Tuple>) typeInfo.createSerializer();

		DeserializationDelegate<Tuple> dd = new DeserializationDelegate<Tuple>(tupleSerializer);
		dd.setInstance(tupleSerializer.createInstance());
		dd.read(in);
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
		recordBatch = new ArrayList<T>();

		for (int k = 0; k < numOfRecords; ++k) {
			T tuple = null;
			readTuple(tuple, in, numOfFields);
			recordBatch.add(tuple);
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
