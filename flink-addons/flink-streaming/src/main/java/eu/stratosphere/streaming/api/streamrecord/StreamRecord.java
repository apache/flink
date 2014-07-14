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
 * Tuples.
 */
// TODO: update documentation
public class StreamRecord implements IOReadableWritable, Serializable {
	private static final long serialVersionUID = 1L;

	private List<Tuple> tupleBatch;
	private StringValue uid = new StringValue();
	private int numOfFields;
	private int numOfTuples;

	private static final Class<?>[] CLASSES = new Class<?>[] { Tuple1.class,
			Tuple2.class, Tuple3.class, Tuple4.class, Tuple5.class,
			Tuple6.class, Tuple7.class, Tuple8.class, Tuple9.class,
			Tuple10.class, Tuple11.class, Tuple12.class, Tuple13.class,
			Tuple14.class, Tuple15.class, Tuple16.class, Tuple17.class,
			Tuple18.class, Tuple19.class, Tuple20.class, Tuple21.class,
			Tuple22.class };

	// TODO implement equals, clone
	/**
	 * Creates a new empty instance for read
	 */
	public StreamRecord() {
	}

	public StreamRecord(int numOfFields) {
		this.numOfFields = numOfFields;
		this.numOfTuples = 0;
		tupleBatch = new ArrayList<Tuple>();

	}

	/**
	 * Creates a new batch of records containing only the given Tuple as element
	 * and sets desired batch size.
	 * 
	 * @param tuple
	 *            Tuple to be pushed to the record
	 * @param batchSize
	 *            Number of tuples in the record
	 */
	public StreamRecord(Tuple tuple, int batchSize) {
		numOfFields = tuple.getArity();
		numOfTuples = 1;
		tupleBatch = new ArrayList<Tuple>(batchSize);
		tupleBatch.add(tuple);

	}

	/**
	 * Given a Tuple, creates a new a record batch containing the Tuple as its
	 * only element
	 * 
	 * @param tuple
	 *            Tuple to be pushed to the record
	 */
	public StreamRecord(Tuple tuple) {
		this(tuple, 1);
	}

	/**
	 * @return Number of fields in the tuples
	 */
	public int getNumOfFields() {
		return numOfFields;
	}

	/**
	 * @return Number of tuples in the batch
	 */
	public int getNumOfTuples() {
		return numOfTuples;
	}

	/**
	 * @return The ID of the object
	 */
	public String getId() {
		return uid.getValue();
	}

	/**
	 * Set the ID of the StreamRecord object
	 * 
	 * @param channelID
	 *            ID of the emitting task
	 * @return The StreamRecord object
	 */
	// TODO: consider sequential ids
	public StreamRecord setId(String channelID) {
		UUID uuid = UUID.randomUUID();
		uid.setValue(channelID + "-" + uuid.toString());
		return this;
	}

	/**
	 * Returns the value of a field in the given position of the first tuple in
	 * the batch as an object, cast needed to obtain a typed version
	 * 
	 * @param fieldNumber
	 *            Position of the field in the tuple
	 * @return value of the field
	 * @throws NoSuchTupleException
	 *             , NoSuchFieldException
	 */
	public Object getField(int fieldNumber) throws NoSuchTupleException,
			NoSuchFieldException {
		return getField(0, fieldNumber);
	}

	/**
	 * Returns the value of a field in the given position of a specific tuple in
	 * the batch as an object, cast needed to obtain a typed version
	 * 
	 * @param tupleNumber
	 *            Position of the tuple in the batch
	 * @param fieldNumber
	 *            Position of the field in the tuple
	 * @return value of the field
	 * @throws NoSuchTupleException
	 *             , NoSuchFieldException
	 */
	public Object getField(int tupleNumber, int fieldNumber)
			throws NoSuchTupleException, NoSuchFieldException {
		Tuple tuple;
		try {
			tuple = tupleBatch.get(tupleNumber);
		} catch (IndexOutOfBoundsException e) {
			throw (new NoSuchTupleException());
		}
		try {
			return tuple.getField(fieldNumber);
		} catch (IndexOutOfBoundsException e) {
			throw (new NoSuchFieldException());
		}
	}

	/**
	 * Get a Boolean from the given field of the first Tuple of the batch
	 * 
	 * @param fieldNumber
	 *            Position of the field in the tuple
	 * @return value of the field as Boolean
	 * @throws NoSuchTupleException
	 *             , NoSuchFieldException
	 */
	public Boolean getBoolean(int fieldNumber) throws NoSuchTupleException,
			NoSuchFieldException {
		return getBoolean(0, fieldNumber);
	}

	/**
	 * Get a Boolean from the given field of the specified Tuple of the batch
	 * 
	 * @param tupleNumber
	 *            Position of the tuple in the batch
	 * @param fieldNumber
	 *            Position of the field in the tuple
	 * @return value of the field as Boolean
	 * @throws NoSuchTupleException
	 *             , NoSuchFieldException
	 */
	// TODO: add exception for cast for all getters
	public Boolean getBoolean(int tupleNumber, int fieldNumber)
			throws NoSuchTupleException, NoSuchFieldException {
		return (Boolean) getField(tupleNumber, fieldNumber);
	}

	/**
	 * Get a Double from the given field of the first Tuple of the batch
	 * 
	 * @param fieldNumber
	 *            Position of the field in the tuple
	 * @return value of the field as Double * @throws NoSuchTupleException ,
	 *         NoSuchFieldException
	 */
	public Double getDouble(int fieldNumber) throws NoSuchTupleException,
			NoSuchFieldException {
		return getDouble(0, fieldNumber);
	}

	/**
	 * Get a Double from the given field of the specified Tuple of the batch
	 * 
	 * @param tupleNumber
	 *            Position of the tuple in the batch
	 * @param fieldNumber
	 *            Position of the field in the tuple
	 * @return value of the field as Double * @throws NoSuchTupleException ,
	 *         NoSuchFieldException
	 */
	public Double getDouble(int tupleNumber, int fieldNumber)
			throws NoSuchTupleException, NoSuchFieldException {
		return (Double) getField(tupleNumber, fieldNumber);
	}

	/**
	 * Get an Integer from the given field of the first Tuple of the batch
	 * 
	 * @param fieldNumber
	 *            Position of the field in the tuple
	 * @return value of the field as Integer
	 * @throws NoSuchTupleException
	 *             , NoSuchFieldException
	 */
	public Integer getInteger(int fieldNumber) throws NoSuchTupleException,
			NoSuchFieldException {
		return getInteger(0, fieldNumber);
	}

	/**
	 * Get an Integer from the given field of the specified Tuple of the batch
	 * 
	 * @param tupleNumber
	 *            Position of the tuple in the batch
	 * @param fieldNumber
	 *            Position of the field in the tuple
	 * @return value of the field as Integer
	 * @throws NoSuchTupleException
	 *             , NoSuchFieldException
	 */
	public Integer getInteger(int tupleNumber, int fieldNumber)
			throws NoSuchTupleException, NoSuchFieldException {
		return (Integer) getField(tupleNumber, fieldNumber);
	}

	/**
	 * Get a Long from the given field of the first Tuple of the batch
	 * 
	 * @param fieldNumber
	 *            Position of the field in the tuple
	 * @return value of the field as Long
	 * @throws NoSuchTupleException
	 *             , NoSuchFieldException
	 */
	public Long getLong(int fieldNumber) throws NoSuchTupleException,
			NoSuchFieldException {
		return getLong(0, fieldNumber);
	}

	/**
	 * Get a Long from the given field of the specified Tuple of the batch
	 * 
	 * @param tupleNumber
	 *            Position of the tuple in the batch
	 * @param fieldNumber
	 *            Position of the field in the tuple
	 * @return value of the field as Long
	 * @throws NoSuchTupleException
	 *             , NoSuchFieldException
	 */
	public Long getLong(int tupleNumber, int fieldNumber)
			throws NoSuchTupleException, NoSuchFieldException {
		return (Long) getField(tupleNumber, fieldNumber);
	}

	/**
	 * Get a String from the given field of the first Tuple of the batch
	 * 
	 * @param fieldNumber
	 *            Position of the field in the tuple
	 * @return value of the field as String
	 * @throws NoSuchTupleException
	 *             , NoSuchFieldException
	 */
	public String getString(int fieldNumber) throws NoSuchTupleException,
			NoSuchFieldException {
		return getString(0, fieldNumber);
	}

	/**
	 * Get a String from the given field of the specified Tuple of the batch
	 * 
	 * @param tupleNumber
	 *            Position of the tuple in the batch
	 * @param fieldNumber
	 *            Position of the field in the tuple
	 * @return value of the field as String
	 */
	public String getString(int tupleNumber, int fieldNumber)
			throws NoSuchTupleException, NoSuchFieldException {
		return (String) getField(tupleNumber, fieldNumber);
	}

	/**
	 * Sets a field in the given position of a specific tuple in the batch
	 * 
	 * @param tupleNumber
	 *            Position of tuple in batch
	 * @param fieldNumber
	 *            Position of field in tuple
	 * @param o
	 *            New value
	 */
	public void setField(int tupleNumber, int fieldNumber, Object o) {
		try {
			tupleBatch.get(tupleNumber).setField(o, fieldNumber);
		} catch (IndexOutOfBoundsException e) {
			throw (new NoSuchTupleException());
		}
	}

	/**
	 * Sets a String field in the given position of a specific tuple in the
	 * batch
	 * 
	 * @param tupleNumber
	 *            Position of tuple in batch
	 * @param fieldNumber
	 *            Position of field in tuple
	 * @param str
	 *            New value
	 */
	public void setString(int tupleNumber, int fieldNumber, String str) {
		try {
			tupleBatch.get(tupleNumber).setField(str, fieldNumber);
		} catch (IndexOutOfBoundsException e) {
			throw (new NoSuchTupleException());
		}
	}

	/**
	 * Sets an Integer field in the given position of a specific tuple in the
	 * batch
	 * 
	 * @param tupleNumber
	 *            Position of tuple in batch
	 * @param fieldNumber
	 *            Position of field in tuple
	 * @param i
	 *            New value
	 */
	public void setInteger(int tupleNumber, int fieldNumber, Integer i) {
		try {
			tupleBatch.get(tupleNumber).setField(i, fieldNumber);
		} catch (IndexOutOfBoundsException e) {
			throw (new NoSuchTupleException());
		}
	}

	/**
	 * Sets a Long field in the given position of a specific tuple in the batch
	 * 
	 * @param tupleNumber
	 *            Position of tuple in batch
	 * @param fieldNumber
	 *            Position of field in tuple
	 * @param l
	 *            New value
	 */
	public void setLong(int tupleNumber, int fieldNumber, Long l) {
		try {
			tupleBatch.get(tupleNumber).setField(l, fieldNumber);
		} catch (IndexOutOfBoundsException e) {
			throw (new NoSuchTupleException());
		}
	}

	/**
	 * Sets a Double field in the given position of a specific tuple in the
	 * batch
	 * 
	 * @param tupleNumber
	 *            Position of tuple in batch
	 * @param fieldNumber
	 *            Position of field in tuple
	 * @param d
	 *            New value
	 */
	public void setDouble(int tupleNumber, int fieldNumber, Double d) {
		try {
			tupleBatch.get(tupleNumber).setField(d, fieldNumber);
		} catch (IndexOutOfBoundsException e) {
			throw (new NoSuchTupleException());
		}
	}

	/**
	 * Sets a Boolean field in the given position of a specific tuple in the
	 * batch
	 * 
	 * @param tupleNumber
	 *            Position of tuple in batch
	 * @param fieldNumber
	 *            Position of field in tuple
	 * @param b
	 *            New value
	 */
	public void setBoolean(int tupleNumber, int fieldNumber, Boolean b) {
		try {
			tupleBatch.get(tupleNumber).setField(b, fieldNumber);
		} catch (IndexOutOfBoundsException e) {
			throw (new NoSuchTupleException());
		}
	}

	/**
	 * Sets a String field in the given position of the first tuple in the batch
	 * 
	 * @param fieldNumber
	 *            Position of field in tuple
	 * @param str
	 *            New value
	 */
	public void setString(int fieldNumber, String str) {
		try {
			tupleBatch.get(0).setField(str, fieldNumber);
		} catch (IndexOutOfBoundsException e) {
			throw (new NoSuchTupleException());
		}
	}

	/**
	 * Sets an Integer field in the given position of the first tuple in the
	 * batch
	 * 
	 * @param fieldNumber
	 *            Position of field in tuple
	 * @param i
	 *            New value
	 */
	public void setInteger(int fieldNumber, Integer i) {
		try {
			tupleBatch.get(0).setField(i, fieldNumber);
		} catch (IndexOutOfBoundsException e) {
			throw (new NoSuchTupleException());
		}
	}

	/**
	 * Sets a Long field in the given position of the first tuple in the batch
	 * 
	 * @param fieldNumber
	 *            Position of field in tuple
	 * @param l
	 *            New value
	 */
	public void setLong(int fieldNumber, Long l) {
		try {
			tupleBatch.get(0).setField(l, fieldNumber);
		} catch (IndexOutOfBoundsException e) {
			throw (new NoSuchTupleException());
		}
	}

	/**
	 * Sets a Double field in the given position of the first tuple in the batch
	 * 
	 * @param fieldNumber
	 *            Position of field in tuple
	 * @param d
	 *            New value
	 */
	public void setDouble(int fieldNumber, Double d) {
		try {
			tupleBatch.get(0).setField(d, fieldNumber);
		} catch (IndexOutOfBoundsException e) {
			throw (new NoSuchTupleException());
		}
	}

	/**
	 * Sets a Boolean field in the given position of the first tuple in the
	 * batch
	 * 
	 * @param fieldNumber
	 *            Position of field in tuple
	 * @param b
	 *            New value
	 */
	public void setBoolean(int fieldNumber, Boolean b) {
		try {
			tupleBatch.get(0).setField(b, fieldNumber);
		} catch (IndexOutOfBoundsException e) {
			throw (new NoSuchTupleException());
		}
	}

	/**
	 * Sets a field in the given position of the first record in the batch
	 * 
	 * @param fieldNumber
	 *            Position of the field in the record
	 * @param o
	 *            New value
	 */
	public void setField(int fieldNumber, Object o) {
		try {
			tupleBatch.get(0).setField(o, fieldNumber);
		} catch (IndexOutOfBoundsException e) {
			throw (new NoSuchFieldException());
		}
	}

	/**
	 * @param tupleNumber
	 *            Position of the record in the batch
	 * @return Chosen tuple
	 */
	public Tuple getTuple(int tupleNumber) {
		try {
			return tupleBatch.get(tupleNumber);
		} catch (IndexOutOfBoundsException e) {
			throw (new NoSuchTupleException());
		}
	}

	/**
	 * @return First tuple of the batch
	 */
	public Tuple getTuple() {
		return getTuple(0);
	}

	/**
	 * Gets the fields of the first tuple of the batch into the parameter tuple
	 * 
	 * @param tuple
	 *            Target tuple
	 */
	public void getTupleInto(Tuple tuple) {
		getTupleInto(0, tuple);
	}

	/**
	 * Gets the fields of the specified tuple of the batch into the parameter
	 * tuple
	 * 
	 * @param tupleNumber
	 *            Position of the tuple to be written out
	 * 
	 * @param tuple
	 *            Target tuple
	 */
	public void getTupleInto(int tupleNumber, Tuple tuple) {

		if (tuple.getArity() == numOfFields) {
			try {
				Tuple source = tupleBatch.get(tupleNumber);
				for (int i = 0; i < numOfFields; i++) {
					tuple.setField(source.getField(i), i);
				}
			} catch (IndexOutOfBoundsException e) {
				throw (new NoSuchTupleException());
			}
		} else {
			throw (new TupleSizeMismatchException());
		}

	}

	/**
	 * Sets the first tuple in the batch
	 * 
	 * @param tuple
	 *            Tuple to set
	 */
	// TODO: refactor this functionality - why new list?
	public void setTuple(Tuple tuple) {
		if (tuple.getArity() == numOfFields) {
			if (numOfTuples != 1) {
				tupleBatch = new ArrayList<Tuple>(1);
				tupleBatch.add(tuple);
			} else {
				tupleBatch.set(0, tuple);
			}
		} else {
			throw (new TupleSizeMismatchException());
		}
	}

	/**
	 * Sets a tuple at the given position in the batch
	 * 
	 * @param tupleNumber
	 *            Position of tuple in the batch
	 * @param tuple
	 *            Value to set
	 */
	public void setTuple(int tupleNumber, Tuple tuple) {
		if (tuple.getArity() == numOfFields) {
			try {
				tupleBatch.set(tupleNumber, tuple);
			} catch (IndexOutOfBoundsException e) {
				throw (new NoSuchTupleException());
			}
		} else {
			throw (new TupleSizeMismatchException());
		}
	}

	/**
	 * Checks if the number of fields are equal to the batch field size then
	 * adds the Tuple to the end of the batch
	 * 
	 * @param tuple
	 *            Tuple to be added as the next record of the batch
	 */
	public void addTuple(Tuple tuple) {
		if (tuple.getArity() == numOfFields) {
			tupleBatch.add(tuple);
			numOfTuples++;
		} else {
			throw new TupleSizeMismatchException();
		}
	}

	/**
	 * Creates a deep copy of the StreamRecord
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
			DataInputStream in = new DataInputStream(new ByteArrayInputStream(
					buff.toByteArray()));

			newRecord.read(in);
		} catch (Exception e) {
		}

		return newRecord;
	}

	public static Tuple copyTuple(Tuple tuple) {
		int numofFields = tuple.getArity();
		Tuple newTuple = null;
		try {
			newTuple = (Tuple) CLASSES[numofFields - 1].newInstance();
		} catch (Exception e) {

		}

		for (int i = 0; i < numofFields; i++) {
			newTuple.setField(tuple.getField(i), i);
		}

		return newTuple;
	}

	/**
	 * Writes tuple to the specified DataOutput
	 * 
	 * @param tuple
	 *            Tuple to be written
	 * @param out
	 *            Output chosen
	 */
	private void writeTuple(Tuple tuple, DataOutput out) {

		@SuppressWarnings("rawtypes")
		Class[] basicTypes = new Class[tuple.getArity()];
		StringBuilder basicTypeNames = new StringBuilder();

		// TODO: exception for empty record - no getField!
		for (int i = 0; i < basicTypes.length; i++) {
			basicTypes[i] = tuple.getField(i).getClass();
			basicTypeNames.append(basicTypes[i].getName() + ",");
		}
		TypeInformation<? extends Tuple> typeInfo = TupleTypeInfo
				.getBasicTupleTypeInfo(basicTypes);

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

	/**
	 * Reads a tuple from the specified DataInput
	 * 
	 * @param in
	 *            Input chosen
	 * @return Tuple read
	 * @throws IOException
	 */
	private Tuple readTuple(DataInput in) throws IOException {

		StringValue typeVal = new StringValue();
		typeVal.read(in);
		// TODO: use StringTokenizer
		String[] types = typeVal.getValue().split(",");
		@SuppressWarnings("rawtypes")
		Class[] basicTypes = new Class[types.length];
		for (int i = 0; i < types.length; i++) {
			try {
				basicTypes[i] = Class.forName(types[i]);
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		TypeInformation<? extends Tuple> typeInfo = TupleTypeInfo
				.getBasicTupleTypeInfo(basicTypes);
		@SuppressWarnings("unchecked")
		TupleSerializer<Tuple> tupleSerializer = (TupleSerializer<Tuple>) typeInfo
				.createSerializer();

		DeserializationDelegate<Tuple> dd = new DeserializationDelegate<Tuple>(
				tupleSerializer);
		dd.setInstance(tupleSerializer.createInstance());
		dd.read(in);
		return dd.getInstance();
	}

	/**
	 * Write method definition for the IOReadableWritable interface
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		uid.write(out);

		// Write the number of fields with an IntValue
		(new IntValue(numOfFields)).write(out);

		// Write the number of records with an IntValue
		(new IntValue(numOfTuples)).write(out);

		for (Tuple tuple : tupleBatch) {
			writeTuple(tuple, out);
		}
	}

	/**
	 * Read method definition for the IOReadableWritable interface
	 */
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
		numOfTuples = numOfRecordsValue.getValue();

		// Make sure the fields have numOfFields elements
		tupleBatch = new ArrayList<Tuple>();

		for (int k = 0; k < numOfTuples; ++k) {
			tupleBatch.add(readTuple(in));
		}
	}

	/**
	 * Creates a String representation as a list of tuples
	 */
	public String toString() {
		StringBuilder outputString = new StringBuilder("[");

		for (Tuple tuple : tupleBatch) {
			outputString.append(tuple + ",");
		}
		outputString.append("]");
		return outputString.toString();
	}

}
