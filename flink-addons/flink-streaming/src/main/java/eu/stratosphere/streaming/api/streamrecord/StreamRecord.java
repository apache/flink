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
import java.util.Arrays;
import java.util.List;

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
import eu.stratosphere.api.java.typeutils.BasicArrayTypeInfo;
import eu.stratosphere.api.java.typeutils.BasicTypeInfo;
import eu.stratosphere.api.java.typeutils.TupleTypeInfo;
import eu.stratosphere.types.TypeInformation;
import eu.stratosphere.api.java.typeutils.runtime.TupleSerializer;
import eu.stratosphere.core.io.IOReadableWritable;
import eu.stratosphere.pact.runtime.plugable.DeserializationDelegate;
import eu.stratosphere.pact.runtime.plugable.SerializationDelegate;

/**
 * Object for storing serializable records in batch (single records are
 * represented batches with one element) used for sending records between task
 * objects in Stratosphere stream processing. The elements of the batch are
 * Tuples.
 */
public class StreamRecord implements IOReadableWritable, Serializable {
	private static final long serialVersionUID = 1L;

	private List<Tuple> tupleBatch;
	private UID uid = new UID();
	private int numOfFields;
	private int numOfTuples;
	private int batchSize;

	private static final Class<?>[] CLASSES = new Class<?>[] { Tuple1.class, Tuple2.class,
			Tuple3.class, Tuple4.class, Tuple5.class, Tuple6.class, Tuple7.class, Tuple8.class,
			Tuple9.class, Tuple10.class, Tuple11.class, Tuple12.class, Tuple13.class,
			Tuple14.class, Tuple15.class, Tuple16.class, Tuple17.class, Tuple18.class,
			Tuple19.class, Tuple20.class, Tuple21.class, Tuple22.class };

	/**
	 * Creates a new empty instance for read
	 */
	public StreamRecord() {
	}

	/**
	 * Creates empty StreamRecord with number of fields set
	 * 
	 * @param numOfFields
	 *            number of fields
	 */
	public StreamRecord(int numOfFields) {
		this.numOfFields = numOfFields;
		this.numOfTuples = 0;
		this.batchSize = 1;
		tupleBatch = new ArrayList<Tuple>(batchSize);
	}

	/**
	 * Creates empty StreamRecord with number of fields and batch size set
	 * 
	 * @param numOfFields
	 *            Number of fields in the tuples
	 * @param batchSize
	 *            Batch size
	 */
	public StreamRecord(int numOfFields, int batchSize) {
		this.numOfFields = numOfFields;
		this.numOfTuples = 0;
		this.batchSize = batchSize;
		tupleBatch = new ArrayList<Tuple>(batchSize);
	}

	public StreamRecord(StreamRecord record) {
		this.numOfFields = record.getNumOfFields();
		this.numOfTuples = 0;
		tupleBatch = new ArrayList<Tuple>();
		this.uid = new UID(Arrays.copyOf(record.getId().getId(), 20));
		for (int i = 0; i < record.getNumOfTuples(); ++i) {
			this.tupleBatch.add(copyTuple(record.getTuple(i)));
		}
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
		this.batchSize = batchSize;
		tupleBatch = new ArrayList<Tuple>(batchSize);
		tupleBatch.add(tuple);
	}

	/**
	 * Creates a new batch of records containing the given Tuple list as
	 * elements
	 * 
	 * @param tupleList
	 *            Tuples to bes stored in the StreamRecord
	 */
	public StreamRecord(List<Tuple> tupleList) {
		numOfFields = tupleList.get(0).getArity();
		numOfTuples = tupleList.size();
		this.batchSize = numOfTuples;
		tupleBatch = new ArrayList<Tuple>(tupleList);
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
	 * Checks whether the record batch is empty
	 * 
	 * @return true if the batch is empty, false if it contains Tuples
	 */
	public boolean isEmpty() {
		return (this.numOfTuples == 0);
	}

	/**
	 * Remove all the contents inside StreamRecord.
	 */
	public void Clear() {
		this.numOfTuples = 0;
		tupleBatch.clear();
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
	public UID getId() {
		return uid;
	}

	/**
	 * Set the ID of the StreamRecord object
	 * 
	 * @param channelID
	 *            ID of the emitting task
	 * @return The StreamRecord object
	 */
	public StreamRecord setId(int channelID) {
		uid = new UID(channelID);
		return this;
	}

	/**
	 * Initializes the record batch elemnts to null
	 */
	public void initRecords() {
		tupleBatch.clear();
		for (int i = 0; i < batchSize; i++) {
			tupleBatch.add(null);
		}
		numOfTuples = batchSize;
	}

	/**
	 * Returns an iterable over the tuplebatch
	 * 
	 * @return batch iterable
	 */
	public Iterable<Tuple> getBatchIterable() {
		return (Iterable<Tuple>) tupleBatch;
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
	public Object getField(int fieldNumber) throws NoSuchTupleException, NoSuchFieldException {
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
	public Object getField(int tupleNumber, int fieldNumber) throws NoSuchTupleException,
			NoSuchFieldException {
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
	public Boolean getBoolean(int fieldNumber) throws NoSuchTupleException, NoSuchFieldException {
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
	public Boolean getBoolean(int tupleNumber, int fieldNumber) throws NoSuchTupleException,
			NoSuchFieldException {
		try {
			return (Boolean) getField(tupleNumber, fieldNumber);
		} catch (ClassCastException e) {
			throw new FieldTypeMismatchException();
		}
	}

	/**
	 * Get a Byte from thne given field of the first Tuple of the batch
	 * 
	 * @param fieldNumber
	 *            Position of the field in the tuple
	 * @return value of the field as Byte
	 * @throws NoSuchTupleException
	 *             , NoSuchFieldException
	 */
	public Byte getByte(int fieldNumber) throws NoSuchTupleException, NoSuchFieldException {
		return getByte(0, fieldNumber);
	}

	/**
	 * Get a Byte from the given field of the specified Tuple of the batch
	 * 
	 * @param tupleNumber
	 *            Position of the tuple in the batch
	 * @param fieldNumber
	 *            Position of the field in the tuple
	 * @return value of the field as Byte
	 * @throws NoSuchTupleException
	 *             , NoSuchFieldException
	 */
	public Byte getByte(int tupleNumber, int fieldNumber) throws NoSuchTupleException,
			NoSuchFieldException {
		try {
			return (Byte) getField(tupleNumber, fieldNumber);
		} catch (ClassCastException e) {
			throw new FieldTypeMismatchException();
		}
	}

	/**
	 * Get a Character from the given field of the first Tuple of the batch
	 * 
	 * @param fieldNumber
	 *            Position of the field in the tuple
	 * @return value of the field as Character
	 * @throws NoSuchTupleException
	 *             , NoSuchFieldException
	 */
	public Character getCharacter(int fieldNumber) throws NoSuchTupleException,
			NoSuchFieldException {
		return getCharacter(0, fieldNumber);
	}

	/**
	 * Get a Character from the given field of the specified Tuple of the batch
	 * 
	 * @param tupleNumber
	 *            Position of the tuple in the batch
	 * @param fieldNumber
	 *            Position of the field in the tuple
	 * @return value of the field as Character
	 * @throws NoSuchTupleException
	 *             , NoSuchFieldException
	 */
	public Character getCharacter(int tupleNumber, int fieldNumber) throws NoSuchTupleException,
			NoSuchFieldException {
		try {
			return (Character) getField(tupleNumber, fieldNumber);
		} catch (ClassCastException e) {
			throw new FieldTypeMismatchException();
		}
	}

	/**
	 * Get a Double from the given field of the first Tuple of the batch
	 * 
	 * @param fieldNumber
	 *            Position of the field in the tuple
	 * @return value of the field as Double
	 * @throws NoSuchTupleException
	 *             , NoSuchFieldException
	 */
	public Double getDouble(int fieldNumber) throws NoSuchTupleException, NoSuchFieldException {
		return getDouble(0, fieldNumber);
	}

	/**
	 * Get a Double from the given field of the specified Tuple of the batch
	 * 
	 * @param tupleNumber
	 *            Position of the tuple in the batch
	 * @param fieldNumber
	 *            Position of the field in the tuple
	 * @return value of the field as Double
	 * @throws NoSuchTupleException
	 *             , NoSuchFieldException
	 */
	public Double getDouble(int tupleNumber, int fieldNumber) throws NoSuchTupleException,
			NoSuchFieldException {
		try {
			return (Double) getField(tupleNumber, fieldNumber);
		} catch (ClassCastException e) {
			throw new FieldTypeMismatchException();
		}
	}

	/**
	 * Get a Float from the given field of the first Tuple of the batch
	 * 
	 * @param fieldNumber
	 *            Position of the field in the tuple
	 * @return value of the field as Float
	 * @throws NoSuchTupleException
	 *             , NoSuchFieldException
	 */
	public Float getFloat(int fieldNumber) throws NoSuchTupleException, NoSuchFieldException {
		return getFloat(0, fieldNumber);
	}

	/**
	 * Get a Float from the given field of the specified Tuple of the batch
	 * 
	 * @param tupleNumber
	 *            Position of the tuple in the batch
	 * @param fieldNumber
	 *            Position of the field in the tuple
	 * @return value of the field as Float
	 * @throws NoSuchTupleException
	 *             , NoSuchFieldException
	 */
	public Float getFloat(int tupleNumber, int fieldNumber) throws NoSuchTupleException,
			NoSuchFieldException {
		try {
			return (Float) getField(tupleNumber, fieldNumber);
		} catch (ClassCastException e) {
			throw new FieldTypeMismatchException();
		}
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
	public Integer getInteger(int fieldNumber) throws NoSuchTupleException, NoSuchFieldException {
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
	public Integer getInteger(int tupleNumber, int fieldNumber) throws NoSuchTupleException,
			NoSuchFieldException {
		try {
			return (Integer) getField(tupleNumber, fieldNumber);
		} catch (ClassCastException e) {
			throw new FieldTypeMismatchException();
		}
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
	public Long getLong(int fieldNumber) throws NoSuchTupleException, NoSuchFieldException {
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
	public Long getLong(int tupleNumber, int fieldNumber) throws NoSuchTupleException,
			NoSuchFieldException {
		try {
			return (Long) getField(tupleNumber, fieldNumber);
		} catch (ClassCastException e) {
			throw new FieldTypeMismatchException();
		}
	}

	/**
	 * Get a Short from the given field of the first Tuple of the batch
	 * 
	 * @param fieldNumber
	 *            Position of the field in the tuple
	 * @return value of the field as Short
	 * @throws NoSuchTupleException
	 *             , NoSuchFieldException
	 */
	public Short getShort(int fieldNumber) throws NoSuchTupleException, NoSuchFieldException {
		return getShort(0, fieldNumber);
	}

	/**
	 * Get a Short from the given field of the specified Tuple of the batch
	 * 
	 * @param tupleNumber
	 *            Position of the tuple in the batch
	 * @param fieldNumber
	 *            Position of the field in the tuple
	 * @return value of the field as Short
	 * @throws NoSuchTupleException
	 *             , NoSuchFieldException
	 */
	public Short getShort(int tupleNumber, int fieldNumber) throws NoSuchTupleException,
			NoSuchFieldException {
		try {
			return (Short) getField(tupleNumber, fieldNumber);
		} catch (ClassCastException e) {
			throw new FieldTypeMismatchException();
		}
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
	public String getString(int fieldNumber) throws NoSuchTupleException, NoSuchFieldException {
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
	public String getString(int tupleNumber, int fieldNumber) throws NoSuchTupleException,
			NoSuchFieldException {
		try {
			return (String) getField(tupleNumber, fieldNumber);
		} catch (ClassCastException e) {
			throw new FieldTypeMismatchException();
		}
	}

	/**
	 * Sets a field in the given position of the first record in the batch
	 * 
	 * @param fieldNumber
	 *            Position of the field in the record
	 * @param o
	 *            New value
	 * @throws NoSuchFieldException
	 *             the Tuple does not have this many fields
	 */
	public void setField(int fieldNumber, Object o) throws NoSuchFieldException {
		setField(0, fieldNumber, o);
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
	 * @throws NoSuchFieldException
	 *             the Tuple does not have this many fields
	 */
	// TODO: consider interaction with batch size
	public void setField(int tupleNumber, int fieldNumber, Object o) throws NoSuchFieldException {
		try {
			tupleBatch.get(tupleNumber).setField(o, fieldNumber);
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
	 * @throws NoSuchFieldException
	 *             the Tuple does not have this many fields
	 */
	public void setBoolean(int fieldNumber, Boolean b) throws NoSuchFieldException {
		setBoolean(0, fieldNumber, b);
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
	 * @throws NoSuchFieldException
	 *             the Tuple does not have this many fields
	 */
	public void setBoolean(int tupleNumber, int fieldNumber, Boolean b) throws NoSuchFieldException {
		setField(tupleNumber, fieldNumber, b);
	}

	/**
	 * Sets a Byte field in the given position of the first tuple in the batch
	 * 
	 * @param fieldNumber
	 *            Position of field in tuple
	 * @param b
	 *            New value
	 * @throws NoSuchFieldException
	 *             the Tuple does not have this many fields
	 */
	public void setByte(int fieldNumber, Byte b) throws NoSuchFieldException {
		setByte(0, fieldNumber, b);
	}

	/**
	 * Sets a Byte field in the given position of a specific tuple in the batch
	 * 
	 * @param tupleNumber
	 *            Position of tuple in batch
	 * @param fieldNumber
	 *            Position of field in tuple
	 * @param b
	 *            New value
	 * @throws NoSuchFieldException
	 *             the Tuple does not have this many fields
	 */
	public void setByte(int tupleNumber, int fieldNumber, Byte b) throws NoSuchFieldException {
		setField(tupleNumber, fieldNumber, b);
	}

	/**
	 * Sets a Character field in the given position of the first tuple in the
	 * batch
	 * 
	 * @param fieldNumber
	 *            Position of field in tuple
	 * @param c
	 *            New value
	 * @throws NoSuchFieldException
	 *             the Tuple does not have this many fields
	 */
	public void setCharacter(int fieldNumber, Character c) throws NoSuchFieldException {
		setCharacter(0, fieldNumber, c);
	}

	/**
	 * Sets a Character field in the given position of a specific tuple in the
	 * batch
	 * 
	 * @param tupleNumber
	 *            Position of tuple in batch
	 * @param fieldNumber
	 *            Position of field in tuple
	 * @param c
	 *            New value
	 * @throws NoSuchFieldException
	 *             the Tuple does not have this many fields
	 */
	public void setCharacter(int tupleNumber, int fieldNumber, Character c)
			throws NoSuchFieldException {
		setField(tupleNumber, fieldNumber, c);
	}

	/**
	 * Sets a Double field in the given position of the first tuple in the batch
	 * 
	 * @param fieldNumber
	 *            Position of field in tuple
	 * @param d
	 *            New value
	 * @throws NoSuchFieldException
	 *             the Tuple does not have this many fields
	 */
	public void setDouble(int fieldNumber, Double d) throws NoSuchFieldException {
		setDouble(0, fieldNumber, d);
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
	 * @throws NoSuchFieldException
	 *             the Tuple does not have this many fields
	 */
	public void setDouble(int tupleNumber, int fieldNumber, Double d) throws NoSuchFieldException {
		setField(tupleNumber, fieldNumber, d);
	}

	/**
	 * Sets a Float field in the given position of the first tuple in the batch
	 * 
	 * @param fieldNumber
	 *            Position of field in tuple
	 * @param f
	 *            New value
	 * @throws NoSuchFieldException
	 *             the Tuple does not have this many fields
	 */
	public void setFloat(int fieldNumber, Float f) throws NoSuchFieldException {
		setFloat(0, fieldNumber, f);
	}

	/**
	 * Sets a Double field in the given position of a specific tuple in the
	 * batch
	 * 
	 * @param tupleNumber
	 *            Position of tuple in batch
	 * @param fieldNumber
	 *            Position of field in tuple
	 * @param f
	 *            New value
	 * @throws NoSuchFieldException
	 *             the Tuple does not have this many fields
	 */
	public void setFloat(int tupleNumber, int fieldNumber, Float f) throws NoSuchFieldException {
		setField(tupleNumber, fieldNumber, f);
	}

	/**
	 * Sets an Integer field in the given position of the first tuple in the
	 * batch
	 * 
	 * @param fieldNumber
	 *            Position of field in tuple
	 * @param i
	 *            New value
	 * @throws NoSuchFieldException
	 *             the Tuple does not have this many fields
	 */
	public void setInteger(int fieldNumber, Integer i) throws NoSuchFieldException {
		setInteger(0, fieldNumber, i);
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
	 * @throws NoSuchFieldException
	 *             the Tuple does not have this many fields
	 */
	public void setInteger(int tupleNumber, int fieldNumber, Integer i) throws NoSuchFieldException {
		setField(tupleNumber, fieldNumber, i);
	}

	/**
	 * Sets a Long field in the given position of the first tuple in the batch
	 * 
	 * @param fieldNumber
	 *            Position of field in tuple
	 * @param l
	 *            New value
	 * @throws NoSuchFieldException
	 *             the Tuple does not have this many fields
	 */
	public void setLong(int fieldNumber, Long l) throws NoSuchFieldException {
		setLong(0, fieldNumber, l);
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
	 * @throws NoSuchFieldException
	 *             the Tuple does not have this many fields
	 */
	public void setLong(int tupleNumber, int fieldNumber, Long l) throws NoSuchFieldException {
		setField(tupleNumber, fieldNumber, l);
	}

	/**
	 * Sets a Short field in the given position of the first tuple in the batch
	 * 
	 * @param fieldNumber
	 *            Position of field in tuple
	 * @param s
	 *            New value
	 * @throws NoSuchFieldException
	 *             the Tuple does not have this many fields
	 */
	public void setShort(int fieldNumber, Short s) throws NoSuchFieldException {
		setShort(0, fieldNumber, s);
	}

	/**
	 * Sets a Short field in the given position of a specific tuple in the batch
	 * 
	 * @param tupleNumber
	 *            Position of tuple in batch
	 * @param fieldNumber
	 *            Position of field in tuple
	 * @param s
	 *            New value
	 * @throws NoSuchFieldException
	 *             the Tuple does not have this many fields
	 */
	public void setShort(int tupleNumber, int fieldNumber, Short s) throws NoSuchFieldException {
		setField(tupleNumber, fieldNumber, s);
	}

	/**
	 * Sets a String field in the given position of the first tuple in the batch
	 * 
	 * @param fieldNumber
	 *            Position of field in tuple
	 * @param str
	 *            New value
	 * @throws NoSuchFieldException
	 *             the Tuple does not have this many fields
	 */
	public void setString(int fieldNumber, String str) throws NoSuchFieldException {
		setField(0, fieldNumber, str);
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
	 * @throws NoSuchFieldException
	 *             the Tuple does not have this many fields
	 */
	public void setString(int tupleNumber, int fieldNumber, String str) throws NoSuchFieldException {
		setField(tupleNumber, fieldNumber, str);
	}

	/**
	 * @return First tuple of the batch
	 * @throws NoSuchTupleException
	 *             the StreamRecord does not have this many tuples
	 */
	public Tuple getTuple() throws NoSuchTupleException {
		return getTuple(0);
	}

	/**
	 * @param tupleNumber
	 *            Position of the record in the batch
	 * @return Chosen tuple
	 * @throws NoSuchTupleException
	 *             the Tuple does not have this many fields
	 */
	public Tuple getTuple(int tupleNumber) throws NoSuchTupleException {
		try {
			return tupleBatch.get(tupleNumber);
		} catch (IndexOutOfBoundsException e) {
			throw (new NoSuchTupleException());
		}
	}

	/**
	 * Gets the fields of the first tuple of the batch into the parameter tuple
	 * 
	 * @param tuple
	 *            Target tuple
	 * @throws NoSuchTupleException
	 *             , TupleSizeMismatchException
	 */
	public void getTupleInto(Tuple tuple) throws NoSuchTupleException, TupleSizeMismatchException {
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
	 * @throws NoSuchTupleException
	 *             , TupleSizeMismatchException
	 */
	public void getTupleInto(int tupleNumber, Tuple tuple) throws NoSuchTupleException,
			TupleSizeMismatchException {

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
	 * Sets the first tuple in the batch with the given tuple
	 * 
	 * @param tuple
	 *            Tuple to set
	 * @throws NoSuchTupleException
	 *             , TupleSizeMismatchException
	 */
	public void setTuple(Tuple tuple) throws NoSuchTupleException, TupleSizeMismatchException {
		setTuple(0, tuple);
	}

	/**
	 * Sets a tuple at the given position in the batch with the given tuple
	 * 
	 * @param tupleNumber
	 *            Position of tuple in the batch
	 * @param tuple
	 *            Value to set
	 * @throws NoSuchTupleException
	 *             , TupleSizeMismatchException
	 */
	public void setTuple(int tupleNumber, Tuple tuple) throws NoSuchTupleException,
			TupleSizeMismatchException {
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
	 * @throws TupleSizeMismatchException
	 *             Tuple specified has illegal size
	 */
	public void addTuple(Tuple tuple) throws TupleSizeMismatchException {
		addTuple(numOfTuples, tuple);
	}

	/**
	 * Checks if the number of fields are equal to the batch field size then
	 * inserts the Tuple to the given position into the recordbatch
	 * 
	 * @param index
	 *            Position of the added tuple
	 * @param tuple
	 *            Tuple to be added as the next record of the batch
	 * @throws TupleSizeMismatchException
	 *             Tuple specified has illegal size
	 */
	public void addTuple(int index, Tuple tuple) throws TupleSizeMismatchException {
		if (tuple.getArity() == numOfFields) {
			tupleBatch.add(index, tuple);
			numOfTuples++;
		} else {
			throw new TupleSizeMismatchException();
		}
	}

	/**
	 * Removes the tuple at the given position from the batch and returns it
	 * 
	 * @param index
	 *            Index of tuple to remove
	 * @return Removed tuple
	 * @throws TupleSizeMismatchException
	 *             Tuple specified has illegal size
	 */
	public Tuple removeTuple(int index) throws TupleSizeMismatchException {
		if (index < numOfTuples) {
			numOfTuples--;
			return tupleBatch.remove(index);
		} else {
			throw new TupleSizeMismatchException();
		}
	}

	/**
	 * Creates a copy of the StreamRecord object by Serializing and
	 * deserializing it
	 * 
	 * @return copy of the StreamRecord
	 * @throws IOException
	 *             Write or read failed
	 */
	public StreamRecord copySerialized() throws IOException {
		ByteArrayOutputStream buff = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(buff);
		StreamRecord newRecord = new StreamRecord();
		this.write(out);
		DataInputStream in = new DataInputStream(new ByteArrayInputStream(buff.toByteArray()));

		newRecord.read(in);
		return newRecord;
	}

	/**
	 * Creates a deep copy of the StreamRecord
	 * 
	 * @return Copy of the StreamRecord
	 * 
	 */
	public StreamRecord copy() {
		StreamRecord newRecord = new StreamRecord(numOfFields, numOfTuples);

		newRecord.uid = new UID(Arrays.copyOf(uid.getId(), 20));

		for (Tuple tuple : tupleBatch) {
			newRecord.tupleBatch.add(copyTuple(tuple));
		}

		return newRecord;
	}

	/**
	 * Creates deep copy of Tuple
	 * 
	 * @param tuple
	 *            Tuple to copy
	 * @return Copy of the tuple
	 */
	public static Tuple copyTuple(Tuple tuple) {
		// TODO: implement deep copy for arrays
		int numofFields = tuple.getArity();
		Tuple newTuple = null;
		try {
			newTuple = (Tuple) CLASSES[numofFields - 1].newInstance();

			for (int i = 0; i < numofFields; i++) {
				Class<? extends Object> type = tuple.getField(i).getClass();
				if (type.isArray()) {
					if (type.equals(Boolean[].class)) {
						Boolean[] arr = (Boolean[]) tuple.getField(i);
						newTuple.setField(Arrays.copyOf(arr, arr.length), i);
					} else if (type.equals(Byte[].class)) {
						Byte[] arr = (Byte[]) tuple.getField(i);
						newTuple.setField(Arrays.copyOf(arr, arr.length), i);
					} else if (type.equals(Character[].class)) {
						Character[] arr = (Character[]) tuple.getField(i);
						newTuple.setField(Arrays.copyOf(arr, arr.length), i);
					} else if (type.equals(Double[].class)) {
						Double[] arr = (Double[]) tuple.getField(i);
						newTuple.setField(Arrays.copyOf(arr, arr.length), i);
					} else if (type.equals(Float[].class)) {
						Float[] arr = (Float[]) tuple.getField(i);
						newTuple.setField(Arrays.copyOf(arr, arr.length), i);
					} else if (type.equals(Integer[].class)) {
						Integer[] arr = (Integer[]) tuple.getField(i);
						newTuple.setField(Arrays.copyOf(arr, arr.length), i);
					} else if (type.equals(Long[].class)) {
						Long[] arr = (Long[]) tuple.getField(i);
						newTuple.setField(Arrays.copyOf(arr, arr.length), i);
					} else if (type.equals(Short[].class)) {
						Short[] arr = (Short[]) tuple.getField(i);
						newTuple.setField(Arrays.copyOf(arr, arr.length), i);
					} else if (type.equals(String[].class)) {
						String[] arr = (String[]) tuple.getField(i);
						newTuple.setField(Arrays.copyOf(arr, arr.length), i);
					}
					newTuple.setField(tuple.getField(i), i);
				} else {
					newTuple.setField(tuple.getField(i), i);
				}
			}
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return newTuple;
	}

	/**
	 * copy tuples from the given record and append them to the end.
	 * 
	 * @param record
	 *            record to be appended
	 */
	public void appendRecord(StreamRecord record) {
		for (int i = 0; i < record.getNumOfTuples(); ++i) {
			this.addTuple(record.getTuple(i));
		}
	}

	/**
	 * Converts tuple field types to a byte array
	 * 
	 * @param tuple
	 * @return byte array representing types
	 */
	byte[] tupleTypesToByteArray(Tuple tuple) {
		byte[] typeNums = new byte[numOfFields];
		for (int i = 0; i < typeNums.length; i++) {
			Class<? extends Object> type = tuple.getField(i).getClass();
			if (type.equals(Boolean.class)) {
				typeNums[i] = 0;
			} else if (type.equals(Byte.class)) {
				typeNums[i] = 1;
			} else if (type.equals(Character.class)) {
				typeNums[i] = 2;
			} else if (type.equals(Double.class)) {
				typeNums[i] = 3;
			} else if (type.equals(Float.class)) {
				typeNums[i] = 4;
			} else if (type.equals(Integer.class)) {
				typeNums[i] = 5;
			} else if (type.equals(Long.class)) {
				typeNums[i] = 6;
			} else if (type.equals(Short.class)) {
				typeNums[i] = 7;
			} else if (type.equals(String.class)) {
				typeNums[i] = 8;
			} else if (type.equals(Boolean[].class)) {
				typeNums[i] = 9;
			} else if (type.equals(Byte[].class)) {
				typeNums[i] = 10;
			} else if (type.equals(Character[].class)) {
				typeNums[i] = 11;
			} else if (type.equals(Double[].class)) {
				typeNums[i] = 12;
			} else if (type.equals(Float[].class)) {
				typeNums[i] = 13;
			} else if (type.equals(Integer[].class)) {
				typeNums[i] = 14;
			} else if (type.equals(Long[].class)) {
				typeNums[i] = 15;
			} else if (type.equals(Short[].class)) {
				typeNums[i] = 16;
			} else if (type.equals(String[].class)) {
				typeNums[i] = 17;
			}
		}
		return typeNums;
	}

	/**
	 * Gets tuple field types from a byte array
	 * 
	 * @param byte array representing types
	 * @param numberOfFields
	 * @return TypeInfo array of field types
	 */
	@SuppressWarnings("rawtypes")
	TypeInformation[] tupleTypesFromByteArray(byte[] representation) {
		TypeInformation[] basicTypes = new TypeInformation[representation.length];
		for (int i = 0; i < basicTypes.length; i++) {
			switch (representation[i]) {
			case 0:
				basicTypes[i] = BasicTypeInfo.BOOLEAN_TYPE_INFO;
				break;
			case 1:
				basicTypes[i] = BasicTypeInfo.BYTE_TYPE_INFO;
				break;
			case 2:
				basicTypes[i] = BasicTypeInfo.CHAR_TYPE_INFO;
				break;
			case 3:
				basicTypes[i] = BasicTypeInfo.DOUBLE_TYPE_INFO;
				break;
			case 4:
				basicTypes[i] = BasicTypeInfo.FLOAT_TYPE_INFO;
				break;
			case 5:
				basicTypes[i] = BasicTypeInfo.INT_TYPE_INFO;
				break;
			case 6:
				basicTypes[i] = BasicTypeInfo.LONG_TYPE_INFO;
				break;
			case 7:
				basicTypes[i] = BasicTypeInfo.SHORT_TYPE_INFO;
				break;
			case 8:
				basicTypes[i] = BasicTypeInfo.STRING_TYPE_INFO;
				break;
			case 9:
				basicTypes[i] = BasicArrayTypeInfo.BOOLEAN_ARRAY_TYPE_INFO;
				break;
			case 10:
				basicTypes[i] = BasicArrayTypeInfo.BYTE_ARRAY_TYPE_INFO;
				break;
			case 11:
				basicTypes[i] = BasicArrayTypeInfo.CHAR_ARRAY_TYPE_INFO;
				break;
			case 12:
				basicTypes[i] = BasicArrayTypeInfo.DOUBLE_ARRAY_TYPE_INFO;
				break;
			case 13:
				basicTypes[i] = BasicArrayTypeInfo.FLOAT_ARRAY_TYPE_INFO;
				break;
			case 14:
				basicTypes[i] = BasicArrayTypeInfo.INT_ARRAY_TYPE_INFO;
				break;
			case 15:
				basicTypes[i] = BasicArrayTypeInfo.LONG_ARRAY_TYPE_INFO;
				break;
			case 16:
				basicTypes[i] = BasicArrayTypeInfo.SHORT_ARRAY_TYPE_INFO;
				break;
			case 17:
				basicTypes[i] = BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO;
				break;
			default:
				basicTypes[i] = BasicTypeInfo.STRING_TYPE_INFO;
				break;
			}
		}
		return basicTypes;
	}

	/**
	 * Write method definition for the IOReadableWritable interface
	 */
	@Override
	public void write(DataOutput out) throws IOException {

		uid.write(out);

		out.writeByte(numOfFields);
		out.writeInt(numOfTuples);

		byte[] typesInByte = tupleTypesToByteArray(getTuple());
		out.write(typesInByte);

		TupleTypeInfo<Tuple> typeInfo = new TupleTypeInfo<Tuple>(
				tupleTypesFromByteArray(typesInByte));
		TupleSerializer<Tuple> tupleSerializer = (TupleSerializer<Tuple>) typeInfo
				.createSerializer();
		SerializationDelegate<Tuple> serializationDelegate = new SerializationDelegate<Tuple>(
				tupleSerializer);

		for (Tuple tuple : tupleBatch) {
			serializationDelegate.setInstance(tuple);
			serializationDelegate.write(out);
		}
	}

	/**
	 * Read method definition for the IOReadableWritable interface
	 */
	@Override
	public void read(DataInput in) throws IOException {
		uid = new UID();
		uid.read(in);

		numOfFields = in.readByte();
		numOfTuples = in.readInt();

		tupleBatch = new ArrayList<Tuple>(numOfTuples);

		byte[] typesInByte = new byte[numOfFields];
		in.readFully(typesInByte, 0, numOfFields);

		TupleTypeInfo<Tuple> typeInfo = new TupleTypeInfo<Tuple>(
				tupleTypesFromByteArray(typesInByte));
		TupleSerializer<Tuple> tupleSerializer = typeInfo.createSerializer();

		DeserializationDelegate<Tuple> dd = new DeserializationDelegate<Tuple>(tupleSerializer);

		for (int k = 0; k < numOfTuples; ++k) {
			dd.setInstance(tupleSerializer.createInstance());
			dd.read(in);
			tupleBatch.add(dd.getInstance());
		}
	}

	/**
	 * Creates a String representation as a list of tuples
	 */
	public String toString() {
		StringBuilder outputString = new StringBuilder("[");

		String prefix = "";

		for (Tuple tuple : tupleBatch) {
			outputString.append(prefix);
			prefix = ",";
			outputString.append(tuple.toString());
		}
		outputString.append("]");
		return outputString.toString();
	}

}
