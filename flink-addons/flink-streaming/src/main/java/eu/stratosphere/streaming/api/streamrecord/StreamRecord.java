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
	 * @return The ID of the object
	 */
	public String getId() {
		return uid.getValue();
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
	 */
	public Object getField(int tupleNumber, int fieldNumber) {
		try {
			return tupleBatch.get(tupleNumber).getField(fieldNumber);
		} catch (IndexOutOfBoundsException e) {
			throw (new NoSuchTupleException());
		}
	}

	/**
	 * Returns the value of a field in the given position of the first tuple in
	 * the batch as an object, cast needed to obtain a typed version
	 * 
	 * @param fieldNumber
	 *            Position of the field in the tuple
	 * @return value of the field
	 */
	public Object getField(int fieldNumber) {
		try {
			return tupleBatch.get(0).getField(fieldNumber);
		} catch (IndexOutOfBoundsException e) {
			throw (new NoSuchFieldException());
		}
	}

	/**
	 * Get a String from the given field of the first Tuple of the batch
	 * 
	 * @param fieldNumber
	 *            Position of the field in the tuple
	 * @return value of the field as String
	 */
	public String getString(int fieldNumber) {
		try {
			return (String) tupleBatch.get(0).getField(fieldNumber);
		} catch (IndexOutOfBoundsException e) {
			throw (new NoSuchFieldException());
		}
	}

	/**
	 * Get an Integer from the given field of the first Tuple of the batch
	 * 
	 * @param fieldNumber
	 *            Position of the field in the tuple
	 * @return value of the field as Integer
	 */
	public Integer getInteger(int fieldNumber) {
		try {
			return (Integer) tupleBatch.get(0).getField(fieldNumber);
		} catch (IndexOutOfBoundsException e) {
			throw (new NoSuchFieldException());
		}
	}

	/**
	 * Get a Long from the given field of the first Tuple of the batch
	 * 
	 * @param fieldNumber
	 *            Position of the field in the tuple
	 * @return value of the field as Long
	 */
	public Long getLong(int fieldNumber) {
		try {
			return (Long) tupleBatch.get(0).getField(fieldNumber);
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
	 */
	public Boolean getBoolean(int fieldNumber) {
		try {
			return (Boolean) tupleBatch.get(0).getField(fieldNumber);
		} catch (IndexOutOfBoundsException e) {
			throw (new NoSuchFieldException());
		}
	}

	/**
	 * Get a Double from the given field of the first Tuple of the batch
	 * 
	 * @param fieldNumber
	 *            Position of the field in the tuple
	 * @return value of the field as Double
	 */
	public Double getDouble(int fieldNumber) {
		try {
			return (Double) tupleBatch.get(0).getField(fieldNumber);
		} catch (IndexOutOfBoundsException e) {
			throw (new NoSuchFieldException());
		}
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
	public String getString(int tupleNumber, int fieldNumber) {
		try {
			return (String) tupleBatch.get(tupleNumber).getField(fieldNumber);
		} catch (IndexOutOfBoundsException e) {
			throw (new NoSuchFieldException());
		}
	}

	/**
	 * Get an Integer from the given field of the specified Tuple of the batch
	 * 
	 * @param tupleNumber
	 *            Position of the tuple in the batch
	 * @param fieldNumber
	 *            Position of the field in the tuple
	 * @return value of the field as Integer
	 */
	public Integer getInteger(int tupleNumber, int fieldNumber) {
		try {
			return (Integer) tupleBatch.get(tupleNumber).getField(fieldNumber);
		} catch (IndexOutOfBoundsException e) {
			throw (new NoSuchFieldException());
		}
	}

	/**
	 * Get a Long from the given field of the specified Tuple of the batch
	 * 
	 * @param tupleNumber
	 *            Position of the tuple in the batch
	 * @param fieldNumber
	 *            Position of the field in the tuple
	 * @return value of the field as Long
	 */
	public Long getLong(int tupleNumber, int fieldNumber) {
		try {
			return (Long) tupleBatch.get(tupleNumber).getField(fieldNumber);
		} catch (IndexOutOfBoundsException e) {
			throw (new NoSuchFieldException());
		}
	}

	/**
	 * Get a Boolean from the given field of the specified Tuple of the batch
	 * 
	 * @param tupleNumber
	 *            Position of the tuple in the batch
	 * @param fieldNumber
	 *            Position of the field in the tuple
	 * @return value of the field as Boolean
	 */
	public Boolean getBoolean(int tupleNumber, int fieldNumber) {
		try {
			return (Boolean) tupleBatch.get(tupleNumber).getField(fieldNumber);
		} catch (IndexOutOfBoundsException e) {
			throw (new NoSuchFieldException());
		}
	}

	/**
	 * Get a Double from the given field of the specified Tuple of the batch
	 * 
	 * @param tupleNumber
	 *            Position of the tuple in the batch
	 * @param fieldNumber
	 *            Position of the field in the tuple
	 * @return value of the field as Double
	 */
	public Double getDouble(int tupleNumber, int fieldNumber) {
		try {
			return (Double) tupleBatch.get(tupleNumber).getField(fieldNumber);
		} catch (IndexOutOfBoundsException e) {
			throw (new NoSuchFieldException());
		}
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
	public void setDouble(int tupleNumber, int fieldNumber, Double tuple) {
		try {
			tupleBatch.get(tupleNumber).setField(tuple, fieldNumber);
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

	//TODO: doc from here on
	public void getTupleInto(Tuple tuple) {

		if (tuple.getArity() == numOfFields) {
			try {
				Tuple source = tupleBatch.get(0);
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
				tupleBatch.set(recordNumber, tuple);
			} catch (IndexOutOfBoundsException e) {
				throw (new NoSuchTupleException());
			}
		} else {
			throw (new TupleSizeMismatchException());
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
	 * Checks if the number of fields are equal to the batch field size then
	 * adds the Value array to the end of the batch
	 * 
	 * @param tuple
	 *            Value array to be added as the next record of the batch
	 */
	public void addRecord(Tuple tuple) {
		if (tuple.getArity() == numOfFields) {
			tupleBatch.add(tuple);
			numOfTuples++;
		} else {
			throw new TupleSizeMismatchException();
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

	private void writeTuple(Tuple tuple, DataOutput out) {

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

		TypeInformation<? extends Tuple> typeInfo = TupleTypeInfo
				.getBasicTupleTypeInfo(basicTypes);
		TupleSerializer<Tuple> tupleSerializer = (TupleSerializer<Tuple>) typeInfo
				.createSerializer();

		DeserializationDelegate<Tuple> dd = new DeserializationDelegate<Tuple>(
				tupleSerializer);
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
		(new IntValue(numOfTuples)).write(out);

		for (Tuple tuple : tupleBatch) {
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
		numOfTuples = numOfRecordsValue.getValue();

		// Make sure the fields have numOfFields elements
		tupleBatch = new ArrayList<Tuple>();

		for (int k = 0; k < numOfTuples; ++k) {
			tupleBatch.add(readTuple(in));
		}
	}

	public String toString() {
		StringBuilder outputString = new StringBuilder("[");

		for (Tuple tuple : tupleBatch) {
			outputString.append(tuple + ",");
		}
		outputString.append("]");
		return outputString.toString();
	}

}
