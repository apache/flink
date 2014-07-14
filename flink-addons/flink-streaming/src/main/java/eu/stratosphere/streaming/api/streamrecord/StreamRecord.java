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

import java.io.Serializable;
import java.util.Arrays;

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
public abstract class StreamRecord implements IOReadableWritable, Serializable {
	private static final long serialVersionUID = 1L;

	protected UID uid = new UID();
	protected int batchSize;

	protected SerializationDelegate<Tuple> serializationDelegate;
	protected DeserializationDelegate<Tuple> deserializationDelegate;
	protected TupleSerializer<Tuple> tupleSerializer;

	private static final Class<?>[] CLASSES = new Class<?>[] { Tuple1.class, Tuple2.class,
			Tuple3.class, Tuple4.class, Tuple5.class, Tuple6.class, Tuple7.class, Tuple8.class,
			Tuple9.class, Tuple10.class, Tuple11.class, Tuple12.class, Tuple13.class,
			Tuple14.class, Tuple15.class, Tuple16.class, Tuple17.class, Tuple18.class,
			Tuple19.class, Tuple20.class, Tuple21.class, Tuple22.class };

	public void setSeralizationDelegate(SerializationDelegate<Tuple> serializationDelegate) {
		this.serializationDelegate = serializationDelegate;
	}

	public void setDeseralizationDelegate(DeserializationDelegate<Tuple> deserializationDelegate,
			TupleSerializer<Tuple> tupleSerializer) {
		this.deserializationDelegate = deserializationDelegate;
		this.tupleSerializer = tupleSerializer;
	}

	/**
	 * @return Number of tuples in the batch
	 */
	public int getBatchSize() {
		return batchSize;
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
	 * Returns an iterable over the tuplebatch
	 * 
	 * @return batch iterable
	 */
	public abstract Iterable<Tuple> getBatchIterable();

	/**
	 * @param tupleNumber
	 *            Position of the record in the batch
	 * @return Chosen tuple
	 * @throws NoSuchTupleException
	 *             the Tuple does not have this many fields
	 */
	public abstract Tuple getTuple(int tupleNumber) throws NoSuchTupleException;

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
	public abstract void setTuple(int tupleNumber, Tuple tuple) throws NoSuchTupleException;

	/**
	 * Creates a deep copy of the StreamRecord
	 * 
	 * @return Copy of the StreamRecord
	 * 
	 */
	public abstract StreamRecord copy();

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

}
