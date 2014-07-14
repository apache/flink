/***********************************************************************************************************************
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **********************************************************************************************************************/

package org.apache.flink.streaming.api.streamrecord;

import java.io.IOException;
import java.util.Arrays;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

/**
 * Object for storing serializable records in batch (single records are
 * represented batches with one element) used for sending records between task
 * objects in Stratosphere stream processing. The elements of the batch are
 * Tuples.
 */
public class ArrayStreamRecord extends StreamRecord {
	private static final long serialVersionUID = 1L;

	private Tuple[] tupleBatch;

	/**
	 * Creates a new empty instance for read
	 */
	public ArrayStreamRecord() {
	}

	public ArrayStreamRecord(int batchsize) {
		this.batchSize = batchsize;
		tupleBatch = new Tuple[batchsize];
	}

	public ArrayStreamRecord(StreamRecord record) {
		this(record, record.getBatchSize());
	}

	public ArrayStreamRecord(StreamRecord record, int truncatedSize) {
		tupleBatch = new Tuple[truncatedSize];
		this.uid = new UID(Arrays.copyOf(record.getId().getId(), 20));
		for (int i = 0; i < truncatedSize; ++i) {
			this.tupleBatch[i] = copyTuple(record.getTuple(i));
		}
		this.batchSize = tupleBatch.length;
	}

	/**
	 * Creates a new batch of records containing the given Tuple array as
	 * elements
	 * 
	 * @param tupleArray
	 *            Tuples to bes stored in the StreamRecord
	 */
	public ArrayStreamRecord(Tuple[] tupleArray) {
		this.batchSize = tupleArray.length;
		tupleBatch = tupleArray;
	}

	/**
	 * Returns an iterable over the tuplebatch
	 * 
	 * @return batch iterable
	 */
	public Iterable<Tuple> getBatchIterable() {
		return (Iterable<Tuple>) Arrays.asList(tupleBatch);
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
			return tupleBatch[tupleNumber];
		} catch (IndexOutOfBoundsException e) {
			throw (new NoSuchTupleException());
		}
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
	 * @return Returns the StreamRecord object
	 */
	public StreamRecord setTuple(int tupleNumber, Tuple tuple) throws NoSuchTupleException {
		try {
			tupleBatch[tupleNumber] = tuple;
		} catch (IndexOutOfBoundsException e) {
			throw (new NoSuchTupleException());
		}
		return this;
	}

	/**
	 * Creates a deep copy of the StreamRecord
	 * 
	 * @return Copy of the StreamRecord
	 * 
	 */
	public ArrayStreamRecord copy() {
		ArrayStreamRecord newRecord = new ArrayStreamRecord(batchSize);

		newRecord.uid = new UID(Arrays.copyOf(uid.getId(), 20));

		for (int i = 0; i < batchSize; i++) {
			newRecord.tupleBatch[i] = copyTuple(tupleBatch[i]);
		}

		return newRecord;
	}

	@Override
	public void write(DataOutputView out) throws IOException {

		uid.write(out);
		out.writeInt(batchSize);

		for (Tuple tuple : tupleBatch) {
			serializationDelegate.setInstance(tuple);
			serializationDelegate.write(out);
		}
	}

	@Override
	public void read(DataInputView in) throws IOException {
		uid = new UID();
		uid.read(in);
		batchSize = in.readInt();
		tupleBatch = new Tuple[batchSize];

		for (int k = 0; k < batchSize; ++k) {
			deserializationDelegate.setInstance(tupleSerializer.createInstance());
			deserializationDelegate.read(in);
			tupleBatch[k] = deserializationDelegate.getInstance();
		}
	}

	/**
	 * Creates a String representation as a list of tuples
	 */
	public String toString() {
		StringBuilder outputString = new StringBuilder("[");

		String prefix = "";
		if(tupleBatch!=null){
			for (Tuple tuple : tupleBatch) {
				outputString.append(prefix);
				prefix = ",";
				outputString.append(tuple.toString());
			}
		}
		outputString.append("]");
		return outputString.toString();
	}

}
