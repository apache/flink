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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import eu.stratosphere.api.java.tuple.Tuple;

/**
 * Object for storing serializable records in batch (single records are
 * represented batches with one element) used for sending records between task
 * objects in Stratosphere stream processing. The elements of the batch are
 * Tuples.
 */
public class ListStreamRecord extends StreamRecord {
	private static final long serialVersionUID = 1L;

	private List<Tuple> tupleBatch;

	/**
	 * Creates a new empty instance for read
	 */
	public ListStreamRecord() {
	}

	public ListStreamRecord(int batchsize) {
		this.batchSize = batchsize;
		tupleBatch = new ArrayList<Tuple>(batchsize);
		initRecords();
	}

	public ListStreamRecord(StreamRecord record) {
		tupleBatch = new ArrayList<Tuple>();
		this.uid = new UID(Arrays.copyOf(record.getId().getId(), 20));
		for (int i = 0; i < record.getBatchSize(); ++i) {
			this.tupleBatch.add(copyTuple(record.getTuple(i)));
		}
		this.batchSize = tupleBatch.size();
	}

	/**
	 * Creates a new batch of records containing the given Tuple list as
	 * elements
	 * 
	 * @param tupleList
	 *            Tuples to bes stored in the StreamRecord
	 */
	public ListStreamRecord(List<Tuple> tupleList) {
		this.batchSize = tupleList.size();
		tupleBatch = new ArrayList<Tuple>(tupleList);
	}

	/**
	 * Initializes the record batch elemnts to null
	 */
	public void initRecords() {
		tupleBatch.clear();
		for (int i = 0; i < batchSize; i++) {
			tupleBatch.add(null);
		}
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
	 * Sets a tuple at the given position in the batch with the given tuple
	 * 
	 * @param tupleNumber
	 *            Position of tuple in the batch
	 * @param tuple
	 *            Value to set
	 * @throws NoSuchTupleException
	 *             , TupleSizeMismatchException
	 */
	public void setTuple(int tupleNumber, Tuple tuple) throws NoSuchTupleException {
		try {
			tupleBatch.set(tupleNumber, tuple);
		} catch (IndexOutOfBoundsException e) {
			throw (new NoSuchTupleException());
		}

	}

	/**
	 * Creates a deep copy of the StreamRecord
	 * 
	 * @return Copy of the StreamRecord
	 * 
	 */
	public ListStreamRecord copy() {
		ListStreamRecord newRecord = new ListStreamRecord(batchSize);

		newRecord.uid = new UID(Arrays.copyOf(uid.getId(), 20));

		for (Tuple tuple : tupleBatch) {
			newRecord.tupleBatch.add(copyTuple(tuple));
		}

		return newRecord;
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

	@Override
	public void write(DataOutput out) throws IOException {

		uid.write(out);
		out.writeInt(batchSize);

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
		batchSize = in.readInt();
		tupleBatch = new ArrayList<Tuple>(batchSize);

		for (int k = 0; k < batchSize; ++k) {
			deserializationDelegate.setInstance(tupleSerializer.createInstance());
			deserializationDelegate.read(in);
			tupleBatch.set(k, deserializationDelegate.getInstance());
		}
	}

}
