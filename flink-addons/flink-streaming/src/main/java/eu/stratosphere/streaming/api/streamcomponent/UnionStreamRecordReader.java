/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.streaming.api.streamcomponent;

import java.io.IOException;

import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.api.java.typeutils.runtime.TupleSerializer;
import eu.stratosphere.nephele.io.AbstractUnionRecordReader;
import eu.stratosphere.nephele.io.MutableRecordReader;
import eu.stratosphere.nephele.io.Reader;
import eu.stratosphere.pact.runtime.plugable.DeserializationDelegate;
import eu.stratosphere.streaming.api.streamrecord.ArrayStreamRecord;

public final class UnionStreamRecordReader extends AbstractUnionRecordReader<ArrayStreamRecord>
		implements Reader<ArrayStreamRecord> {

	private final Class<ArrayStreamRecord> recordType;

	private ArrayStreamRecord lookahead;
	private DeserializationDelegate<Tuple> deserializationDelegate;
	private TupleSerializer<Tuple> tupleSerializer;

	public UnionStreamRecordReader(MutableRecordReader<ArrayStreamRecord>[] recordReaders,
			DeserializationDelegate<Tuple> deserializationDelegate,
			TupleSerializer<Tuple> tupleSerializer) {
		super(recordReaders);
		this.recordType = ArrayStreamRecord.class;
		this.deserializationDelegate = deserializationDelegate;
		this.tupleSerializer = tupleSerializer;
	}

	@Override
	public boolean hasNext() throws IOException, InterruptedException {
		if (this.lookahead != null) {
			return true;
		} else {
			ArrayStreamRecord record = instantiateRecordType();
			record.setDeseralizationDelegate(deserializationDelegate, tupleSerializer);
			if (getNextRecord(record)) {
				this.lookahead = record;
				return true;
			} else {
				return false;
			}
		}
	}

	@Override
	public ArrayStreamRecord next() throws IOException, InterruptedException {
		if (hasNext()) {
			ArrayStreamRecord tmp = this.lookahead;
			this.lookahead = null;
			return tmp;
		} else {
			return null;
		}
	}

	private ArrayStreamRecord instantiateRecordType() {
		try {
			return this.recordType.newInstance();
		} catch (InstantiationException e) {
			throw new RuntimeException("Cannot instantiate class '" + this.recordType.getName()
					+ "'.", e);
		} catch (IllegalAccessException e) {
			throw new RuntimeException("Cannot instantiate class '" + this.recordType.getName()
					+ "'.", e);
		}
	}
}
