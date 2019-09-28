/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.api.reader;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;

import java.io.IOException;

/**
 * Record oriented reader for immutable types.
 *
 * @param <T> Thy type of the records that is read.
 */
public class RecordReader<T extends IOReadableWritable> extends AbstractRecordReader<T> implements Reader<T> {

	private final Class<T> recordType;

	private T currentRecord;

	/**
	 * Creates a new RecordReader that de-serializes records from the given input gate and
	 * can spill partial records to disk, if they grow large.
	 *
	 * @param inputGate The input gate to read from.
	 * @param tmpDirectories The temp directories. USed for spilling if the reader concurrently
	 *                       reconstructs multiple large records.
	 */
	public RecordReader(InputGate inputGate, Class<T> recordType, String[] tmpDirectories) {
		super(inputGate, tmpDirectories);

		this.recordType = recordType;
	}

	@Override
	public boolean hasNext() throws IOException, InterruptedException {
		if (currentRecord != null) {
			return true;
		}
		else {
			T record = instantiateRecordType();
			if (getNextRecord(record)) {
				currentRecord = record;
				return true;
			}
			else {
				return false;
			}
		}
	}

	@Override
	public T next() throws IOException, InterruptedException {
		if (hasNext()) {
			T tmp = currentRecord;
			currentRecord = null;
			return tmp;
		}
		else {
			return null;
		}
	}

	@Override
	public void clearBuffers() {
		super.clearBuffers();
	}

	private T instantiateRecordType() {
		try {
			return recordType.newInstance();
		}
		catch (InstantiationException | IllegalAccessException e) {
			throw new RuntimeException("Cannot instantiate class " + recordType.getName(), e);
		}
	}
}
