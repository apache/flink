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

package org.apache.flink.runtime.operators.shipping;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * The OutputCollector collects records, and emits them to the  {@link RecordWriter}s.
 * The OutputCollector tracks to which writers a deep-copy must be given and which not.
 */
public class OutputCollector<T> implements Collector<T> {

	// list of writers
	private final RecordWriter<SerializationDelegate<T>>[] writers;

	private final SerializationDelegate<T> delegate;

	
	/**
	 * Initializes the output collector with a set of writers. 
	 * To specify for a writer that it must be fed with a deep-copy, set the bit in the copy flag bit mask to 1 that 
	 * corresponds to the position of the writer within the {@link List}.
	 * 
	 * @param writers List of all writers.
	 */
	@SuppressWarnings("unchecked")
	public OutputCollector(List<RecordWriter<SerializationDelegate<T>>> writers, TypeSerializer<T> serializer) {
		this.delegate = new SerializationDelegate<T>(serializer);
		this.writers = (RecordWriter<SerializationDelegate<T>>[]) writers.toArray(new RecordWriter[writers.size()]);
	}

	/**
	 * Collects a record and emits it to all writers.
	 */
	@Override
	public void collect(T record)  {
		if (record != null) {
			this.delegate.setInstance(record);
			try {
				for (RecordWriter<SerializationDelegate<T>> writer : writers) {
					writer.emit(this.delegate);
				}
			}
			catch (IOException e) {
				throw new RuntimeException("Emitting the record caused an I/O exception: " + e.getMessage(), e);
			}
			catch (InterruptedException e) {
				throw new RuntimeException("Emitting the record was interrupted: " + e.getMessage(), e);
			}
		}
		else {
			throw new NullPointerException("The system does not support records that are null."
								+ "Null values are only supported as fields inside other objects.");
		}
	}

	@Override
	public void close() {
		for (RecordWriter<?> writer : writers) {
			writer.clearBuffers();
			writer.flushAll();
		}
	}

	/**
	 * List of writers that are associated with this output collector
	 * @return list of writers
	 */
	@SuppressWarnings("unchecked")
	public List<RecordWriter<SerializationDelegate<T>>> getWriters() {
		return Collections.unmodifiableList(Arrays.asList(this.writers));
	}
}
