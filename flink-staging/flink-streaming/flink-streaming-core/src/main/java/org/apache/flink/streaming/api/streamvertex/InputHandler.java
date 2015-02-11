/*
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
 */

package org.apache.flink.streaming.api.streamvertex;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.io.network.api.reader.MutableReader;
import org.apache.flink.runtime.io.network.api.reader.UnionBufferReader;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.streaming.api.StreamConfig;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.streaming.api.streamrecord.StreamRecordSerializer;
import org.apache.flink.streaming.io.IndexedMutableReader;
import org.apache.flink.streaming.io.IndexedReaderIterator;

public class InputHandler<IN> {
	private StreamRecordSerializer<IN> inputSerializer = null;
	private IndexedReaderIterator<StreamRecord<IN>> inputIter;
	private IndexedMutableReader<DeserializationDelegate<StreamRecord<IN>>> inputs;

	private StreamVertex<IN, ?> streamVertex;
	private StreamConfig configuration;

	public InputHandler(StreamVertex<IN, ?> streamComponent) {
		this.streamVertex = streamComponent;
		this.configuration = new StreamConfig(streamComponent.getTaskConfiguration());
		try {
			setConfigInputs();
		} catch (Exception e) {
			throw new StreamVertexException("Cannot register inputs for "
					+ getClass().getSimpleName(), e);
		}

	}

	protected void setConfigInputs() throws StreamVertexException {
		inputSerializer = configuration.getTypeSerializerIn1(streamVertex.userClassLoader);

		int numberOfInputs = configuration.getNumberOfInputs();
		if (numberOfInputs > 0) {

			if (numberOfInputs < 2) {

				inputs = new IndexedMutableReader<DeserializationDelegate<StreamRecord<IN>>>(
						streamVertex.getEnvironment().getReader(0));

			} else {
				UnionBufferReader reader = new UnionBufferReader(streamVertex.getEnvironment()
						.getAllReaders());
				inputs = new IndexedMutableReader<DeserializationDelegate<StreamRecord<IN>>>(reader);
			}

			inputIter = createInputIterator();
		}
	}

	private IndexedReaderIterator<StreamRecord<IN>> createInputIterator() {
		final IndexedReaderIterator<StreamRecord<IN>> iter = new IndexedReaderIterator<StreamRecord<IN>>(
				inputs, inputSerializer);
		return iter;
	}

	protected static <T> IndexedReaderIterator<StreamRecord<T>> staticCreateInputIterator(
			MutableReader<?> inputReader, TypeSerializer<StreamRecord<T>> serializer) {

		// generic data type serialization
		@SuppressWarnings("unchecked")
		IndexedMutableReader<DeserializationDelegate<StreamRecord<T>>> reader = (IndexedMutableReader<DeserializationDelegate<StreamRecord<T>>>) inputReader;
		final IndexedReaderIterator<StreamRecord<T>> iter = new IndexedReaderIterator<StreamRecord<T>>(
				reader, serializer);
		return iter;
	}

	public StreamRecordSerializer<IN> getInputSerializer() {
		return inputSerializer;
	}

	public IndexedReaderIterator<StreamRecord<IN>> getInputIter() {
		return inputIter;
	}
}
