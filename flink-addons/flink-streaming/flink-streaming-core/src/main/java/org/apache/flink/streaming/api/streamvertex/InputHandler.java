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
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.io.network.api.MutableReader;
import org.apache.flink.runtime.io.network.api.MutableRecordReader;
import org.apache.flink.runtime.io.network.api.MutableUnionRecordReader;
import org.apache.flink.runtime.operators.util.ReaderIterator;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.streaming.api.StreamConfig;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.streaming.api.streamrecord.StreamRecordSerializer;
import org.apache.flink.util.MutableObjectIterator;

public class InputHandler<IN> {
	private StreamRecordSerializer<IN> inputSerializer = null;
	private MutableObjectIterator<StreamRecord<IN>> inputIter;
	private MutableReader<IOReadableWritable> inputs;

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

	@SuppressWarnings("unchecked")
	protected void setConfigInputs() throws StreamVertexException {
		inputSerializer = configuration.getTypeSerializerIn1(streamVertex.userClassLoader);

		int numberOfInputs = configuration.getNumberOfInputs();
		if (numberOfInputs > 0) {

			if (numberOfInputs < 2) {

				inputs = new MutableRecordReader<IOReadableWritable>(streamVertex);

			} else {
				MutableRecordReader<IOReadableWritable>[] recordReaders = (MutableRecordReader<IOReadableWritable>[]) new MutableRecordReader<?>[numberOfInputs];

				for (int i = 0; i < numberOfInputs; i++) {
					recordReaders[i] = new MutableRecordReader<IOReadableWritable>(streamVertex);
				}
				inputs = new MutableUnionRecordReader<IOReadableWritable>(recordReaders);
			}

			inputIter = createInputIterator();
		}
	}

	private MutableObjectIterator<StreamRecord<IN>> createInputIterator() {
		@SuppressWarnings({ "unchecked", "rawtypes" })
		final MutableObjectIterator<StreamRecord<IN>> iter = new ReaderIterator(inputs,
				inputSerializer);
		return iter;
	}

	protected static <T> MutableObjectIterator<StreamRecord<T>> staticCreateInputIterator(
			MutableReader<?> inputReader, TypeSerializer<?> serializer) {

		// generic data type serialization
		@SuppressWarnings("unchecked")
		MutableReader<DeserializationDelegate<?>> reader = (MutableReader<DeserializationDelegate<?>>) inputReader;
		@SuppressWarnings({ "unchecked", "rawtypes" })
		final MutableObjectIterator<StreamRecord<T>> iter = new ReaderIterator(reader, serializer);
		return iter;
	}

	public StreamRecordSerializer<IN> getInputSerializer() {
		return inputSerializer;
	}

	public MutableObjectIterator<StreamRecord<IN>> getInputIter() {
		return inputIter;
	}
}
