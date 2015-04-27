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

package org.apache.flink.streaming.runtime.tasks;

import java.io.IOException;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.io.network.api.reader.MutableReader;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.runtime.io.IndexedMutableReader;
import org.apache.flink.streaming.runtime.io.IndexedReaderIterator;
import org.apache.flink.streaming.runtime.io.InputGateFactory;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecordSerializer;

public class InputHandler<IN> {
	private StreamRecordSerializer<IN> inputSerializer = null;
	private IndexedReaderIterator<StreamRecord<IN>> inputIter;
	private IndexedMutableReader<DeserializationDelegate<StreamRecord<IN>>> inputs;

	private StreamTask<IN, ?> streamVertex;
	private StreamConfig configuration;

	public InputHandler(StreamTask<IN, ?> streamComponent) {
		this.streamVertex = streamComponent;
		this.configuration = new StreamConfig(streamComponent.getTaskConfiguration());
		try {
			setConfigInputs();
		} catch (Exception e) {
			throw new StreamTaskException("Cannot register inputs for "
					+ getClass().getSimpleName(), e);
		}

	}

	protected void setConfigInputs() throws StreamTaskException {
		inputSerializer = configuration.getTypeSerializerIn1(streamVertex.userClassLoader);

		int numberOfInputs = configuration.getNumberOfInputs();

		if (numberOfInputs > 0) {
			InputGate inputGate = InputGateFactory.createInputGate(streamVertex.getEnvironment().getAllInputGates());
			inputs = new IndexedMutableReader<DeserializationDelegate<StreamRecord<IN>>>(inputGate);

			inputs.registerTaskEventListener(streamVertex.getSuperstepListener(),
					StreamingSuperstep.class);

			inputIter = new IndexedReaderIterator<StreamRecord<IN>>(inputs, inputSerializer);
		}
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

	public void clearReaders() throws IOException {
		if (inputs != null) {
			inputs.clearBuffers();
			inputs.cleanup();
		}
	}
}
