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

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.io.network.api.RecordWriter;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.StreamConfig;
import org.apache.flink.streaming.api.collector.DirectedStreamCollector;
import org.apache.flink.streaming.api.collector.OutputSelector;
import org.apache.flink.streaming.api.collector.StreamCollector;
import org.apache.flink.streaming.api.invokable.StreamInvokable;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.streaming.api.streamrecord.StreamRecordSerializer;
import org.apache.flink.streaming.io.StreamRecordWriter;
import org.apache.flink.streaming.partitioner.StreamPartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OutputHandler<OUT> {
	private static final Logger LOG = LoggerFactory.getLogger(OutputHandler.class);

	private StreamVertex<?, OUT> streamVertex;
	private StreamConfig configuration;

	private List<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>> outputs;
	private StreamCollector<OUT> collector;
	private long bufferTimeout;

	TypeInformation<OUT> outTypeInfo = null;
	StreamRecordSerializer<OUT> outSerializer = null;
	SerializationDelegate<StreamRecord<OUT>> outSerializationDelegate = null;

	public OutputHandler(StreamVertex<?, OUT> streamComponent) {
		this.streamVertex = streamComponent;
		this.outputs = new LinkedList<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>>();
		this.configuration = new StreamConfig(streamComponent.getTaskConfiguration());

		try {
			setConfigOutputs();
		} catch (StreamVertexException e) {
			throw new StreamVertexException("Cannot register outputs for "
					+ streamComponent.getClass().getSimpleName(), e);
		}
	}

	public List<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>> getOutputs() {
		return outputs;
	}

	private void setConfigOutputs() {
		setSerializers();
		setCollector();

		int numberOfOutputs = configuration.getNumberOfOutputs();
		bufferTimeout = configuration.getBufferTimeout();

		for (int i = 0; i < numberOfOutputs; i++) {
			setPartitioner(i, outputs);
		}
	}

	private StreamCollector<OUT> setCollector() {
		if (streamVertex.configuration.getDirectedEmit()) {
			OutputSelector<OUT> outputSelector = streamVertex.configuration
					.getOutputSelector(streamVertex.userClassLoader);

			collector = new DirectedStreamCollector<OUT>(streamVertex.getInstanceID(),
					outSerializationDelegate, outputSelector);
		} else {
			collector = new StreamCollector<OUT>(streamVertex.getInstanceID(),
					outSerializationDelegate);
		}
		return collector;
	}

	public StreamCollector<OUT> getCollector() {
		return collector;
	}

	void setSerializers() {
		outSerializer = configuration.getTypeSerializerOut1(streamVertex.userClassLoader);
		if (outSerializer != null) {
			outSerializationDelegate = new SerializationDelegate<StreamRecord<OUT>>(outSerializer);
			outSerializationDelegate.setInstance(outSerializer.createInstance());
		}
	}

	void setPartitioner(int outputNumber,
			List<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>> outputs) {
		StreamPartitioner<OUT> outputPartitioner = null;

		try {
			outputPartitioner = configuration.getPartitioner(streamVertex.userClassLoader,
					outputNumber);

		} catch (Exception e) {
			throw new StreamVertexException("Cannot deserialize partitioner for "
					+ streamVertex.getName() + " with " + outputNumber + " outputs", e);
		}

		RecordWriter<SerializationDelegate<StreamRecord<OUT>>> output;

		if (bufferTimeout >= 0) {
			output = new StreamRecordWriter<SerializationDelegate<StreamRecord<OUT>>>(streamVertex,
					outputPartitioner, bufferTimeout);

			if (LOG.isTraceEnabled()) {
				LOG.trace("StreamRecordWriter initiated with {} bufferTimeout for {}",
						bufferTimeout, streamVertex.getClass().getSimpleName());
			}
		} else {
			output = new RecordWriter<SerializationDelegate<StreamRecord<OUT>>>(streamVertex,
					outputPartitioner);

			if (LOG.isTraceEnabled()) {
				LOG.trace("RecordWriter initiated for {}", streamVertex.getClass().getSimpleName());
			}
		}

		outputs.add(output);
		List<String> outputName = configuration.getOutputName(outputNumber);
		boolean isSelectAllOutput = configuration.getSelectAll(outputNumber);

		if (collector != null) {
			collector.addOutput(output, outputName, isSelectAllOutput);
		}

		if (LOG.isTraceEnabled()) {
			LOG.trace("Partitioner set: {} with {} outputs for {}", outputPartitioner.getClass()
					.getSimpleName(), outputNumber, streamVertex.getClass().getSimpleName());
		}
	}

	public void flushOutputs() throws IOException, InterruptedException {
		for (RecordWriter<SerializationDelegate<StreamRecord<OUT>>> output : outputs) {
			if (output instanceof StreamRecordWriter) {
				((StreamRecordWriter<SerializationDelegate<StreamRecord<OUT>>>) output).close();
			} else {
				output.flush();
			}
		}
	}

	public void initializeOutputSerializers() {
		for (RecordWriter<SerializationDelegate<StreamRecord<OUT>>> output : outputs) {
			output.initializeSerializers();
		}
	}

	long startTime;

	public void invokeUserFunction(String componentTypeName, StreamInvokable<?, OUT> userInvokable)
			throws IOException, InterruptedException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("{} {} invoked with instance id {}", componentTypeName,
					streamVertex.getName(), streamVertex.getInstanceID());
		}

		initializeOutputSerializers();

		try {
			streamVertex.invokeUserFunction(userInvokable);
		} catch (Exception e) {
			flushOutputs();
			throw new RuntimeException(e);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("{} {} invoke finished instance id {}", componentTypeName,
					streamVertex.getName(), streamVertex.getInstanceID());
		}

		flushOutputs();
	}
}
