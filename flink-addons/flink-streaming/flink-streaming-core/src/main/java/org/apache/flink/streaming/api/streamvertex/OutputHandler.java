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
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.StreamConfig;
import org.apache.flink.streaming.api.collector.CollectorWrapper;
import org.apache.flink.streaming.api.collector.DirectedOutputWrapper;
import org.apache.flink.streaming.api.collector.OutputSelector;
import org.apache.flink.streaming.api.collector.StreamOutput;
import org.apache.flink.streaming.api.collector.StreamOutputWrapper;
import org.apache.flink.streaming.api.invokable.ChainableInvokable;
import org.apache.flink.streaming.api.invokable.StreamInvokable;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.streaming.api.streamrecord.StreamRecordSerializer;
import org.apache.flink.streaming.io.StreamRecordWriter;
import org.apache.flink.streaming.partitioner.StreamPartitioner;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OutputHandler<OUT> {
	private static final Logger LOG = LoggerFactory.getLogger(OutputHandler.class);

	private StreamVertex<?, OUT> streamVertex;
	private StreamConfig configuration;

	private List<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>> outputs;
	private Collector<OUT> endCollector;

	TypeInformation<OUT> outTypeInfo = null;
	StreamRecordSerializer<OUT> outSerializer = null;
	SerializationDelegate<StreamRecord<OUT>> outSerializationDelegate = null;

	public List<ChainableInvokable<?, ?>> chainedInvokables;

	private int numberOfChainedTasks;

	public OutputHandler(StreamVertex<?, OUT> streamComponent) {
		this.streamVertex = streamComponent;
		this.outputs = new LinkedList<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>>();
		this.configuration = new StreamConfig(streamComponent.getTaskConfiguration());
		this.chainedInvokables = new ArrayList<ChainableInvokable<?, ?>>();

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
		numberOfChainedTasks = configuration.getNumberofChainedTasks();
		endCollector = createChainedOutputs(0);
	}

	@SuppressWarnings("unchecked")
	private Collector<OUT> createChainedOutputs(int chainedTaskIndex) {

		if (numberOfChainedTasks == chainedTaskIndex) {
			return createEndCollector();
		} else {
			CollectorWrapper<OUT> chainedCollector = new CollectorWrapper<OUT>();

			@SuppressWarnings("rawtypes")
			ChainableInvokable chainableInvokable = configuration.getChainedInvokable(
					chainedTaskIndex, streamVertex.getUserCodeClassLoader());

			chainableInvokable.setup(
					createChainedOutputs(chainedTaskIndex + 1),
					configuration.getChainedInSerializer(chainedTaskIndex,
							streamVertex.getUserCodeClassLoader()));

			chainedInvokables.add(chainableInvokable);

			chainedCollector.addChainedOutput((Collector<OUT>) chainableInvokable);

			return chainedCollector;
		}

	}

	private Collector<OUT> createEndCollector() {

		setSerializers();

		StreamOutputWrapper<OUT> collector;

		if (streamVertex.configuration.getDirectedEmit()) {
			OutputSelector<OUT> outputSelector = streamVertex.configuration
					.getOutputSelector(streamVertex.userClassLoader);

			collector = new DirectedOutputWrapper<OUT>(streamVertex.getInstanceID(),
					outSerializationDelegate, outputSelector);
		} else {
			collector = new StreamOutputWrapper<OUT>(streamVertex.getInstanceID(),
					outSerializationDelegate);
		}

		int numberOfOutputs = configuration.getNumberOfOutputs();
		for (int i = 0; i < numberOfOutputs; i++) {
			collector = (StreamOutputWrapper<OUT>) setPartitioner(i, collector);
		}

		return collector;
	}

	public Collector<OUT> getCollector() {
		return endCollector;
	}

	void setSerializers() {
		outSerializer = configuration.getTypeSerializerOut1(streamVertex.userClassLoader);
		if (outSerializer != null) {
			outSerializationDelegate = new SerializationDelegate<StreamRecord<OUT>>(outSerializer);
			outSerializationDelegate.setInstance(outSerializer.createInstance());
		}
	}

	Collector<OUT> setPartitioner(int outputNumber, StreamOutputWrapper<OUT> endCollector) {
		StreamPartitioner<OUT> outputPartitioner = null;

		try {
			outputPartitioner = configuration.getPartitioner(streamVertex.userClassLoader,
					outputNumber);

		} catch (Exception e) {
			throw new StreamVertexException("Cannot deserialize partitioner for "
					+ streamVertex.getName() + " with " + outputNumber + " outputs", e);
		}

		RecordWriter<SerializationDelegate<StreamRecord<OUT>>> output;

		long bufferTimeout = configuration.getBufferTimeout();

		if (bufferTimeout >= 0) {
			output = new StreamRecordWriter<SerializationDelegate<StreamRecord<OUT>>>(streamVertex
					.getEnvironment().getWriter(outputNumber), outputPartitioner, bufferTimeout);

			if (LOG.isTraceEnabled()) {
				LOG.trace("StreamRecordWriter initiated with {} bufferTimeout for {}",
						bufferTimeout, streamVertex.getClass().getSimpleName());
			}
		} else {
			output = new RecordWriter<SerializationDelegate<StreamRecord<OUT>>>(streamVertex
					.getEnvironment().getWriter(outputNumber), outputPartitioner);

			if (LOG.isTraceEnabled()) {
				LOG.trace("RecordWriter initiated for {}", streamVertex.getClass().getSimpleName());
			}
		}

		outputs.add(output);
		List<String> outputNames = configuration.getOutputNames(outputNumber);
		boolean isSelectAllOutput = configuration.getSelectAll(outputNumber);

		if (endCollector != null) {
			endCollector.addOutput(new StreamOutput<OUT>(output, isSelectAllOutput ? null
					: outputNames));
		}

		if (LOG.isTraceEnabled()) {
			LOG.trace("Partitioner set: {} with {} outputs for {}", outputPartitioner.getClass()
					.getSimpleName(), outputNumber, streamVertex.getClass().getSimpleName());
		}

		return endCollector;
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

	long startTime;

	public void invokeUserFunction(String componentTypeName, StreamInvokable<?, OUT> userInvokable)
			throws IOException, InterruptedException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("{} {} invoked with instance id {}", componentTypeName,
					streamVertex.getName(), streamVertex.getInstanceID());
		}

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
