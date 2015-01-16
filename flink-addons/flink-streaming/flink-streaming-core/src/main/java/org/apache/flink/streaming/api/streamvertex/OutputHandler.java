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

	private StreamVertex<?, OUT> vertex;
	private StreamConfig configuration;

	private List<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>> outputs;
	private Collector<OUT> outerCollector;

	TypeInformation<OUT> outTypeInfo = null;
	StreamRecordSerializer<OUT> outSerializer = null;
	SerializationDelegate<StreamRecord<OUT>> outSerializationDelegate = null;

	public List<ChainableInvokable<?, ?>> chainedInvokables;

	private int numberOfChainedTasks;

	public OutputHandler(StreamVertex<?, OUT> vertex) {
		this.vertex = vertex;
		this.outputs = new LinkedList<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>>();
		this.configuration = new StreamConfig(vertex.getTaskConfiguration());
		this.chainedInvokables = new ArrayList<ChainableInvokable<?, ?>>();
		this.numberOfChainedTasks = configuration.getNumberofChainedTasks();

		this.outerCollector = createChainedCollector(0);

	}

	public List<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>> getOutputs() {
		return outputs;
	}

	// We create the outer collector by nesting the chainable invokables into
	// each other
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private Collector<OUT> createChainedCollector(int chainedTaskIndex) {

		if (numberOfChainedTasks == chainedTaskIndex) {
			// At the end of the chain we create the collector that sends data
			// to the recordwriters
			return createNetworkCollector();
		} else {

			ChainableInvokable chainableInvokable = configuration.getChainedInvokable(
					chainedTaskIndex, vertex.getUserCodeClassLoader());

			// The nesting is done by calling this method recursively when
			// passing the collector to the invokable
			chainableInvokable.setup(
					createChainedCollector(chainedTaskIndex + 1),
					configuration.getChainedInSerializer(chainedTaskIndex,
							vertex.getUserCodeClassLoader()));

			// We hold a list of the chained invokables for initializaton
			// afterwards
			chainedInvokables.add(chainableInvokable);

			return chainableInvokable;
		}

	}

	private Collector<OUT> createNetworkCollector() {

		createOutSerializer();

		StreamOutputWrapper<OUT> collector;

		if (vertex.configuration.isDirectedEmit()) {
			OutputSelector<OUT> outputSelector = vertex.configuration
					.getOutputSelector(vertex.userClassLoader);

			collector = new DirectedOutputWrapper<OUT>(vertex.getInstanceID(),
					outSerializationDelegate, outputSelector);
		} else {
			collector = new StreamOutputWrapper<OUT>(vertex.getInstanceID(),
					outSerializationDelegate);
		}

		int numberOfOutputs = configuration.getNumberOfOutputs();
		for (int i = 0; i < numberOfOutputs; i++) {
			collector = (StreamOutputWrapper<OUT>) addStreamOutput(i, collector);
		}

		return collector;
	}

	public Collector<OUT> getCollector() {
		return outerCollector;
	}

	void createOutSerializer() {
		outSerializer = configuration.getTypeSerializerOut1(vertex.userClassLoader);
		if (outSerializer != null) {
			outSerializationDelegate = new SerializationDelegate<StreamRecord<OUT>>(outSerializer);
			outSerializationDelegate.setInstance(outSerializer.createInstance());
		}
	}

	Collector<OUT> addStreamOutput(int outputNumber, StreamOutputWrapper<OUT> networkCollector) {

		StreamPartitioner<OUT> outputPartitioner;

		try {
			outputPartitioner = configuration.getPartitioner(vertex.userClassLoader,
					outputNumber);
		} catch (Exception e) {
			throw new StreamVertexException("Cannot deserialize partitioner for "
					+ vertex.getName() + " with " + outputNumber + " outputs", e);
		}

		RecordWriter<SerializationDelegate<StreamRecord<OUT>>> output;

		long bufferTimeout = configuration.getBufferTimeout();

		if (bufferTimeout >= 0) {
			output = new StreamRecordWriter<SerializationDelegate<StreamRecord<OUT>>>(vertex
					.getEnvironment().getWriter(outputNumber), outputPartitioner, bufferTimeout);

			if (LOG.isTraceEnabled()) {
				LOG.trace("StreamRecordWriter initiated with {} bufferTimeout for {}",
						bufferTimeout, vertex.getClass().getSimpleName());
			}
		} else {
			output = new RecordWriter<SerializationDelegate<StreamRecord<OUT>>>(vertex
					.getEnvironment().getWriter(outputNumber), outputPartitioner);

			if (LOG.isTraceEnabled()) {
				LOG.trace("RecordWriter initiated for {}", vertex.getClass().getSimpleName());
			}
		}

		outputs.add(output);

		networkCollector.addOutput(new StreamOutput<OUT>(output, configuration
				.isSelectAll(outputNumber) ? null : configuration.getOutputNames(outputNumber)));

		if (LOG.isTraceEnabled()) {
			LOG.trace("Partitioner set: {} with {} outputs for {}", outputPartitioner.getClass()
					.getSimpleName(), outputNumber, vertex.getClass().getSimpleName());
		}

		return networkCollector;
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

	public void invokeUserFunction(String componentTypeName, StreamInvokable<?, OUT> userInvokable)
			throws IOException, InterruptedException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("{} {} invoked with instance id {}", componentTypeName,
					vertex.getName(), vertex.getInstanceID());
		}

		try {
			vertex.invokeUserFunction(userInvokable);
		} catch (Exception e) {
			flushOutputs();
			throw new RuntimeException(e);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("{} {} invoke finished instance id {}", componentTypeName,
					vertex.getName(), vertex.getInstanceID());
		}

		flushOutputs();
	}
}
