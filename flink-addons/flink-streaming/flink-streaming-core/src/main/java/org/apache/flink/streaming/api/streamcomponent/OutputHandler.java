/**
 *
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
 *
 */

package org.apache.flink.streaming.api.streamcomponent;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.runtime.io.network.api.RecordWriter;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.StreamConfig;
import org.apache.flink.streaming.api.collector.DirectedStreamCollector;
import org.apache.flink.streaming.api.collector.OutputSelector;
import org.apache.flink.streaming.api.collector.StreamCollector;
import org.apache.flink.streaming.api.invokable.StreamComponentInvokable;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.streaming.api.streamrecord.StreamRecordSerializer;
import org.apache.flink.streaming.partitioner.StreamPartitioner;
import org.apache.flink.types.TypeInformation;

public class OutputHandler<OUT> {
	private static final Log LOG = LogFactory.getLog(OutputHandler.class);

	private AbstractStreamComponent streamComponent;
	private StreamConfig configuration;

	private List<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>> outputs;
	private StreamCollector<OUT> collector;
	private long bufferTimeout;

	TypeInformation<OUT> outTypeInfo = null;
	StreamRecordSerializer<OUT> outSerializer = null;
	SerializationDelegate<StreamRecord<OUT>> outSerializationDelegate = null;

	public OutputHandler(AbstractStreamComponent streamComponent) {
		this.streamComponent = streamComponent;
		this.outputs = new LinkedList<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>>();
		this.configuration = new StreamConfig(streamComponent.getTaskConfiguration());

		try {
			setConfigOutputs();
		} catch (StreamComponentException e) {
			throw new StreamComponentException("Cannot register outputs for "
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
		if (streamComponent.configuration.getDirectedEmit()) {
			OutputSelector<OUT> outputSelector = streamComponent.configuration.getOutputSelector();

			collector = new DirectedStreamCollector<OUT>(streamComponent.getInstanceID(),
					outSerializationDelegate, outputSelector);
		} else {
			collector = new StreamCollector<OUT>(streamComponent.getInstanceID(),
					outSerializationDelegate);
		}
		return collector;
	}

	public StreamCollector<OUT> getCollector() {
		return collector;
	}

	void setSerializers() {
		outTypeInfo = configuration.getTypeInfoOut1();
		outSerializer = new StreamRecordSerializer<OUT>(outTypeInfo);
		outSerializationDelegate = new SerializationDelegate<StreamRecord<OUT>>(outSerializer);
		outSerializationDelegate.setInstance(outSerializer.createInstance());
	}

	void setPartitioner(int outputNumber,
			List<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>> outputs) {
		StreamPartitioner<OUT> outputPartitioner = null;

		try {
			outputPartitioner = configuration.getPartitioner(outputNumber);

		} catch (Exception e) {
			throw new StreamComponentException("Cannot deserialize partitioner for "
					+ streamComponent.getName() + " with " + outputNumber + " outputs", e);
		}

		RecordWriter<SerializationDelegate<StreamRecord<OUT>>> output;

		if (bufferTimeout > 0) {
			output = new StreamRecordWriter<SerializationDelegate<StreamRecord<OUT>>>(
					streamComponent, outputPartitioner, bufferTimeout);
		} else {
			output = new RecordWriter<SerializationDelegate<StreamRecord<OUT>>>(streamComponent,
					outputPartitioner);
		}

		outputs.add(output);
		List<String> outputName = configuration.getOutputName(outputNumber);

		if (collector != null) {
			collector.addOutput(output, outputName);
		}

		if (LOG.isTraceEnabled()) {
			LOG.trace("Partitioner set: " + outputPartitioner.getClass().getSimpleName() + " with "
					+ outputNumber + " outputs");
		}
	}

	public void flushOutputs() throws IOException, InterruptedException {
		for (RecordWriter<SerializationDelegate<StreamRecord<OUT>>> output : outputs) {
			output.flush();
		}
	}

	public void initializeOutputSerializers() {
		for (RecordWriter<SerializationDelegate<StreamRecord<OUT>>> output : outputs) {
			output.initializeSerializers();
		}
	}

	long startTime;

	public void invokeUserFunction(String componentTypeName,
			StreamComponentInvokable<OUT> userInvokable) throws IOException, InterruptedException {
		if (LOG.isDebugEnabled()) {
			LOG.debug(componentTypeName + " " + streamComponent.getName()
					+ " invoked with instance id " + streamComponent.getInstanceID());
		}

		initializeOutputSerializers();

		try {
			streamComponent.invokeUserFunction(userInvokable);
		} catch (Exception e) {
			flushOutputs();
			throw new RuntimeException(e);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug(componentTypeName + " " + streamComponent.getName()
					+ " invoke finished with instance id " + streamComponent.getInstanceID());
		}

		flushOutputs();
	}
}
