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

import org.apache.commons.lang.SerializationUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.io.network.api.MutableReader;
import org.apache.flink.runtime.io.network.api.RecordWriter;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.operators.util.ReaderIterator;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.StreamConfig;
import org.apache.flink.streaming.api.collector.DirectedStreamCollector;
import org.apache.flink.streaming.api.collector.OutputSelector;
import org.apache.flink.streaming.api.collector.StreamCollector;
import org.apache.flink.streaming.api.invokable.StreamComponentInvokable;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.streaming.api.streamrecord.StreamRecordSerializer;
import org.apache.flink.streaming.partitioner.StreamPartitioner;
import org.apache.flink.streaming.util.serialization.TypeSerializerWrapper;
import org.apache.flink.types.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;

public abstract class AbstractStreamComponent<OUT> extends AbstractInvokable {

	protected static final String SOURCE = "source";

	private static final Log LOG = LogFactory.getLog(AbstractStreamComponent.class);

	protected TypeInformation<OUT> outTypeInfo = null;
	protected StreamRecordSerializer<OUT> outSerializer = null;
	protected SerializationDelegate<StreamRecord<OUT>> outSerializationDelegate = null;
	protected OutputHandler outputHandler = createEmptyOutputHandler();
	
	protected StreamConfig configuration;
	protected TypeSerializerWrapper<?, ?, OUT> typeWrapper;
	protected StreamCollector<OUT> collector;
	protected int instanceID;
	protected String name;
	private static int numComponents = 0;
	protected boolean isMutable;
	protected Object function;
	protected String functionName;
	protected long bufferTimeout;

	protected static int newComponent() {
		numComponents++;
		return numComponents;
	}

	@Override
	public void registerInputOutput() {
		initialize();
		setInputsOutputs();
		setInvokable();
		setCollector();
	}

	protected class OutputHandler {
		private List<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>> outputs;
		
		public OutputHandler() {
			this.outputs = new LinkedList<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>>();
		}

		public List<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>> getOutputs() {
			return outputs;
		}
		
		public void setConfigOutputs() {
			setSerializers();
			setCollector();

			int numberOfOutputs = configuration.getNumberOfOutputs();
			bufferTimeout = configuration.getBufferTimeout();

			for (int i = 0; i < numberOfOutputs; i++) {
				setPartitioner(i, outputs);
			}
		}
		
		public void flushOutputs() throws IOException, InterruptedException {
			for (RecordWriter<SerializationDelegate<StreamRecord<OUT>>> output : outputs) {
				output.flush();
			}
		}
		
		public void initializeOutputSerializers() {
			for (RecordWriter<SerializationDelegate<StreamRecord<OUT>>> output : outputHandler.getOutputs()) {
				output.initializeSerializers();
			}
		}
		
		public void invokeUserFunction(String componentTypeName, StreamComponentInvokable<OUT> userInvokable) throws IOException, InterruptedException {
			if (LOG.isDebugEnabled()) {
				LOG.debug(componentTypeName + " " + name + " invoked with instance id "
						+ instanceID);
			}

			initializeOutputSerializers();

			try {
				userInvokable.open(getTaskConfiguration());
				userInvokable.invoke();
				userInvokable.close();
			} catch (Exception e) {
				flushOutputs();
				throw new RuntimeException(e);
			}

			if (LOG.isDebugEnabled()) {
				LOG.debug(componentTypeName + " " + name + " invoke finished with instance id "
						+ instanceID);
			}

			flushOutputs();
		}
	}
	
	private OutputHandler createEmptyOutputHandler() {
		return new OutputHandler();
	}
	
	protected void initialize() {
		this.configuration = new StreamConfig(getTaskConfiguration());
		this.name = configuration.getComponentName();
		this.isMutable = configuration.getMutability();
		this.functionName = configuration.getFunctionName();
		this.function = configuration.getFunction();
		this.typeWrapper = configuration.getTypeWrapper();
	}

	protected Collector<OUT> setCollector() {
		if (configuration.getDirectedEmit()) {
			OutputSelector<OUT> outputSelector = configuration.getOutputSelector();

			collector = new DirectedStreamCollector<OUT>(instanceID, outSerializationDelegate,
					outputSelector);
		} else {
			collector = new StreamCollector<OUT>(instanceID, outSerializationDelegate);
		}
		return collector;
	}

	protected void setSerializers() {
		setSerializer();
	}

	protected void setSerializer() {
		outTypeInfo = typeWrapper.getOutputTypeInfo();
		outSerializer = new StreamRecordSerializer<OUT>(outTypeInfo);
		outSerializationDelegate = new SerializationDelegate<StreamRecord<OUT>>(outSerializer);
		outSerializationDelegate.setInstance(outSerializer.createInstance());
	}

	private void setPartitioner(int outputNumber,
			List<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>> outputs) {
		StreamPartitioner<OUT> outputPartitioner = null;

		try {
			outputPartitioner = configuration.getPartitioner(outputNumber);

		} catch (Exception e) {
			throw new StreamComponentException("Cannot deserialize partitioner for " + name
					+ " with " + outputNumber + " outputs", e);
		}

		RecordWriter<SerializationDelegate<StreamRecord<OUT>>> output;

		if (bufferTimeout > 0) {
			output = new StreamRecordWriter<SerializationDelegate<StreamRecord<OUT>>>(this,
					outputPartitioner, bufferTimeout);
		} else {
			output = new RecordWriter<SerializationDelegate<StreamRecord<OUT>>>(this,
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

	/**
	 * Reads and creates a StreamComponent from the config.
	 * 
	 * @param userFunctionClass
	 *            Class of the invokable function
	 * @return The StreamComponent object
	 */
	@SuppressWarnings("unchecked")
	protected <T extends StreamComponentInvokable<OUT>> T getInvokable() {
		return (T) configuration.getUserInvokableObject();
	}

	protected <IN> MutableObjectIterator<StreamRecord<IN>> createInputIterator(
			MutableReader<?> inputReader, TypeSerializer<?> serializer) {

		// generic data type serialization
		@SuppressWarnings("unchecked")
		MutableReader<DeserializationDelegate<?>> reader = (MutableReader<DeserializationDelegate<?>>) inputReader;
		@SuppressWarnings({ "unchecked", "rawtypes" })
		final MutableObjectIterator<StreamRecord<IN>> iter = new ReaderIterator(reader, serializer);
		return iter;
	}

	@SuppressWarnings("unchecked")
	protected static <T> T deserializeObject(byte[] serializedObject) throws IOException,
			ClassNotFoundException {
		return (T) SerializationUtils.deserialize(serializedObject);
	}

	protected abstract void setInputsOutputs();

	protected abstract void setInvokable();

}
