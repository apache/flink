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

package org.apache.flink.streaming.api;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.apache.commons.lang3.SerializationException;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.collector.OutputSelector;
import org.apache.flink.streaming.api.invokable.StreamComponentInvokable;
import org.apache.flink.streaming.api.streamcomponent.StreamComponentException;
import org.apache.flink.streaming.partitioner.ShufflePartitioner;
import org.apache.flink.streaming.partitioner.StreamPartitioner;
import org.apache.flink.streaming.util.serialization.TypeSerializerWrapper;

public class StreamConfig {
	private static final String INPUT_TYPE = "inputType_";
	private static final String NUMBER_OF_OUTPUTS = "numberOfOutputs";
	private static final String NUMBER_OF_INPUTS = "numberOfInputs";
	private static final String OUTPUT_NAME = "outputName_";
	private static final String PARTITIONER_OBJECT = "partitionerObject_";
	private static final String NUMBER_OF_OUTPUT_CHANNELS = "numOfOutputs_";
	private static final String ITERATION_ID = "iteration-id";
	private static final String OUTPUT_SELECTOR = "outputSelector";
	private static final String DIRECTED_EMIT = "directedEmit";
	private static final String FUNCTION_NAME = "operatorName";
	private static final String FUNCTION = "operator";
	private static final String COMPONENT_NAME = "componentName";
	private static final String SERIALIZEDUDF = "serializedudf";
	private static final String USER_FUNCTION = "userfunction";
	private static final String BUFFER_TIMEOUT = "bufferTimeout";

	// DEFAULT VALUES

	private static final boolean DEFAULT_IS_MUTABLE = false;

	private static final long DEFAULT_TIMEOUT = 0;

	// STRINGS

	private static final String MUTABILITY = "isMutable";
	private static final String ITERATON_WAIT = "iterationWait";

	private Configuration config;

	public StreamConfig(Configuration config) {
		this.config = config;
	}

	public Configuration getConfiguration() {
		return config;
	}

	// CONFIGS

	public void setTypeWrapper(TypeSerializerWrapper<?, ?, ?> typeWrapper) {
		config.setBytes("typeWrapper", SerializationUtils.serialize(typeWrapper));
	}

	@SuppressWarnings("unchecked")
	public <IN1, IN2, OUT> TypeSerializerWrapper<IN1, IN2, OUT> getTypeWrapper() {
		byte[] serializedWrapper = config.getBytes("typeWrapper", null);

		if (serializedWrapper == null) {
			throw new RuntimeException("TypeSerializationWrapper must be set");
		}

		return (TypeSerializerWrapper<IN1, IN2, OUT>) SerializationUtils
				.deserialize(serializedWrapper);
	}

	public void setMutability(boolean isMutable) {
		config.setBoolean(MUTABILITY, isMutable);
	}

	public boolean getMutability() {
		return config.getBoolean(MUTABILITY, DEFAULT_IS_MUTABLE);
	}

	public void setBufferTimeout(long timeout) {
		config.setLong(BUFFER_TIMEOUT, timeout);
	}

	public long getBufferTimeout() {
		return config.getLong(BUFFER_TIMEOUT, DEFAULT_TIMEOUT);
	}

	public void setUserInvokable(StreamComponentInvokable<?> invokableObject) {
		if (invokableObject != null) {
			config.setClass(USER_FUNCTION, invokableObject.getClass());

			try {
				config.setBytes(SERIALIZEDUDF, SerializationUtils.serialize(invokableObject));
			} catch (SerializationException e) {
				throw new RuntimeException("Cannot serialize invokable object "
						+ invokableObject.getClass(), e);
			}
		}
	}

	public <T> StreamComponentInvokable<T> getUserInvokableObject() {
		try {
			return deserializeObject(config.getBytes(SERIALIZEDUDF, null));
		} catch (Exception e) {
			throw new StreamComponentException("Cannot instantiate user function");
		}
	}

	public void setComponentName(String componentName) {
		config.setString(COMPONENT_NAME, componentName);
	}

	public String getComponentName() {
		return config.getString(COMPONENT_NAME, null);
	}

	public void setFunction(byte[] serializedFunction, String functionName) {
		if (serializedFunction != null) {
			config.setBytes(FUNCTION, serializedFunction);
			config.setString(FUNCTION_NAME, functionName);
		}
	}

	public Object getFunction() {
		try {
			return SerializationUtils.deserialize(config.getBytes(FUNCTION, null));
		} catch (SerializationException e) {
			throw new RuntimeException("Cannot deserialize invokable object", e);
		}
	}

	public String getFunctionName() {
		return config.getString(FUNCTION_NAME, "");
	}

	public void setDirectedEmit(boolean directedEmit) {
		config.setBoolean(DIRECTED_EMIT, directedEmit);
	}

	public boolean getDirectedEmit() {
		return config.getBoolean(DIRECTED_EMIT, false);
	}

	public void setOutputSelector(byte[] outputSelector) {
		if (outputSelector != null) {
			setDirectedEmit(true);
			config.setBytes(OUTPUT_SELECTOR, outputSelector);
		}
	}

	public <T> OutputSelector<T> getOutputSelector() {
		try {
			return deserializeObject(config.getBytes(OUTPUT_SELECTOR, null));
		} catch (Exception e) {
			throw new StreamComponentException("Cannot deserialize and instantiate OutputSelector",
					e);
		}
	}

	public void setIterationId(String iterationId) {
		config.setString(ITERATION_ID, iterationId);
	}

	public String getIterationId() {
		return config.getString(ITERATION_ID, "iteration-0");
	}

	public void setIterationWaitTime(long time) {
		config.setLong(ITERATON_WAIT, time);
	}

	public long getIterationWaitTime() {
		return config.getLong(ITERATON_WAIT, 0);
	}

	public void setNumberOfOutputChannels(int outputIndex, Integer numberOfOutputChannels) {
		config.setInteger(NUMBER_OF_OUTPUT_CHANNELS + outputIndex, numberOfOutputChannels);
	}

	public int getNumberOfOutputChannels(int outputIndex) {
		return config.getInteger(NUMBER_OF_OUTPUT_CHANNELS + outputIndex, 0);
	}

	public <T> void setPartitioner(int outputIndex, StreamPartitioner<T> partitionerObject) {

		config.setBytes(PARTITIONER_OBJECT + outputIndex,
				SerializationUtils.serialize(partitionerObject));
	}

	public <T> StreamPartitioner<T> getPartitioner(int outputIndex) throws ClassNotFoundException,
			IOException {
		return deserializeObject(config.getBytes(PARTITIONER_OBJECT + outputIndex,
				SerializationUtils.serialize(new ShufflePartitioner<T>())));
	}

	public void setOutputName(int outputIndex, List<String> outputName) {
		if (outputName != null) {
			config.setBytes(OUTPUT_NAME + outputIndex,
					SerializationUtils.serialize((Serializable) outputName));
		}
	}

	@SuppressWarnings("unchecked")
	public List<String> getOutputName(int outputIndex) {
		return (List<String>) SerializationUtils.deserialize(config.getBytes(OUTPUT_NAME
				+ outputIndex, null));
	}

	public void setNumberOfInputs(int numberOfInputs) {
		config.setInteger(NUMBER_OF_INPUTS, numberOfInputs);
	}

	public int getNumberOfInputs() {
		return config.getInteger(NUMBER_OF_INPUTS, 0);
	}

	public void setNumberOfOutputs(int numberOfOutputs) {
		config.setInteger(NUMBER_OF_OUTPUTS, numberOfOutputs);
	}

	public int getNumberOfOutputs() {
		return config.getInteger(NUMBER_OF_OUTPUTS, 0);
	}

	public void setInputType(int inputNumber, Integer inputTypeNumber) {
		config.setInteger(INPUT_TYPE + inputNumber++, inputTypeNumber);
	}

	public int getInputType(int inputNumber) {
		return config.getInteger(INPUT_TYPE + inputNumber, 0);
	}

	public void setFunctionClass(Class<? extends AbstractRichFunction> functionClass) {
		config.setClass("functionClass", functionClass);
	}

	@SuppressWarnings("unchecked")
	public Class<? extends AbstractRichFunction> getFunctionClass() {
		return (Class<? extends AbstractRichFunction>) config.getClass("functionClass", null);
	}

	@SuppressWarnings("unchecked")
	protected static <T> T deserializeObject(byte[] serializedObject) throws IOException,
			ClassNotFoundException {
		return (T) SerializationUtils.deserialize(serializedObject);
	}

}
