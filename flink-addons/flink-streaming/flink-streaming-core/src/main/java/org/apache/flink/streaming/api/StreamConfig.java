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

package org.apache.flink.streaming.api;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.SerializationException;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.collector.OutputSelector;
import org.apache.flink.streaming.api.invokable.StreamInvokable;
import org.apache.flink.streaming.api.streamvertex.StreamVertexException;
import org.apache.flink.streaming.partitioner.ShufflePartitioner;
import org.apache.flink.streaming.partitioner.StreamPartitioner;
import org.apache.flink.streaming.state.OperatorState;
import org.apache.flink.streaming.util.serialization.TypeWrapper;
import org.apache.flink.util.InstantiationUtil;

public class StreamConfig {
	private static final String INPUT_TYPE = "inputType_";
	private static final String NUMBER_OF_OUTPUTS = "numberOfOutputs";
	private static final String NUMBER_OF_INPUTS = "numberOfInputs";
	private static final String OUTPUT_NAME = "outputName_";
	private static final String OUTPUT_SELECT_ALL = "outputSelectAll_";
	private static final String PARTITIONER_OBJECT = "partitionerObject_";
	private static final String NUMBER_OF_OUTPUT_CHANNELS = "numOfOutputs_";
	private static final String ITERATION_ID = "iteration-id";
	private static final String OUTPUT_SELECTOR = "outputSelector";
	private static final String DIRECTED_EMIT = "directedEmit";
	private static final String FUNCTION_NAME = "operatorName";
	private static final String FUNCTION = "operator";
	private static final String VERTEX_NAME = "vertexName";
	private static final String SERIALIZEDUDF = "serializedudf";
	private static final String USER_FUNCTION = "userfunction";
	private static final String BUFFER_TIMEOUT = "bufferTimeout";
	private static final String OPERATOR_STATES = "operatorStates";

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

	private static final String TYPE_WRAPPER_IN_1 = "typeWrapper_in_1";
	private static final String TYPE_WRAPPER_IN_2 = "typeWrapper_in_2";
	private static final String TYPE_WRAPPER_OUT_1 = "typeWrapper_out_1";
	private static final String TYPE_WRAPPER_OUT_2 = "typeWrapper_out_2";

	public void setTypeWrapperIn1(TypeWrapper<?> typeWrapper) {
		setTypeWrapper(TYPE_WRAPPER_IN_1, typeWrapper);
	}

	public void setTypeWrapperIn2(TypeWrapper<?> typeWrapper) {
		setTypeWrapper(TYPE_WRAPPER_IN_2, typeWrapper);
	}

	public void setTypeWrapperOut1(TypeWrapper<?> typeWrapper) {
		setTypeWrapper(TYPE_WRAPPER_OUT_1, typeWrapper);
	}

	public void setTypeWrapperOut2(TypeWrapper<?> typeWrapper) {
		setTypeWrapper(TYPE_WRAPPER_OUT_2, typeWrapper);
	}

	public <T> TypeInformation<T> getTypeInfoIn1(ClassLoader cl) {
		return getTypeInfo(TYPE_WRAPPER_IN_1, cl);
	}

	public <T> TypeInformation<T> getTypeInfoIn2(ClassLoader cl) {
		return getTypeInfo(TYPE_WRAPPER_IN_2, cl);
	}

	public <T> TypeInformation<T> getTypeInfoOut1(ClassLoader cl) {
		return getTypeInfo(TYPE_WRAPPER_OUT_1, cl);
	}

	public <T> TypeInformation<T> getTypeInfoOut2(ClassLoader cl) {
		return getTypeInfo(TYPE_WRAPPER_OUT_2, cl);
	}

	private void setTypeWrapper(String key, TypeWrapper<?> typeWrapper) {
		config.setBytes(key, SerializationUtils.serialize(typeWrapper));
	}

	@SuppressWarnings("unchecked")
	private <T> TypeInformation<T> getTypeInfo(String key, ClassLoader cl) {

		TypeWrapper<T> typeWrapper;
		try {
			typeWrapper = (TypeWrapper<T>) InstantiationUtil.readObjectFromConfig(this.config, key,
					cl);
		} catch (Exception e) {
			throw new RuntimeException("Cannot load typeinfo");
		}
		if (typeWrapper != null) {
			return typeWrapper.getTypeInfo();
		} else {
			return null;
		}

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

	public void setUserInvokable(StreamInvokable<?, ?> invokableObject) {
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

	@SuppressWarnings({ "unchecked" })
	public <T> T getUserInvokable(ClassLoader cl) {
		try {
			return (T) InstantiationUtil.readObjectFromConfig(this.config, SERIALIZEDUDF, cl);
		} catch (Exception e) {
			throw new StreamVertexException("Cannot instantiate user function", e);
		}
	}

	public void setVertexName(String vertexName) {
		config.setString(VERTEX_NAME, vertexName);
	}

	public String getVertexName() {
		return config.getString(VERTEX_NAME, null);
	}

	public void setFunction(byte[] serializedFunction, String functionName) {
		if (serializedFunction != null) {
			config.setBytes(FUNCTION, serializedFunction);
			config.setString(FUNCTION_NAME, functionName);
		}
	}

	public Object getFunction(ClassLoader cl) {
		try {
			return InstantiationUtil.readObjectFromConfig(this.config, FUNCTION, cl);
		} catch (Exception e) {
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

	@SuppressWarnings("unchecked")
	public <T> OutputSelector<T> getOutputSelector(ClassLoader cl) {
		try {
			return (OutputSelector<T>) InstantiationUtil.readObjectFromConfig(this.config,
					OUTPUT_SELECTOR, cl);
		} catch (Exception e) {
			throw new StreamVertexException("Cannot deserialize and instantiate OutputSelector", e);
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

	public <T> StreamPartitioner<T> getPartitioner(ClassLoader cl, int outputIndex)
			throws ClassNotFoundException, IOException {
		@SuppressWarnings("unchecked")
		StreamPartitioner<T> partitioner = (StreamPartitioner<T>) InstantiationUtil
				.readObjectFromConfig(this.config, PARTITIONER_OBJECT + outputIndex, cl);
		if (partitioner != null) {
			return partitioner;
		} else {
			return new ShufflePartitioner<T>();
		}
	}

	public void setSelectAll(int outputIndex, Boolean selectAll) {
		if (selectAll != null) {
			config.setBoolean(OUTPUT_SELECT_ALL + outputIndex, selectAll);
		}
	}

	public boolean getSelectAll(int outputIndex) {
		return config.getBoolean(OUTPUT_SELECT_ALL + outputIndex, false);
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

	public Class<? extends AbstractRichFunction> getFunctionClass(ClassLoader cl) {
		try {
			return config.getClass("functionClass", null, cl);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("Could not load function class", e);
		}
	}

	public void setOperatorStates(Map<String, OperatorState<?>> states) {
		config.setBytes(OPERATOR_STATES, SerializationUtils.serialize((Serializable) states));
	}

	@SuppressWarnings("unchecked")
	public Map<String, OperatorState<?>> getOperatorStates(ClassLoader cl) {
		try {
			return (Map<String, OperatorState<?>>) InstantiationUtil.readObjectFromConfig(
					this.config, OPERATOR_STATES, cl);
		} catch (Exception e) {
			throw new RuntimeException("Could not load operator state");
		}
	}

}
