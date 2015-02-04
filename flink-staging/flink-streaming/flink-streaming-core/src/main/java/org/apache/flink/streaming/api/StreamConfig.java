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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.SerializationException;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.collector.OutputSelector;
import org.apache.flink.streaming.api.invokable.StreamInvokable;
import org.apache.flink.streaming.api.streamrecord.StreamRecordSerializer;
import org.apache.flink.streaming.api.streamvertex.StreamVertexException;
import org.apache.flink.streaming.partitioner.StreamPartitioner;
import org.apache.flink.streaming.state.OperatorState;
import org.apache.flink.util.InstantiationUtil;

public class StreamConfig implements Serializable {

	private static final long serialVersionUID = 1L;

	private static final String INPUT_TYPE = "inputType_";
	private static final String NUMBER_OF_OUTPUTS = "numberOfOutputs";
	private static final String NUMBER_OF_INPUTS = "numberOfInputs";
	private static final String CHAINED_OUTPUTS = "chainedOutputs";
	private static final String CHAINED_TASK_CONFIG = "chainedTaskConfig_";
	private static final String IS_CHAINED_VERTEX = "isChainedSubtask";
	private static final String OUTPUT_NAME = "outputName_";
	private static final String PARTITIONER_OBJECT = "partitionerObject_";
	private static final String VERTEX_NAME = "vertexName";
	private static final String ITERATION_ID = "iteration-id";
	private static final String OUTPUT_SELECTOR = "outputSelector";
	private static final String DIRECTED_EMIT = "directedEmit";
	private static final String SERIALIZEDUDF = "serializedudf";
	private static final String USER_FUNCTION = "userfunction";
	private static final String BUFFER_TIMEOUT = "bufferTimeout";
	private static final String OPERATOR_STATES = "operatorStates";
	private static final String TYPE_SERIALIZER_IN_1 = "typeSerializer_in_1";
	private static final String TYPE_SERIALIZER_IN_2 = "typeSerializer_in_2";
	private static final String TYPE_SERIALIZER_OUT_1 = "typeSerializer_out_1";
	private static final String TYPE_SERIALIZER_OUT_2 = "typeSerializer_out_2";
	private static final String ITERATON_WAIT = "iterationWait";
	private static final String OUTPUTS = "outVertexNames";
	private static final String EDGES_IN_ORDER = "rwOrder";

	// DEFAULT VALUES

	private static final long DEFAULT_TIMEOUT = 100;

	// CONFIG METHODS

	private Configuration config;

	public StreamConfig(Configuration config) {
		this.config = config;
	}

	public Configuration getConfiguration() {
		return config;
	}

	public void setVertexName(String vertexName) {
		config.setString(VERTEX_NAME, vertexName);
	}

	public String getTaskName() {
		return config.getString(VERTEX_NAME, "Missing");
	}

	public void setTypeSerializerIn1(StreamRecordSerializer<?> serializer) {
		setTypeSerializer(TYPE_SERIALIZER_IN_1, serializer);
	}

	public void setTypeSerializerIn2(StreamRecordSerializer<?> serializer) {
		setTypeSerializer(TYPE_SERIALIZER_IN_2, serializer);
	}

	public void setTypeSerializerOut1(StreamRecordSerializer<?> serializer) {
		setTypeSerializer(TYPE_SERIALIZER_OUT_1, serializer);
	}

	public void setTypeSerializerOut2(StreamRecordSerializer<?> serializer) {
		setTypeSerializer(TYPE_SERIALIZER_OUT_2, serializer);
	}

	@SuppressWarnings("unchecked")
	public <T> StreamRecordSerializer<T> getTypeSerializerIn1(ClassLoader cl) {
		try {
			return (StreamRecordSerializer<T>) InstantiationUtil.readObjectFromConfig(this.config,
					TYPE_SERIALIZER_IN_1, cl);
		} catch (Exception e) {
			throw new RuntimeException("Could not instantiate serializer.");
		}
	}

	@SuppressWarnings("unchecked")
	public <T> StreamRecordSerializer<T> getTypeSerializerIn2(ClassLoader cl) {
		try {
			return (StreamRecordSerializer<T>) InstantiationUtil.readObjectFromConfig(this.config,
					TYPE_SERIALIZER_IN_2, cl);
		} catch (Exception e) {
			throw new RuntimeException("Could not instantiate serializer.");
		}
	}

	@SuppressWarnings("unchecked")
	public <T> StreamRecordSerializer<T> getTypeSerializerOut1(ClassLoader cl) {
		try {
			return (StreamRecordSerializer<T>) InstantiationUtil.readObjectFromConfig(this.config,
					TYPE_SERIALIZER_OUT_1, cl);
		} catch (Exception e) {
			throw new RuntimeException("Could not instantiate serializer.");
		}
	}

	@SuppressWarnings("unchecked")
	public <T> StreamRecordSerializer<T> getTypeSerializerOut2(ClassLoader cl) {
		try {
			return (StreamRecordSerializer<T>) InstantiationUtil.readObjectFromConfig(this.config,
					TYPE_SERIALIZER_OUT_2, cl);
		} catch (Exception e) {
			throw new RuntimeException("Could not instantiate serializer.");
		}
	}

	private void setTypeSerializer(String key, StreamRecordSerializer<?> typeWrapper) {
		config.setBytes(key, SerializationUtils.serialize(typeWrapper));
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

	public void setDirectedEmit(boolean directedEmit) {
		config.setBoolean(DIRECTED_EMIT, directedEmit);
	}

	public boolean isDirectedEmit() {
		return config.getBoolean(DIRECTED_EMIT, false);
	}

	public void setOutputSelectors(List<OutputSelector<?>> outputSelector) {
		try {
			if (outputSelector != null) {
				setDirectedEmit(true);
				config.setBytes(OUTPUT_SELECTOR,
						SerializationUtils.serialize((Serializable) outputSelector));
			}
		} catch (SerializationException e) {
			throw new RuntimeException("Cannot serialize OutputSelector");
		}
	}

	@SuppressWarnings("unchecked")
	public <T> List<OutputSelector<T>> getOutputSelectors(ClassLoader cl) {
		try {
			return (List<OutputSelector<T>>) InstantiationUtil.readObjectFromConfig(this.config,
					OUTPUT_SELECTOR, cl);
		} catch (Exception e) {
			throw new StreamVertexException("Cannot deserialize and instantiate OutputSelector", e);
		}
	}

	public void setIterationId(Integer iterationId) {
		config.setInteger(ITERATION_ID, iterationId);
	}

	public Integer getIterationId() {
		return config.getInteger(ITERATION_ID, 0);
	}

	public void setIterationWaitTime(long time) {
		config.setLong(ITERATON_WAIT, time);
	}

	public long getIterationWaitTime() {
		return config.getLong(ITERATON_WAIT, 0);
	}

	public <T> void setPartitioner(String output, StreamPartitioner<T> partitionerObject) {

		config.setBytes(PARTITIONER_OBJECT + output,
				SerializationUtils.serialize(partitionerObject));
	}

	@SuppressWarnings("unchecked")
	public <T> StreamPartitioner<T> getPartitioner(ClassLoader cl, String output) {
		StreamPartitioner<T> partitioner = null;
		try {
			partitioner = (StreamPartitioner<T>) InstantiationUtil.readObjectFromConfig(
					this.config, PARTITIONER_OBJECT + output, cl);
		} catch (Exception e) {
			throw new RuntimeException("Partitioner could not be instantiated.");
		}
		return partitioner;
	}

	public void setSelectedNames(String output, List<String> selected) {
		if (selected != null) {
			config.setBytes(OUTPUT_NAME + output,
					SerializationUtils.serialize((Serializable) selected));
		} else {
			config.setBytes(OUTPUT_NAME + output,
					SerializationUtils.serialize((Serializable) new ArrayList<String>()));
		}
	}

	@SuppressWarnings("unchecked")
	public List<String> getSelectedNames(String output) {
		return (List<String>) SerializationUtils.deserialize(config.getBytes(OUTPUT_NAME + output,
				null));
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

	public void setOutputs(List<String> outputVertexNames) {
		config.setBytes(OUTPUTS, SerializationUtils.serialize((Serializable) outputVertexNames));
	}

	@SuppressWarnings("unchecked")
	public List<String> getOutputs(ClassLoader cl) {
		try {
			return (List<String>) InstantiationUtil.readObjectFromConfig(this.config, OUTPUTS, cl);
		} catch (Exception e) {
			throw new RuntimeException("Could not instantiate outputs.");
		}
	}

	public void setOutEdgesInOrder(List<Tuple2<String, String>> outEdgeList) {

		config.setBytes(EDGES_IN_ORDER, SerializationUtils.serialize((Serializable) outEdgeList));
	}

	@SuppressWarnings("unchecked")
	public List<Tuple2<String, String>> getOutEdgesInOrder(ClassLoader cl) {
		try {
			return (List<Tuple2<String, String>>) InstantiationUtil.readObjectFromConfig(
					this.config, EDGES_IN_ORDER, cl);
		} catch (Exception e) {
			throw new RuntimeException("Could not instantiate outputs.");
		}
	}

	public void setInputIndex(int inputNumber, Integer inputTypeNumber) {
		config.setInteger(INPUT_TYPE + inputNumber++, inputTypeNumber);
	}

	public int getInputIndex(int inputNumber) {
		return config.getInteger(INPUT_TYPE + inputNumber, 0);
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

	public void setChainedOutputs(List<String> chainedOutputs) {
		config.setBytes(CHAINED_OUTPUTS,
				SerializationUtils.serialize((Serializable) chainedOutputs));
	}

	@SuppressWarnings("unchecked")
	public List<String> getChainedOutputs(ClassLoader cl) {
		try {
			return (List<String>) InstantiationUtil.readObjectFromConfig(this.config,
					CHAINED_OUTPUTS, cl);
		} catch (Exception e) {
			throw new RuntimeException("Could not instantiate chained outputs.");
		}
	}

	public void setTransitiveChainedTaskConfigs(Map<String, StreamConfig> chainedTaskConfigs) {
		config.setBytes(CHAINED_TASK_CONFIG,
				SerializationUtils.serialize((Serializable) chainedTaskConfigs));
	}

	@SuppressWarnings("unchecked")
	public Map<String, StreamConfig> getTransitiveChainedTaskConfigs(ClassLoader cl) {
		try {

			Map<String, StreamConfig> confs = (Map<String, StreamConfig>) InstantiationUtil
					.readObjectFromConfig(this.config, CHAINED_TASK_CONFIG, cl);

			return confs == null ? new HashMap<String, StreamConfig>() : confs;
		} catch (Exception e) {
			throw new RuntimeException("Could not instantiate configuration.");
		}
	}

	public void setChainStart() {
		config.setBoolean(IS_CHAINED_VERTEX, true);
	}

	public boolean isChainStart() {
		return config.getBoolean(IS_CHAINED_VERTEX, false);
	}

	@Override
	public String toString() {

		ClassLoader cl = getClass().getClassLoader();

		StringBuilder builder = new StringBuilder();
		builder.append("\n=======================");
		builder.append("Stream Config");
		builder.append("=======================");
		builder.append("\nTask name: " + getTaskName());
		builder.append("\nNumber of non-chained inputs: " + getNumberOfInputs());
		builder.append("\nNumber of non-chained outputs: " + getNumberOfOutputs());
		builder.append("\nOutput names: " + getOutputs(cl));
		builder.append("\nPartitioning:");
		for (String outputname : getOutputs(cl)) {
			builder.append("\n\t" + outputname + ": " + getPartitioner(cl, outputname));
		}

		builder.append("\nChained subtasks: " + getChainedOutputs(cl));

		try {
			builder.append("\nInvokable: " + getUserInvokable(cl).getClass().getSimpleName());
		} catch (Exception e) {
			builder.append("\nInvokable: Missing");
		}
		builder.append("\nBuffer timeout: " + getBufferTimeout());
		if (isChainStart() && getChainedOutputs(cl).size() > 0) {
			builder.append("\n\n\n---------------------\nChained task configs\n---------------------\n");
			builder.append(getTransitiveChainedTaskConfigs(cl)).toString();
		}

		return builder.toString();
	}
}
