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

package org.apache.flink.streaming.api.graph;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateHandleProvider;
import org.apache.flink.streaming.api.collector.selector.OutputSelectorWrapper;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecordSerializer;
import org.apache.flink.streaming.runtime.tasks.StreamTaskException;
import org.apache.flink.util.InstantiationUtil;

public class StreamConfig implements Serializable {

	private static final long serialVersionUID = 1L;

	private static final String NUMBER_OF_OUTPUTS = "numberOfOutputs";
	private static final String NUMBER_OF_INPUTS = "numberOfInputs";
	private static final String CHAINED_OUTPUTS = "chainedOutputs";
	private static final String CHAINED_TASK_CONFIG = "chainedTaskConfig_";
	private static final String IS_CHAINED_VERTEX = "isChainedSubtask";
	private static final String OUTPUT_NAME = "outputName_";
	private static final String VERTEX_NAME = "vertexID";
	private static final String OPERATOR_NAME = "operatorName";
	private static final String ITERATION_ID = "iterationId";
	private static final String OUTPUT_SELECTOR_WRAPPER = "outputSelectorWrapper";
	private static final String SERIALIZEDUDF = "serializedUDF";
	private static final String USER_FUNCTION = "userFunction";
	private static final String BUFFER_TIMEOUT = "bufferTimeout";
	private static final String TYPE_SERIALIZER_IN_1 = "typeSerializer_in_1";
	private static final String TYPE_SERIALIZER_IN_2 = "typeSerializer_in_2";
	private static final String TYPE_SERIALIZER_OUT_1 = "typeSerializer_out_1";
	private static final String TYPE_SERIALIZER_OUT_2 = "typeSerializer_out_2";
	private static final String ITERATON_WAIT = "iterationWait";
	private static final String NONCHAINED_OUTPUTS = "nonChainedOutputs";
	private static final String EDGES_IN_ORDER = "edgesInOrder";
	private static final String OUT_STREAM_EDGES = "outStreamEdges";
	private static final String IN_STREAM_EDGES = "inStreamEdges";
	private static final String STATEHANDLE_PROVIDER = "stateHandleProvider";

	// DEFAULT VALUES
	private static final long DEFAULT_TIMEOUT = 100;
	public static final String STATE_MONITORING = "STATE_MONITORING";

	// CONFIG METHODS

	private Configuration config;

	public StreamConfig(Configuration config) {
		this.config = config;
	}

	public Configuration getConfiguration() {
		return config;
	}

	public void setVertexID(Integer vertexID) {
		config.setInteger(VERTEX_NAME, vertexID);
	}

	public Integer getVertexID() {
		return config.getInteger(VERTEX_NAME, -1);
	}

	public void setOperatorName(String name) {
		config.setString(OPERATOR_NAME, name);
	}

	public String getOperatorName() {
		return config.getString(OPERATOR_NAME, "Missing");
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
			throw new StreamTaskException("Could not instantiate serializer.", e);
		}
	}

	@SuppressWarnings("unchecked")
	public <T> StreamRecordSerializer<T> getTypeSerializerIn2(ClassLoader cl) {
		try {
			return (StreamRecordSerializer<T>) InstantiationUtil.readObjectFromConfig(this.config,
					TYPE_SERIALIZER_IN_2, cl);
		} catch (Exception e) {
			throw new StreamTaskException("Could not instantiate serializer.", e);
		}
	}

	@SuppressWarnings("unchecked")
	public <T> StreamRecordSerializer<T> getTypeSerializerOut1(ClassLoader cl) {
		try {
			return (StreamRecordSerializer<T>) InstantiationUtil.readObjectFromConfig(this.config,
					TYPE_SERIALIZER_OUT_1, cl);
		} catch (Exception e) {
			throw new StreamTaskException("Could not instantiate serializer.", e);
		}
	}

	@SuppressWarnings("unchecked")
	public <T> StreamRecordSerializer<T> getTypeSerializerOut2(ClassLoader cl) {
		try {
			return (StreamRecordSerializer<T>) InstantiationUtil.readObjectFromConfig(this.config,
					TYPE_SERIALIZER_OUT_2, cl);
		} catch (Exception e) {
			throw new StreamTaskException("Could not instantiate serializer.", e);
		}
	}

	private void setTypeSerializer(String key, StreamRecordSerializer<?> typeWrapper) {
		try {
			InstantiationUtil.writeObjectToConfig(typeWrapper, this.config, key);
		} catch (IOException e) {
			throw new StreamTaskException("Could not serialize type serializer.", e);
		}
	}

	public void setBufferTimeout(long timeout) {
		config.setLong(BUFFER_TIMEOUT, timeout);
	}

	public long getBufferTimeout() {
		return config.getLong(BUFFER_TIMEOUT, DEFAULT_TIMEOUT);
	}

	public void setStreamOperator(StreamOperator<?> operator) {
		if (operator != null) {
			config.setClass(USER_FUNCTION, operator.getClass());

			try {
				InstantiationUtil.writeObjectToConfig(operator, this.config, SERIALIZEDUDF);
			} catch (IOException e) {
				throw new StreamTaskException("Cannot serialize operator object "
						+ operator.getClass() + ".", e);
			}
		}
	}

	@SuppressWarnings({ "unchecked" })
	public <T> T getStreamOperator(ClassLoader cl) {
		try {
			return (T) InstantiationUtil.readObjectFromConfig(this.config, SERIALIZEDUDF, cl);
		} catch (Exception e) {
			throw new StreamTaskException("Cannot instantiate user function.", e);
		}
	}

	public void setOutputSelectorWrapper(OutputSelectorWrapper<?> outputSelectorWrapper) {
		try {
			InstantiationUtil.writeObjectToConfig(outputSelectorWrapper, this.config, OUTPUT_SELECTOR_WRAPPER);
		} catch (IOException e) {
			throw new StreamTaskException("Cannot serialize OutputSelectorWrapper.", e);
		}
	}

	@SuppressWarnings("unchecked")
	public <T> OutputSelectorWrapper<T> getOutputSelectorWrapper(ClassLoader cl) {
		try {
			return (OutputSelectorWrapper<T>) InstantiationUtil.readObjectFromConfig(this.config,
					OUTPUT_SELECTOR_WRAPPER, cl);
		} catch (Exception e) {
			throw new StreamTaskException("Cannot deserialize and instantiate OutputSelectorWrapper.", e);
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

	public void setSelectedNames(Integer output, List<String> selected) {
		if (selected == null) {
			selected = new ArrayList<String>();
		}

		try {
			InstantiationUtil.writeObjectToConfig(selected, this.config, OUTPUT_NAME + output);
		} catch (IOException e) {
			throw new StreamTaskException("Cannot serialize OutputSelector for name \"" + output+ "\".", e);
		}
	}

	@SuppressWarnings("unchecked")
	public List<String> getSelectedNames(Integer output, ClassLoader cl) {
		List<String> selectedNames;
		try {
			selectedNames = (List<String>) InstantiationUtil.readObjectFromConfig(this.config, OUTPUT_NAME + output, cl);
		} catch (Exception e) {
			throw new StreamTaskException("Cannot deserialize OutputSelector for name \"" + output + "\".", e);
		}
		return selectedNames == null ? new ArrayList<String>() : selectedNames;
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

	public void setNonChainedOutputs(List<StreamEdge> outputvertexIDs) {
		try {
			InstantiationUtil.writeObjectToConfig(outputvertexIDs, this.config, NONCHAINED_OUTPUTS);
		} catch (IOException e) {
			throw new StreamTaskException("Cannot serialize non chained outputs.", e);
		}
	}

	@SuppressWarnings("unchecked")
	public List<StreamEdge> getNonChainedOutputs(ClassLoader cl) {
		try {
			List<StreamEdge> nonChainedOutputs = (List<StreamEdge>) InstantiationUtil.readObjectFromConfig(this.config, NONCHAINED_OUTPUTS, cl);
			return nonChainedOutputs == null ?  new ArrayList<StreamEdge>() : nonChainedOutputs;
		} catch (Exception e) {
			throw new StreamTaskException("Could not instantiate non chained outputs.", e);
		}
	}

	public void setChainedOutputs(List<StreamEdge> chainedOutputs) {
		try {
			InstantiationUtil.writeObjectToConfig(chainedOutputs, this.config, CHAINED_OUTPUTS);
		} catch (IOException e) {
			throw new StreamTaskException("Cannot serialize chained outputs.", e);
		}
	}

	@SuppressWarnings("unchecked")
	public List<StreamEdge> getChainedOutputs(ClassLoader cl) {
		try {
			List<StreamEdge> chainedOutputs = (List<StreamEdge>) InstantiationUtil.readObjectFromConfig(this.config, CHAINED_OUTPUTS, cl);
			return chainedOutputs == null ? new ArrayList<StreamEdge>() : chainedOutputs;
		} catch (Exception e) {
			throw new StreamTaskException("Could not instantiate chained outputs.", e);
		}
	}

	public void setOutEdges(List<StreamEdge> outEdges) {
		try {
			InstantiationUtil.writeObjectToConfig(outEdges, this.config, OUT_STREAM_EDGES);
		} catch (IOException e) {
			throw new StreamTaskException("Cannot serialize outward edges.", e);
		}
	}

	@SuppressWarnings("unchecked")
	public List<StreamEdge> getOutEdges(ClassLoader cl) {
		try {
			List<StreamEdge> outEdges = (List<StreamEdge>) InstantiationUtil.readObjectFromConfig(
					this.config, OUT_STREAM_EDGES, cl);
			return outEdges == null ? new ArrayList<StreamEdge>() : outEdges;
		} catch (Exception e) {
			throw new StreamTaskException("Could not instantiate outputs.", e);
		}
	}

	public void setInPhysicalEdges(List<StreamEdge> inEdges) {
		try {
			InstantiationUtil.writeObjectToConfig(inEdges, this.config, IN_STREAM_EDGES);
		} catch (IOException e) {
			throw new StreamTaskException("Cannot serialize inward edges.", e);
		}
	}

	@SuppressWarnings("unchecked")
	public List<StreamEdge> getInPhysicalEdges(ClassLoader cl) {
		try {
			List<StreamEdge> inEdges = (List<StreamEdge>) InstantiationUtil.readObjectFromConfig(
					this.config, IN_STREAM_EDGES, cl);
			return inEdges == null ? new ArrayList<StreamEdge>() : inEdges;
		} catch (Exception e) {
			throw new StreamTaskException("Could not instantiate inputs.", e);
		}
	}

	public void setStateMonitoring(boolean stateMonitoring) {
		config.setBoolean(STATE_MONITORING, stateMonitoring);
	}

	public boolean getStateMonitoring() {
		return config.getBoolean(STATE_MONITORING, false);
	}

	public void setOutEdgesInOrder(List<StreamEdge> outEdgeList) {
		try {
			InstantiationUtil.writeObjectToConfig(outEdgeList, this.config, EDGES_IN_ORDER);
		} catch (IOException e) {
			throw new StreamTaskException("Could not serialize outputs in order.", e);
		}
	}

	@SuppressWarnings("unchecked")
	public List<StreamEdge> getOutEdgesInOrder(ClassLoader cl) {
		try {
			List<StreamEdge> outEdgesInOrder = (List<StreamEdge>) InstantiationUtil.readObjectFromConfig(
					this.config, EDGES_IN_ORDER, cl);
			return outEdgesInOrder == null ? new ArrayList<StreamEdge>() : outEdgesInOrder;
		} catch (Exception e) {
			throw new StreamTaskException("Could not instantiate outputs in order.", e);
		}
	}

	public void setTransitiveChainedTaskConfigs(Map<Integer, StreamConfig> chainedTaskConfigs) {

		try {
			InstantiationUtil.writeObjectToConfig(chainedTaskConfigs, this.config, CHAINED_TASK_CONFIG);
		} catch (IOException e) {
			throw new StreamTaskException("Could not serialize configuration.", e);
		}
	}

	@SuppressWarnings("unchecked")
	public Map<Integer, StreamConfig> getTransitiveChainedTaskConfigs(ClassLoader cl) {
		try {
			Map<Integer, StreamConfig> confs = (Map<Integer, StreamConfig>) InstantiationUtil
					.readObjectFromConfig(this.config, CHAINED_TASK_CONFIG, cl);
			return confs == null ? new HashMap<Integer, StreamConfig>() : confs;
		} catch (Exception e) {
			throw new StreamTaskException("Could not instantiate configuration.", e);
		}
	}
	
	public void setStateHandleProvider(StateHandleProvider<?> provider) {

		try {
			InstantiationUtil.writeObjectToConfig(provider, this.config, STATEHANDLE_PROVIDER);
		} catch (IOException e) {
			throw new StreamTaskException("Could not serialize stateHandle provider.", e);
		}
	}

	@SuppressWarnings("unchecked")
	public <R> StateHandleProvider<R> getStateHandleProvider(ClassLoader cl) {
		try {
			return (StateHandleProvider<R>) InstantiationUtil
					.readObjectFromConfig(this.config, STATEHANDLE_PROVIDER, cl);
		} catch (Exception e) {
			throw new StreamTaskException("Could not instantiate statehandle provider.", e);
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
		builder.append("\nTask name: " + getVertexID());
		builder.append("\nNumber of non-chained inputs: " + getNumberOfInputs());
		builder.append("\nNumber of non-chained outputs: " + getNumberOfOutputs());
		builder.append("\nOutput names: " + getNonChainedOutputs(cl));
		builder.append("\nPartitioning:");
		for (StreamEdge output : getNonChainedOutputs(cl)) {
			int outputname = output.getTargetID();
			builder.append("\n\t" + outputname + ": " + output.getPartitioner());
		}

		builder.append("\nChained subtasks: " + getChainedOutputs(cl));

		try {
			builder.append("\nOperator: " + getStreamOperator(cl).getClass().getSimpleName());
		} catch (Exception e) {
			builder.append("\nOperator: Missing");
		}
		builder.append("\nBuffer timeout: " + getBufferTimeout());
		builder.append("\nState Monitoring: " + getStateMonitoring());
		if (isChainStart() && getChainedOutputs(cl).size() > 0) {
			builder.append("\n\n\n---------------------\nChained task configs\n---------------------\n");
			builder.append(getTransitiveChainedTaskConfigs(cl)).toString();
		}

		return builder.toString();
	}
}
