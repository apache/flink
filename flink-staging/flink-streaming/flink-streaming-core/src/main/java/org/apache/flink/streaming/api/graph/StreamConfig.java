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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateHandleProvider;
import org.apache.flink.runtime.util.ClassLoaderUtil;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.collector.selector.OutputSelectorWrapper;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.tasks.StreamTaskException;
import org.apache.flink.util.InstantiationUtil;

public class StreamConfig implements Serializable {

	private static final long serialVersionUID = 1L;

	// ------------------------------------------------------------------------
	//  Config Keys
	// ------------------------------------------------------------------------
	
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

	private static final String CHECKPOINTING_ENABLED = "checkpointing";
	private static final String STATEHANDLE_PROVIDER = "stateHandleProvider";
	private static final String STATE_PARTITIONER = "statePartitioner";
	private static final String CHECKPOINT_MODE = "checkpointMode";
	
	
	// ------------------------------------------------------------------------
	//  Default Values
	// ------------------------------------------------------------------------
	
	private static final long DEFAULT_TIMEOUT = 100;
	private static final CheckpointingMode DEFAULT_CHECKPOINTING_MODE = CheckpointingMode.EXACTLY_ONCE;
	
	
	// ------------------------------------------------------------------------
	//  Config
	// ------------------------------------------------------------------------

	private final Configuration config;

	public StreamConfig(Configuration config) {
		this.config = config;
	}

	public Configuration getConfiguration() {
		return config;
	}

	// ------------------------------------------------------------------------
	//  Configured Properties
	// ------------------------------------------------------------------------
	

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

	public void setTypeSerializerIn1(TypeSerializer<?> serializer) {
		setTypeSerializer(TYPE_SERIALIZER_IN_1, serializer);
	}

	public void setTypeSerializerIn2(TypeSerializer<?> serializer) {
		setTypeSerializer(TYPE_SERIALIZER_IN_2, serializer);
	}

	public void setTypeSerializerOut1(TypeSerializer<?> serializer) {
		setTypeSerializer(TYPE_SERIALIZER_OUT_1, serializer);
	}

	public void setTypeSerializerOut2(TypeSerializer<?> serializer) {
		setTypeSerializer(TYPE_SERIALIZER_OUT_2, serializer);
	}

	@SuppressWarnings("unchecked")
	public <T> TypeSerializer<T> getTypeSerializerIn1(ClassLoader cl) {
		try {
			return (TypeSerializer<T>) InstantiationUtil.readObjectFromConfig(this.config,
					TYPE_SERIALIZER_IN_1, cl);
		} catch (Exception e) {
			throw new StreamTaskException("Could not instantiate serializer.", e);
		}
	}

	@SuppressWarnings("unchecked")
	public <T> TypeSerializer<T> getTypeSerializerIn2(ClassLoader cl) {
		try {
			return (TypeSerializer<T>) InstantiationUtil.readObjectFromConfig(this.config,
					TYPE_SERIALIZER_IN_2, cl);
		} catch (Exception e) {
			throw new StreamTaskException("Could not instantiate serializer.", e);
		}
	}

	@SuppressWarnings("unchecked")
	public <T> TypeSerializer<T> getTypeSerializerOut1(ClassLoader cl) {
		try {
			return (TypeSerializer<T>) InstantiationUtil.readObjectFromConfig(this.config,
					TYPE_SERIALIZER_OUT_1, cl);
		} catch (Exception e) {
			throw new StreamTaskException("Could not instantiate serializer.", e);
		}
	}

	@SuppressWarnings("unchecked")
	public <T> TypeSerializer<T> getTypeSerializerOut2(ClassLoader cl) {
		try {
			return (TypeSerializer<T>) InstantiationUtil.readObjectFromConfig(this.config,
					TYPE_SERIALIZER_OUT_2, cl);
		} catch (Exception e) {
			throw new StreamTaskException("Could not instantiate serializer.", e);
		}
	}

	private void setTypeSerializer(String key, TypeSerializer<?> typeWrapper) {
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
	
	public <T> T getStreamOperator(ClassLoader cl) {
		try {
			@SuppressWarnings("unchecked")
			T result = (T) InstantiationUtil.readObjectFromConfig(this.config, SERIALIZEDUDF, cl);
			return result;
		}
		catch (ClassNotFoundException e) {
			String classLoaderInfo = ClassLoaderUtil.getUserCodeClassLoaderInfo(cl);
			boolean loadableDoubleCheck = ClassLoaderUtil.validateClassLoadable(e, cl);
			
			String exceptionMessage = "Cannot load user class: " + e.getMessage()
					+ "\nClassLoader info: " + classLoaderInfo + 
					(loadableDoubleCheck ? 
							"\nClass was actually found in classloader - deserialization issue." :
							"\nClass not resolvable through given classloader.");
			
			throw new StreamTaskException(exceptionMessage);
		}
		catch (Exception e) {
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

	public void setIterationId(String iterationId) {
		config.setString(ITERATION_ID, iterationId);
	}

	public String getIterationId() {
		return config.getString(ITERATION_ID, "");
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

	// --------------------- checkpointing -----------------------
	
	public void setCheckpointingEnabled(boolean enabled) {
		config.setBoolean(CHECKPOINTING_ENABLED, enabled);
	}

	public boolean isCheckpointingEnabled() {
		return config.getBoolean(CHECKPOINTING_ENABLED, false);
	}
	
	public void setCheckpointMode(CheckpointingMode mode) {
		config.setInteger(CHECKPOINT_MODE, mode.ordinal());
	}

	public CheckpointingMode getCheckpointMode() {
		int ordinal = config.getInteger(CHECKPOINT_MODE, -1);
		if (ordinal >= 0) {
			return CheckpointingMode.values()[ordinal];
		} else {
			return DEFAULT_CHECKPOINTING_MODE; 
		}
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
	
	public void setStatePartitioner(KeySelector<?, Serializable> partitioner) {
		try {
			InstantiationUtil.writeObjectToConfig(partitioner, this.config, STATE_PARTITIONER);
		} catch (IOException e) {
			throw new StreamTaskException("Could not serialize state partitioner.", e);
		}
	}

	@SuppressWarnings("unchecked")
	public KeySelector<?, Serializable> getStatePartitioner(ClassLoader cl) {
		try {
			return (KeySelector<?, Serializable>) InstantiationUtil
					.readObjectFromConfig(this.config, STATE_PARTITIONER, cl);
		} catch (Exception e) {
			throw new StreamTaskException("Could not instantiate state partitioner.", e);
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
		builder.append("\nTask name: ").append(getVertexID());
		builder.append("\nNumber of non-chained inputs: ").append(getNumberOfInputs());
		builder.append("\nNumber of non-chained outputs: ").append(getNumberOfOutputs());
		builder.append("\nOutput names: ").append(getNonChainedOutputs(cl));
		builder.append("\nPartitioning:");
		for (StreamEdge output : getNonChainedOutputs(cl)) {
			int outputname = output.getTargetId();
			builder.append("\n\t").append(outputname).append(": ").append(output.getPartitioner());
		}

		builder.append("\nChained subtasks: ").append(getChainedOutputs(cl));

		try {
			builder.append("\nOperator: ").append(getStreamOperator(cl).getClass().getSimpleName());
		}
		catch (Exception e) {
			builder.append("\nOperator: Missing");
		}
		builder.append("\nBuffer timeout: ").append(getBufferTimeout());
		builder.append("\nState Monitoring: ").append(isCheckpointingEnabled());
		if (isChainStart() && getChainedOutputs(cl).size() > 0) {
			builder.append("\n\n\n---------------------\nChained task configs\n---------------------\n");
			builder.append(getTransitiveChainedTaskConfigs(cl));
		}

		return builder.toString();
	}
}
