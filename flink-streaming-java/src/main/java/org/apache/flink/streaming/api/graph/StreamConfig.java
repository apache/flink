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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.util.CorruptConfigurationException;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.util.ClassLoaderUtil;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorSubstitutor;
import org.apache.flink.streaming.runtime.tasks.StreamTaskException;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Internal configuration for a {@link StreamOperator}. This is created and populated by the
 * {@link StreamingJobGraphGenerator}.
 */
@Internal
public class StreamConfig implements Serializable {

	private static final long serialVersionUID = 1L;

	// ------------------------------------------------------------------------
	//  Config Keys
	// ------------------------------------------------------------------------

	private static final String NUMBER_OF_OUTPUTS = "numberOfOutputs";
	private static final String NUMBER_OF_INPUTS = "numberOfInputs";
	private static final String CHAINED_OUTPUTS = "chainedOutputs";
	private static final String IS_CHAINED_VERTEX = "isChainedSubtask";
	private static final String CHAIN_INDEX = "chainIndex";
	private static final String VERTEX_NAME = "vertexID";
	private static final String ITERATION_ID = "iterationId";
	private static final String OUTPUT_SELECTOR_WRAPPER = "outputSelectorWrapper";
	private static final String SERIALIZEDUDF = "serializedUDF";
	private static final String USER_FUNCTION = "userFunction";
	private static final String BUFFER_TIMEOUT = "bufferTimeout";
	private static final String TYPE_SERIALIZER_IN_1 = "typeSerializer_in_1";
	private static final String TYPE_SERIALIZER_IN_2 = "typeSerializer_in_2";
	private static final String TYPE_SERIALIZER_OUT_1 = "typeSerializer_out";
	private static final String TYPE_SERIALIZER_SIDEOUT_PREFIX = "typeSerializer_sideout_";
	private static final String ITERATON_WAIT = "iterationWait";
	private static final String NONCHAINED_OUTPUTS = "nonChainedOutputs";
	private static final String OPERATOR_NAME = "operatorName";
	private static final String OPERATOR_ID = "operatorID";
	private static final String CHAIN_END = "chainEnd";

	private static final String CHECKPOINTING_ENABLED = "checkpointing";
	private static final String CHECKPOINT_MODE = "checkpointMode";

	private static final String STATE_BACKEND = "statebackend";
	private static final String STATE_PARTITIONER = "statePartitioner";

	private static final String STATE_KEY_SERIALIZER = "statekeyser";

	private static final String TIME_CHARACTERISTIC = "timechar";

	private static final String CUSTOM_CONFIGURATION = "customConfiguration";

	// ------------------------------------------------------------------------
	//  Default Values
	// ------------------------------------------------------------------------

	private static final long DEFAULT_TIMEOUT = 100;
	public static final CheckpointingMode DEFAULT_CHECKPOINTING_MODE = CheckpointingMode.EXACTLY_ONCE;


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

	public void setTimeCharacteristic(TimeCharacteristic characteristic) {
		config.setInteger(TIME_CHARACTERISTIC, characteristic.ordinal());
	}

	public TimeCharacteristic getTimeCharacteristic() {
		int ordinal = config.getInteger(TIME_CHARACTERISTIC, -1);
		if (ordinal >= 0) {
			return TimeCharacteristic.values()[ordinal];
		} else {
			throw new CorruptConfigurationException("time characteristic is not set");
		}
	}

	public void setTypeSerializerIn1(TypeSerializer<?> serializer) {
		setTypeSerializer(TYPE_SERIALIZER_IN_1, serializer);
	}

	public void setTypeSerializerIn2(TypeSerializer<?> serializer) {
		setTypeSerializer(TYPE_SERIALIZER_IN_2, serializer);
	}

	public void setTypeSerializerOut(TypeSerializer<?> serializer) {
		setTypeSerializer(TYPE_SERIALIZER_OUT_1, serializer);
	}

	public void setTypeSerializerSideOut(OutputTag<?> outputTag, TypeSerializer<?> serializer) {
		setTypeSerializer(TYPE_SERIALIZER_SIDEOUT_PREFIX + outputTag.getId(), serializer);
	}

	public <T> TypeSerializer<T> getTypeSerializerIn1(ClassLoader cl) {
		try {
			return InstantiationUtil.readObjectFromConfig(this.config, TYPE_SERIALIZER_IN_1, cl);
		} catch (Exception e) {
			throw new StreamTaskException("Could not instantiate serializer.", e);
		}
	}

	public <T> TypeSerializer<T> getTypeSerializerIn2(ClassLoader cl) {
		try {
			return InstantiationUtil.readObjectFromConfig(this.config, TYPE_SERIALIZER_IN_2, cl);
		} catch (Exception e) {
			throw new StreamTaskException("Could not instantiate serializer.", e);
		}
	}

	public <T> TypeSerializer<T> getTypeSerializerOut(ClassLoader cl) {
		try {
			return InstantiationUtil.readObjectFromConfig(this.config, TYPE_SERIALIZER_OUT_1, cl);
		} catch (Exception e) {
			throw new StreamTaskException("Could not instantiate serializer.", e);
		}
	}

	public <T> TypeSerializer<T> getTypeSerializerSideOut(OutputTag<?> outputTag, ClassLoader cl) {
		Preconditions.checkNotNull(outputTag, "Side output id must not be null.");
		try {
			return InstantiationUtil.readObjectFromConfig(this.config, TYPE_SERIALIZER_SIDEOUT_PREFIX + outputTag.getId(), cl);
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

	public boolean isFlushAlwaysEnabled() {
		return getBufferTimeout() == 0;
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

	public <T extends StreamOperator<?>> T getStreamOperator(ClassLoader cl) {
		try {
			StreamOperator<?> operator = InstantiationUtil.readObjectFromConfig(this.config, SERIALIZEDUDF, cl);
			if (operator != null && operator instanceof StreamOperatorSubstitutor) {
				return (T) ((StreamOperatorSubstitutor) operator).getActualStreamOperator(cl);
			} else {
				return (T) operator;
			}
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

	public void setOutputSelectors(List<OutputSelector<?>> outputSelectors) {
		try {
			InstantiationUtil.writeObjectToConfig(outputSelectors, this.config, OUTPUT_SELECTOR_WRAPPER);
		} catch (IOException e) {
			throw new StreamTaskException("Could not serialize output selectors", e);
		}
	}

	public <T> List<OutputSelector<T>> getOutputSelectors(ClassLoader userCodeClassloader) {
		try {
			List<OutputSelector<T>> selectors =
					InstantiationUtil.readObjectFromConfig(this.config, OUTPUT_SELECTOR_WRAPPER, userCodeClassloader);
			return selectors == null ? Collections.<OutputSelector<T>>emptyList() : selectors;

		} catch (Exception e) {
			throw new StreamTaskException("Could not read output selectors", e);
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

	public List<StreamEdge> getNonChainedOutputs(ClassLoader cl) {
		try {
			List<StreamEdge> nonChainedOutputs = InstantiationUtil.readObjectFromConfig(this.config, NONCHAINED_OUTPUTS, cl);
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

	public List<StreamEdge> getChainedOutputs(ClassLoader cl) {
		try {
			List<StreamEdge> chainedOutputs = InstantiationUtil.readObjectFromConfig(this.config, CHAINED_OUTPUTS, cl);
			return chainedOutputs == null ? new ArrayList<StreamEdge>() : chainedOutputs;
		} catch (Exception e) {
			throw new StreamTaskException("Could not instantiate chained outputs.", e);
		}
	}

	public void setCustomConfiguration(Configuration configuration) {
		try {
			InstantiationUtil.writeObjectToConfig(configuration, this.config, CUSTOM_CONFIGURATION);
		} catch (IOException e) {
			throw new StreamTaskException("Cannot serialize custom configuration.", e);
		}
	}

	public Configuration getCustomConfiguration(ClassLoader cl) {
		try {
			Configuration customConfiguration = InstantiationUtil.readObjectFromConfig(this.config, CUSTOM_CONFIGURATION, cl);
			return customConfiguration == null ? new Configuration() : customConfiguration;
		} catch (Exception e) {
			throw new StreamTaskException("Could not instantiate custom configuration.", e);
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

	public void setOperatorID(OperatorID operatorID) {
		this.config.setBytes(OPERATOR_ID, operatorID.getBytes());
	}

	public OperatorID getOperatorID() {
		byte[] operatorIDBytes = config.getBytes(OPERATOR_ID, null);
		return new OperatorID(Preconditions.checkNotNull(operatorIDBytes));
	}

	public void setOperatorName(String name) {
		this.config.setString(OPERATOR_NAME, name);
	}

	public String getOperatorName() {
		return this.config.getString(OPERATOR_NAME, null);
	}

	public void setChainIndex(int index) {
		this.config.setInteger(CHAIN_INDEX, index);
	}

	public int getChainIndex() {
		return this.config.getInteger(CHAIN_INDEX, 0);
	}

	// ------------------------------------------------------------------------
	//  State backend
	// ------------------------------------------------------------------------

	public void setStateBackend(StateBackend backend) {
		if (backend != null) {
			try {
				InstantiationUtil.writeObjectToConfig(backend, this.config, STATE_BACKEND);
			} catch (Exception e) {
				throw new StreamTaskException("Could not serialize stateHandle provider.", e);
			}
		}
	}

	public StateBackend getStateBackend(ClassLoader cl) {
		try {
			return InstantiationUtil.readObjectFromConfig(this.config, STATE_BACKEND, cl);
		} catch (Exception e) {
			throw new StreamTaskException("Could not instantiate statehandle provider.", e);
		}
	}

	public byte[] getSerializedStateBackend() {
		return this.config.getBytes(STATE_BACKEND, null);
	}

	public void setStatePartitioner(int input, KeySelector<?, ?> partitioner) {
		try {
			InstantiationUtil.writeObjectToConfig(partitioner, this.config, STATE_PARTITIONER + input);
		} catch (IOException e) {
			throw new StreamTaskException("Could not serialize state partitioner.", e);
		}
	}

	public KeySelector<?, Serializable> getStatePartitioner(int input, ClassLoader cl) {
		try {
			return InstantiationUtil.readObjectFromConfig(this.config, STATE_PARTITIONER + input, cl);
		} catch (Exception e) {
			throw new StreamTaskException("Could not instantiate state partitioner.", e);
		}
	}

	public void setStateKeySerializer(TypeSerializer<?> serializer) {
		try {
			InstantiationUtil.writeObjectToConfig(serializer, this.config, STATE_KEY_SERIALIZER);
		} catch (IOException e) {
			throw new StreamTaskException("Could not serialize state key serializer.", e);
		}
	}

	public <K> TypeSerializer<K> getStateKeySerializer(ClassLoader cl) {
		try {
			return InstantiationUtil.readObjectFromConfig(this.config, STATE_KEY_SERIALIZER, cl);
		} catch (Exception e) {
			throw new StreamTaskException("Could not instantiate state key serializer from task config.", e);
		}
	}

	// ------------------------------------------------------------------------
	//  Miscellaneous
	// ------------------------------------------------------------------------

	public void setChainStart() {
		config.setBoolean(IS_CHAINED_VERTEX, true);
	}

	public boolean isChainStart() {
		return config.getBoolean(IS_CHAINED_VERTEX, false);
	}

	public void setChainEnd() {
		config.setBoolean(CHAIN_END, true);
	}

	public boolean isChainEnd() {
		return config.getBoolean(CHAIN_END, false);
	}

	@Override
	public String toString() {

		ClassLoader cl = getClass().getClassLoader();

		StringBuilder builder = new StringBuilder();
		builder.append("\n=======================");
		builder.append("Stream Config (vertexID: ").append(getVertexID()).append(")");
		builder.append("=======================");
		builder.append("\nNumber of non-chained inputs: ").append(getNumberOfInputs());
		builder.append("\nNumber of non-chained outputs: ").append(getNumberOfOutputs());
		builder.append("\nOutput names: ").append(getNonChainedOutputs(cl));
		builder.append("\nPartitioning:");
		for (StreamEdge output : getNonChainedOutputs(cl)) {
			int outputname = output.getTargetId();
			builder.append("\n\t").append(outputname).append(": ").append(output.getPartitioner());
		}

		builder.append("\nChained outputs: ").append(getChainedOutputs(cl));

		try {
			StreamOperator streamOperator = getStreamOperator(cl);
			builder.append("\nOperator: " + (streamOperator == null ? null : streamOperator.getClass().getSimpleName()));
		}
		catch (Throwable t) {
			builder.append("\nOperator: ").append(t.getMessage());
		}
		builder.append("\nBuffer timeout: ").append(getBufferTimeout());
		try {
			builder.append("\nTimeCharacteristic: " + getTimeCharacteristic());
		} catch (Throwable t) {
			builder.append("\nTimeCharacteristic: ").append(t.getMessage());
		}

		builder.append("\nState monitoring: ").append(isCheckpointingEnabled());
		builder.append("\nCheckpoint mode: ").append(getCheckpointMode());

		try {
			StateBackend stateBackend = getStateBackend(cl);
			builder.append("\nStateBackend: " + (stateBackend == null ? null : stateBackend.getClass().getName()));
		} catch (Throwable t) {
			builder.append("\nStateBackend: ").append(t.getMessage());
		}

		return builder.toString();
	}
}
