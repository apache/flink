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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.operators.util.CorruptConfigurationException;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.runtime.tasks.StreamTaskException;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Internal configuration for a {@link StreamTask}. This is created and populated by the
 * {@link StreamingJobGraphGenerator}.
 */
@Internal
public class StreamTaskConfig {

	//  ------------------------input---------------------------------

	private static final String INPUTS_NUM = "inputs.num";

	private static final String IN_PHYSICAL_EDGES = "in.physical.edges";

	// ----------------------------output-----------------------------

	private static final String OUT_EDGES_IN_ORDER = "out.edges.in.order";

	// ----------------------------chain-----------------------------

	private static final String HEAD_NODE_ID = "head.node.id";

	private static final String CHAINED_TASK_CONFIGS = "chained.task.configs";

	// ----------------------------other-----------------------------

	private static final String CHECKPOINTING_ENABLED = "checkpointing.enabled";

	private static final String CHECKPOINT_MODE = "checkpoint.mode";

	private static final String STATE_BACKEND = "state.backend";

	private static final String TIME_CHARACTERISTIC = "time.char";

	private static final String ITERATION_WAIT = "iteration.wait";

	private static final String ITERATION_ID = "iteration.id";

	private static final String BUFFER_TIMEOUT = "buffer.timeout";

	private static final long DEFAULT_TIMEOUT = 100;

	private static final CheckpointingMode DEFAULT_CHECKPOINTING_MODE = CheckpointingMode.EXACTLY_ONCE;

	private Configuration config;

	public StreamTaskConfig(Configuration configuration) {
		this.config = configuration;
	}

	public OperatorConfig getHeadOperatorConfig(ClassLoader cl) {
		Map<Integer, OperatorConfig> chainedOperatorConfigs = getChainedTaskConfigs(cl);
		int headOperatorId = config.getInteger(HEAD_NODE_ID, -1);
		if (!chainedOperatorConfigs.containsKey(headOperatorId)) {
			throw new RuntimeException("cannot find head operator config");
		}
		return chainedOperatorConfigs.get(headOperatorId);
	}

	public void setHeadNodeID(Integer headNodeId) {
		config.setInteger(HEAD_NODE_ID, headNodeId);
	}

	public int getHeadNodeID() {
		return config.getInteger(HEAD_NODE_ID, -1);
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

	// ------------------------------------------------------------------------
	//  State backend
	// ------------------------------------------------------------------------
	public void setStateBackend(AbstractStateBackend backend) {
		if (backend != null) {
			try {
				InstantiationUtil.writeObjectToConfig(backend, this.config, STATE_BACKEND);
			} catch (Exception e) {
				throw new StreamTaskException("Could not serialize stateHandle provider.", e);
			}
		}
	}

	public AbstractStateBackend getStateBackend(ClassLoader cl) {
		try {
			return InstantiationUtil.readObjectFromConfig(this.config, STATE_BACKEND, cl);
		} catch (Exception e) {
			throw new StreamTaskException("Could not instantiate stateHandle provider.", e);
		}
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

	public void setInputsNum(int inputsNum) {
		config.setInteger(INPUTS_NUM, inputsNum);
	}

	public int getInputsNum() {
		return config.getInteger(INPUTS_NUM, 0);
	}

	public void setInPhysicalEdges(List<StreamEdge> inEdges) {
		try {
			InstantiationUtil.writeObjectToConfig(inEdges, this.config, IN_PHYSICAL_EDGES);
		} catch (IOException e) {
			throw new StreamTaskException("Cannot serialize inward edges.", e);
		}
	}

	public List<StreamEdge> getInPhysicalEdges(ClassLoader cl) {
		try {
			List<StreamEdge> inEdges = InstantiationUtil.readObjectFromConfig(this.config, IN_PHYSICAL_EDGES, cl);
			return inEdges == null ? new ArrayList<StreamEdge>() : inEdges;
		} catch (Exception e) {
			throw new StreamTaskException("Could not instantiate inputs.", e);
		}
	}

	public void setChainedTaskConfigs(Map<Integer, OperatorConfig> chainedTaskConfigs) {
		try {
			InstantiationUtil.writeObjectToConfig(chainedTaskConfigs, this.config, CHAINED_TASK_CONFIGS);
		} catch (IOException e) {
			throw new StreamTaskException("Could not serialize chained task configurations.", e);
		}
	}

	public Map<Integer, OperatorConfig> getChainedTaskConfigs(ClassLoader cl) {
		try {
			Map<Integer, OperatorConfig> configs = InstantiationUtil.readObjectFromConfig(this.config, CHAINED_TASK_CONFIGS, cl);
			return configs == null ? new HashMap<Integer, OperatorConfig>() : configs;
		} catch (Exception e) {
			throw new StreamTaskException("Could not instantiate chained task configurations.", e);
		}
	}

	public void setOutEdgesInOrder(List<StreamEdge> outEdgeList) {
		try {
			InstantiationUtil.writeObjectToConfig(outEdgeList, this.config, OUT_EDGES_IN_ORDER);
		} catch (IOException e) {
			throw new StreamTaskException("Could not serialize outputs in order.", e);
		}
	}

	public List<StreamEdge> getOutEdgesInOrder(ClassLoader cl) {
		try {
			List<StreamEdge> outEdgesInOrder = InstantiationUtil.readObjectFromConfig(this.config, OUT_EDGES_IN_ORDER, cl);
			return outEdgesInOrder == null ? new ArrayList<StreamEdge>() : outEdgesInOrder;
		} catch (Exception e) {
			throw new StreamTaskException("Could not instantiate outputs in order.", e);
		}
	}

	public void setIterationId(String iterationId) {
		config.setString(ITERATION_ID, iterationId);
	}

	public String getIterationId() {
		return config.getString(ITERATION_ID, "");
	}

	public void setIterationWaitTime(long time) {
		config.setLong(ITERATION_WAIT, time);
	}

	public long getIterationWaitTime() {
		return config.getLong(ITERATION_WAIT, 0);
	}

	public void setCheckpointingEnabled(boolean enabled) {
		config.setBoolean(CHECKPOINTING_ENABLED, enabled);
	}

	public boolean isCheckpointingEnabled() {
		return config.getBoolean(CHECKPOINTING_ENABLED, false);
	}

	public void setBufferTimeout(long timeout) {
		config.setLong(BUFFER_TIMEOUT, timeout);
	}

	public long getBufferTimeout() {
		return config.getLong(BUFFER_TIMEOUT, DEFAULT_TIMEOUT);
	}

	public Configuration getConfiguration() {
		return config;
	}

	@Override
	public String toString() {
		ClassLoader cl = getClass().getClassLoader();
		StringBuilder builder = new StringBuilder();
		builder.append("\n=======================");
		builder.append("Stream Config");
		builder.append("=======================");
		builder.append("\nNumber of non-chained inputs: ").append(getInputsNum());
		builder.append("\nNumber of non-chained outputs: ").append(getOutEdgesInOrder(cl).size());
		builder.append("\nNon-chained outputs: ").append(getOutEdgesInOrder(cl));
		builder.append("\nPartitioning:");
		for (StreamEdge output : getOutEdgesInOrder(cl)) {
			int outputName = output.getTargetId();
			builder.append("\n\t").append(outputName).append(": ").append(output.getPartitioner());
		}
		builder.append("\nHead node is: ").append(getHeadNodeID());
		try {
			builder.append("\nHead operator: ").append(getHeadOperatorConfig(cl).getStreamOperator().getClass().getSimpleName());
		} catch (Exception e) {
			builder.append("\nOperator: Missing");
		}
		builder.append("\nTimeCharacteristic: ").append(getTimeCharacteristic());
		builder.append("\nBuffer timeout: ").append(getBufferTimeout());
		builder.append("\nState Monitoring: ").append(isCheckpointingEnabled());
		builder.append("\nChained subTasks num: ").append(getChainedTaskConfigs(cl).size());
		builder.append("\n\n\n---------------------\nChained task configs\n---------------------\n");
		builder.append(getChainedTaskConfigs(cl));
		return builder.toString();
	}
}
