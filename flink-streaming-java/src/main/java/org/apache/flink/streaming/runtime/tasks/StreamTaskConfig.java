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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Internal configuration for a {@link StreamTask}. This is created and populated by the
 * {@link StreamingJobGraphGenerator}.
 */
@Internal
public class StreamTaskConfig implements Serializable {

	private static final long serialVersionUID = 1L;

	// ------------------------------------------------------------------------
	//  Config Keys
	// ------------------------------------------------------------------------

	private static final String TIME_CHARACTERISTIC = "timechar";

	private static final String CHAINED_NODE_CONFIGS = "chainedNodeConfigs";
	private static final String CHAINED_HEAD_NODE_IDS = "chainedHeadNodeIds";
	private static final String CHAIN_IN_STREAM_EDGES = "chainInStreamEdges";
	private static final String CHAIN_OUT_STREAM_EDGES = "chainOutStreamEdges";

	private static final String CHECKPOINTING_ENABLED = "checkpointing";
	private static final String CHECKPOINT_MODE = "checkpointMode";

	private static final String STATE_BACKEND = "statebackend";

	// ------------------------------------------------------------------------
	//  Config
	// ------------------------------------------------------------------------

	private final Configuration config;

	public StreamTaskConfig(Configuration config) {
		this.config = config;
	}

	public Configuration getConfiguration() {
		return config;
	}

	// ------------------------------------------------------------------------
	//  Configured Properties
	// ------------------------------------------------------------------------

	public void setTimeCharacteristic(TimeCharacteristic characteristic) {
		config.setInteger(TIME_CHARACTERISTIC, characteristic.ordinal());
	}

	public TimeCharacteristic getTimeCharacteristic() {
		int ordinal = config.getInteger(TIME_CHARACTERISTIC, -1);
		if (ordinal >= 0) {
			return TimeCharacteristic.values()[ordinal];
		} else {
			return null;
		}
	}

	// ------------------------------------------------------------------------
	//  Chaining
	// ------------------------------------------------------------------------

	public void setInStreamEdgesOfChain(List<StreamEdge> inEdges) {
		try {
			InstantiationUtil.writeObjectToConfig(inEdges, this.config, CHAIN_IN_STREAM_EDGES);
		} catch (IOException e) {
			throw new StreamTaskException("Cannot serialize inward edges.", e);
		}
	}

	public List<StreamEdge> getInStreamEdgesOfChain(ClassLoader cl) {
		try {
			List<StreamEdge> inEdges = InstantiationUtil.readObjectFromConfig(this.config, CHAIN_IN_STREAM_EDGES, cl);
			return inEdges == null ? new ArrayList<>() : inEdges;
		} catch (Exception e) {
			throw new StreamTaskException("Could not instantiate inputs.", e);
		}
	}

	public void setOutStreamEdgesOfChain(List<StreamEdge> outEdgeList) {
		try {
			InstantiationUtil.writeObjectToConfig(outEdgeList, this.config, CHAIN_OUT_STREAM_EDGES);
		} catch (IOException e) {
			throw new StreamTaskException("Could not serialize outputs in order.", e);
		}
	}

	public List<StreamEdge> getOutStreamEdgesOfChain(ClassLoader cl) {
		try {
			List<StreamEdge> outEdgesInOrder = InstantiationUtil.readObjectFromConfig(this.config, CHAIN_OUT_STREAM_EDGES, cl);
			return outEdgesInOrder == null ? new ArrayList<>() : outEdgesInOrder;
		} catch (Exception e) {
			throw new StreamTaskException("Could not instantiate outputs in order.", e);
		}
	}

	public void setChainedNodeConfigs(Map<Integer, StreamConfig> chainedTaskConfigs) {
		try {
			InstantiationUtil.writeObjectToConfig(chainedTaskConfigs, this.config, CHAINED_NODE_CONFIGS);
		} catch (IOException e) {
			throw new StreamTaskException("Could not serialize configuration.", e);
		}
	}

	public Map<Integer, StreamConfig> getChainedNodeConfigs(ClassLoader cl) {
		try {
			Map<Integer, StreamConfig> confs = InstantiationUtil.readObjectFromConfig(this.config, CHAINED_NODE_CONFIGS, cl);
			return confs == null ? new HashMap<>() : confs;
		} catch (Exception e) {
			throw new StreamTaskException("Could not instantiate configuration.", e);
		}
	}

	public void setChainedHeadNodeIds(List<Integer> headNodeIds) {
		try {
			InstantiationUtil.writeObjectToConfig(headNodeIds, this.config, CHAINED_HEAD_NODE_IDS);
		} catch (IOException e) {
			throw new StreamTaskException("Cannot serialize chained head node list.", e);
		}
	}

	public List<Integer> getChainedHeadNodeIds(ClassLoader cl) {
		try {
			List<Integer> headNodeIds = InstantiationUtil.readObjectFromConfig(this.config, CHAINED_HEAD_NODE_IDS, cl);
			return headNodeIds == null ? new ArrayList<>() : headNodeIds;
		} catch (Exception e) {
			throw new StreamTaskException("Could not instantiate outputs in order.", e);
		}
	}

	public List<StreamConfig> getStreamConfigOfHeadNodes(ClassLoader cl) {
		return getStreamConfigOfHeadNodes(getChainedHeadNodeIds(cl), getChainedNodeConfigs(cl));
	}

	public static List<StreamConfig> getStreamConfigOfHeadNodes(List<Integer> headNodeIds, Map<Integer, StreamConfig> chainedConfigs) {
		if (headNodeIds == null || chainedConfigs == null) {
			return new ArrayList<>();
		}

		return headNodeIds.stream()
			.map(nodeId -> {
				if (!chainedConfigs.containsKey(nodeId)) {
					throw new StreamTaskException("Could not find StreamConfig of the node (nodeId: " + nodeId + ").");
				}

				return chainedConfigs.get(nodeId);
			}).collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
	}

	// ------------------------------------------------------------------------
	//  Checkpointing
	// ------------------------------------------------------------------------

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
			return StreamConfig.DEFAULT_CHECKPOINTING_MODE;
		}
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

	@Override
	public String toString() {
		ClassLoader cl = getClass().getClassLoader();

		List<Integer> headNodeIds = getChainedHeadNodeIds(cl);
		Map<Integer, StreamConfig> chainConfigs = getChainedNodeConfigs(cl);

		StringBuilder builder = new StringBuilder();
		builder.append("\n=======================");
		builder.append("StreamTask Config");
		builder.append("=======================");
		builder.append("\nChained subTasks num: ").append(chainConfigs.size());
		builder.append("\nNumber of non-chained outputs: ").append(getOutStreamEdgesOfChain(cl).size());
		builder.append("\nNon-chained outputs: ").append(getOutStreamEdgesOfChain(cl));
		builder.append("\nPartitioning:");
		for (StreamEdge output : getOutStreamEdgesOfChain(cl)) {
			int outputName = output.getTargetId();
			builder.append("\n\t").append(outputName).append(": ").append(output.getPartitioner());
		}
		builder.append("\nHead nodeIds: ").append(Arrays.toString(headNodeIds.toArray(new Integer[0])));
		try {
			builder.append("\nHead operators: " +
				Arrays.toString(
					getStreamConfigOfHeadNodes(headNodeIds, chainConfigs)
						.stream()
						.map(config -> config.getStreamOperator(cl).getClass().getSimpleName())
						.toArray(String[]::new)));
		} catch (Throwable t) {
			builder.append("\nHead Operators: ").append(t.getMessage());
		}

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

		builder.append("\n\n\n---------------------\nChained subTask configs\n---------------------\n");
		builder.append(chainConfigs);

		return builder.toString();
	}
}
