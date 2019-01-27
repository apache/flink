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
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * In-memory objects cache for {@link StreamTaskConfig} read-write.
 */
@Internal
public class StreamTaskConfigCache implements StreamTaskConfigSnapshot {

	private final ClassLoader classLoader;

	// ------------------------------------------------------------------------
	//  Objects cache
	// ------------------------------------------------------------------------
	private TimeCharacteristic timeChar;

	private Map<Integer, StreamConfig> chainedNodeConfigMap;
	private List<Integer> chainedHeadNodeIds;
	private List<StreamEdge> inStreamEdgesOfChain;
	private List<StreamEdge> outStreamEdgesOfChain;

	private boolean isCheckpointingEnabled;
	private CheckpointingMode checkpointMode;
	private StateBackend stateBackend;

	public StreamTaskConfigCache(ClassLoader classLoader) {
		this(null, classLoader);
	}

	private StreamTaskConfigCache(StreamTaskConfig config, ClassLoader classLoader) {
		this.classLoader = checkNotNull(classLoader);

		if (config != null) {
			this.timeChar = config.getTimeCharacteristic();

			this.chainedNodeConfigMap = Collections.unmodifiableMap(config.getChainedNodeConfigs(this.classLoader));
			this.chainedHeadNodeIds = Collections.unmodifiableList(config.getChainedHeadNodeIds(this.classLoader));
			this.inStreamEdgesOfChain = Collections.unmodifiableList(config.getInStreamEdgesOfChain(this.classLoader));
			this.outStreamEdgesOfChain = Collections.unmodifiableList(config.getOutStreamEdgesOfChain(this.classLoader));

			this.isCheckpointingEnabled = config.isCheckpointingEnabled();
			this.checkpointMode = config.getCheckpointMode();
			this.stateBackend = config.getStateBackend(this.classLoader);
		}
	}

	public void clear() {
		timeChar = null;

		chainedNodeConfigMap = null;
		chainedHeadNodeIds = null;
		inStreamEdgesOfChain = null;
		outStreamEdgesOfChain = null;

		isCheckpointingEnabled = false;
		checkpointMode = null;
		stateBackend = null;
	}

	public void serializeTo(StreamTaskConfig config) {
		checkNotNull(config);

		if (timeChar != null) {
			config.setTimeCharacteristic(timeChar);
		}

		config.setChainedNodeConfigs(chainedNodeConfigMap);
		config.setChainedHeadNodeIds(chainedHeadNodeIds);
		config.setInStreamEdgesOfChain(inStreamEdgesOfChain);
		config.setOutStreamEdgesOfChain(outStreamEdgesOfChain);

		config.setCheckpointingEnabled(isCheckpointingEnabled);
		config.setCheckpointMode(checkpointMode);
		config.setStateBackend(stateBackend);
	}

	public static StreamTaskConfigCache deserializeFrom(StreamTaskConfig config, ClassLoader classLoader) {
		return new StreamTaskConfigCache(checkNotNull(config), classLoader);
	}

	// ------------------------------------------------------------------------
	//  Configured Properties
	// ------------------------------------------------------------------------

	public void setTimeCharacteristic(TimeCharacteristic characteristic) {
		this.timeChar = characteristic;
	}

	@Override
	public TimeCharacteristic getTimeCharacteristic() {
		return this.timeChar;
	}

	// ------------------------------------------------------------------------
	//  Chaining
	// ------------------------------------------------------------------------

	public void setInStreamEdgesOfChain(List<StreamEdge> inEdges) {
		this.inStreamEdgesOfChain = inEdges;
	}

	@Override
	public List<StreamEdge> getInStreamEdgesOfChain() {
		return this.inStreamEdgesOfChain;
	}

	public void setOutStreamEdgesOfChain(List<StreamEdge> outEdges) {
		this.outStreamEdgesOfChain = outEdges;
	}

	@Override
	public List<StreamEdge> getOutStreamEdgesOfChain() {
		return this.outStreamEdgesOfChain;
	}

	public void setChainedNodeConfigs(Map<Integer, StreamConfig> chainedConfigMap) {
		this.chainedNodeConfigMap = chainedConfigMap;
	}

	@Override
	public Map<Integer, StreamConfig> getChainedNodeConfigs() {
		return this.chainedNodeConfigMap;
	}

	public void setChainedHeadNodeIds(List<Integer> headNodeIds) {
		this.chainedHeadNodeIds = headNodeIds;
	}

	@Override
	public List<Integer> getChainedHeadNodeIds()  {
		return this.chainedHeadNodeIds;
	}

	@Override
	public List<StreamConfig> getChainedHeadNodeConfigs() {
		return Collections.unmodifiableList(
			StreamTaskConfig.getStreamConfigOfHeadNodes(getChainedHeadNodeIds(), getChainedNodeConfigs()));
	}

	// ------------------------------------------------------------------------
	//  Checkpointing
	// ------------------------------------------------------------------------

	public void setCheckpointingEnabled(boolean enabled) {
		this.isCheckpointingEnabled = enabled;
	}

	@Override
	public boolean isCheckpointingEnabled() {
		return this.isCheckpointingEnabled;
	}

	public void setCheckpointMode(CheckpointingMode mode) {
		this.checkpointMode = mode;
	}

	@Override
	public CheckpointingMode getCheckpointMode() {
		return this.checkpointMode;
	}

	public void setStateBackend(StateBackend backend) {
		this.stateBackend = backend;
	}

	@Override
	public StateBackend getStateBackend() {
		return this.stateBackend;
	}

	@Override
	public String toString() {
		return "TimeCharacteristic: " + timeChar +
			", chainedNodeConfigMap: " + chainedNodeConfigMap +
			", chainedHeadNodeIds: " + chainedHeadNodeIds +
			", inStreamEdgesOfChain: " + inStreamEdgesOfChain +
			", outStreamEdgesOfChain: " + outStreamEdgesOfChain +
			", isCheckpointingEnabled: " + isCheckpointingEnabled +
			", checkpointMode: " + checkpointMode +
			", stateBackend: " + stateBackend;
	}
}
