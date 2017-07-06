/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.graph;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

/**
 * Properties for StreamGraph, builds from the streamExecutionEnvironment. It only contains information need to use later.
 */
@Internal
public class StreamGraphProperties {

	private ExecutionConfig executionConfig;
	private CheckpointConfig checkpointConfig;
	private TimeCharacteristic timeCharacteristic;
	private AbstractStateBackend stateBackend;
	private boolean chainingEnabled;
	private List<Tuple2<String, DistributedCache.DistributedCacheEntry>> cachedFiles;
	private String jobName = StreamExecutionEnvironment.DEFAULT_JOB_NAME;

	public static StreamGraphProperties buildProperties(StreamExecutionEnvironment env) {
		StreamGraphProperties streamGraphProperties = new StreamGraphProperties();
		streamGraphProperties.setExecutionConfig(env.getConfig());
		streamGraphProperties.setCheckpointConfig(env.getCheckpointConfig());
		streamGraphProperties.setTimeCharacteristic(env.getStreamTimeCharacteristic());
		streamGraphProperties.setStateBackend(env.getStateBackend());
		streamGraphProperties.setChainingEnabled(env.isChainingEnabled());
		streamGraphProperties.setCachedFiles(env.getCachedFiles());
		return streamGraphProperties;
	}

	public void setExecutionConfig(ExecutionConfig executionConfig) {
		this.executionConfig = executionConfig;
	}

	public void setCheckpointConfig(CheckpointConfig checkpointConfig) {
		this.checkpointConfig = checkpointConfig;
	}

	public void setTimeCharacteristic(TimeCharacteristic timeCharacteristic) {
		this.timeCharacteristic = timeCharacteristic;
	}

	public void setStateBackend(AbstractStateBackend stateBackend) {
		this.stateBackend = stateBackend;
	}

	public void setChainingEnabled(boolean chainingEnabled) {
		this.chainingEnabled = chainingEnabled;
	}

	public void setCachedFiles(List<Tuple2<String, DistributedCache.DistributedCacheEntry>> cachedFiles) {
		this.cachedFiles = cachedFiles;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	public ExecutionConfig getExecutionConfig() {
		return executionConfig;
	}

	public CheckpointConfig getCheckpointConfig() {
		return checkpointConfig;
	}

	public TimeCharacteristic getTimeCharacteristic() {
		return timeCharacteristic;
	}

	public AbstractStateBackend getStateBackend() {
		return stateBackend;
	}

	public boolean isChainingEnabled() {
		return chainingEnabled;
	}

	public List<Tuple2<String, DistributedCache.DistributedCacheEntry>> getCachedFiles() {
		return cachedFiles;
	}

	public String getJobName() {
		return jobName;
	}
}
