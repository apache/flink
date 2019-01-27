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

import java.util.List;
import java.util.Map;

/**
 * In-memory objects snapshot to read {@link StreamTaskConfig}.
 */

@Internal
public interface StreamTaskConfigSnapshot {

	TimeCharacteristic getTimeCharacteristic();

	List<StreamEdge> getInStreamEdgesOfChain();

	List<StreamEdge> getOutStreamEdgesOfChain();

	Map<Integer, StreamConfig> getChainedNodeConfigs();

	List<Integer> getChainedHeadNodeIds();

	List<StreamConfig> getChainedHeadNodeConfigs();

	boolean isCheckpointingEnabled();

	CheckpointingMode getCheckpointMode();

	StateBackend getStateBackend();
}
