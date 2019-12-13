/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.utils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.InputDependencyConstraint;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.flink.streaming.api.transformations.ShuffleMode;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;

import java.util.List;

/**
 * Utility class to generate StreamGraph and set properties for batch.
 */
public class ExecutorUtils {

	/**
	 * Generate {@link StreamGraph} by {@link StreamGraphGenerator}.
	 */
	public static StreamGraph generateStreamGraph(
			StreamExecutionEnvironment execEnv,
			List<Transformation<?>> transformations) {
		if (transformations.size() <= 0) {
			throw new IllegalStateException("No operators defined in streaming topology. Cannot generate StreamGraph.");
		}
		StreamGraphGenerator generator =
				new StreamGraphGenerator(transformations, execEnv.getConfig(), execEnv.getCheckpointConfig())
						.setStateBackend(execEnv.getStateBackend())
						.setChaining(execEnv.isChainingEnabled())
						.setUserArtifacts(execEnv.getCachedFiles())
						.setTimeCharacteristic(execEnv.getStreamTimeCharacteristic())
						.setDefaultBufferTimeout(execEnv.getBufferTimeout());
		return generator.generate();
	}

	/**
	 * Sets batch properties for {@link StreamExecutionEnvironment}.
	 */
	public static void setBatchProperties(StreamExecutionEnvironment execEnv, TableConfig tableConfig) {
		ExecutionConfig executionConfig = execEnv.getConfig();
		executionConfig.enableObjectReuse();
		executionConfig.setLatencyTrackingInterval(-1);
		execEnv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
		execEnv.setBufferTimeout(-1);
		if (isShuffleModeAllBatch(tableConfig)) {
			executionConfig.setDefaultInputDependencyConstraint(InputDependencyConstraint.ALL);
		}
	}

	/**
	 * Sets batch properties for {@link StreamGraph}.
	 */
	public static void setBatchProperties(StreamGraph streamGraph, TableConfig tableConfig) {
		streamGraph.getStreamNodes().forEach(
				sn -> sn.setResources(ResourceSpec.UNKNOWN, ResourceSpec.UNKNOWN));
		streamGraph.setChaining(true);
		streamGraph.setAllVerticesInSameSlotSharingGroupByDefault(false);
		streamGraph.setScheduleMode(ScheduleMode.LAZY_FROM_SOURCES_WITH_BATCH_SLOT_REQUEST);
		streamGraph.setStateBackend(null);
		if (streamGraph.getCheckpointConfig().isCheckpointingEnabled()) {
			throw new IllegalArgumentException("Checkpoint is not supported for batch jobs.");
		}
		if (ExecutorUtils.isShuffleModeAllBatch(tableConfig)) {
			streamGraph.setBlockingConnectionsBetweenChains(true);
		}
	}

	private static boolean isShuffleModeAllBatch(TableConfig tableConfig) {
		String value = tableConfig.getConfiguration().getString(ExecutionConfigOptions.TABLE_EXEC_SHUFFLE_MODE);
		if (value.equalsIgnoreCase(ShuffleMode.BATCH.toString())) {
			return true;
		} else if (!value.equalsIgnoreCase(ShuffleMode.PIPELINED.toString())) {
			throw new IllegalArgumentException(ExecutionConfigOptions.TABLE_EXEC_SHUFFLE_MODE.key() +
					" can only be set to " + ShuffleMode.BATCH.toString() + " or " + ShuffleMode.PIPELINED.toString());
		}
		return false;
	}
}
