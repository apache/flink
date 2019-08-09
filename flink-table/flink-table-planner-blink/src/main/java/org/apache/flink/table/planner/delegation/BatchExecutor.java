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

package org.apache.flink.table.planner.delegation;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.InputDependencyConstraint;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.transformations.ShuffleMode;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.planner.plan.nodes.resource.NodeResourceUtil;

import java.util.List;

/**
 * An implementation of {@link Executor} that is backed by a {@link StreamExecutionEnvironment}.
 * This is the only executor that {@link org.apache.flink.table.planner.delegation.BatchPlanner} supports.
 */
@Internal
public class BatchExecutor extends ExecutorBase {

	@VisibleForTesting
	public BatchExecutor(StreamExecutionEnvironment executionEnvironment) {
		super(executionEnvironment);
	}

	@Override
	public JobExecutionResult execute(String jobName) throws Exception {
		StreamExecutionEnvironment execEnv = getExecutionEnvironment();
		StreamGraph streamGraph = generateStreamGraph(jobName);
		return execEnv.execute(streamGraph);
	}

	/**
	 * Sets batch configs.
	 */
	private void setBatchProperties(StreamExecutionEnvironment execEnv) {
		ExecutionConfig executionConfig = execEnv.getConfig();
		executionConfig.enableObjectReuse();
		executionConfig.setLatencyTrackingInterval(-1);
		execEnv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
		execEnv.setBufferTimeout(-1);
		if (isShuffleModeAllBatch()) {
			executionConfig.setDefaultInputDependencyConstraint(InputDependencyConstraint.ALL);
		}
	}

	@Override
	public StreamGraph generateStreamGraph(List<Transformation<?>> transformations, String jobName) {
		StreamExecutionEnvironment execEnv = getExecutionEnvironment();
		setBatchProperties(execEnv);
		transformations.forEach(execEnv::addOperator);
		StreamGraph streamGraph;
		streamGraph = execEnv.getStreamGraph(getNonEmptyJobName(jobName));
		// All transformations should set managed memory size.
		ResourceSpec managedResourceSpec = NodeResourceUtil.fromManagedMem(0);
		streamGraph.getStreamNodes().forEach(sn -> {
			if (sn.getMinResources().equals(ResourceSpec.DEFAULT)) {
				sn.setResources(managedResourceSpec, managedResourceSpec);
			}
		});
		streamGraph.setChaining(true);
		streamGraph.setScheduleMode(ScheduleMode.LAZY_FROM_SOURCES_WITH_BATCH_SLOT_REQUEST);
		streamGraph.setStateBackend(null);
		if (streamGraph.getCheckpointConfig().isCheckpointingEnabled()) {
			throw new IllegalArgumentException("Checkpoint is not supported for batch jobs.");
		}
		if (isShuffleModeAllBatch()) {
			streamGraph.setBlockingConnectionsBetweenChains(true);
		}
		return streamGraph;
	}

	private boolean isShuffleModeAllBatch() {
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
