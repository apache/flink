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

package org.apache.flink.table.executor;

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
import org.apache.flink.table.api.ExecutionConfigOptions;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.plan.nodes.resource.NodeResourceUtil;

import java.util.List;

/**
 * An implementation of {@link Executor} that is backed by a {@link StreamExecutionEnvironment}.
 * This is the only executor that {@link org.apache.flink.table.planner.BatchPlanner} supports.
 */
@Internal
public class BatchExecutor extends ExecutorBase {

	private BatchExecEnvConfig batchExecEnvConfig = new BatchExecEnvConfig();

	@VisibleForTesting
	public BatchExecutor(StreamExecutionEnvironment executionEnvironment) {
		super(executionEnvironment);
	}

	@Override
	public JobExecutionResult execute(String jobName) throws Exception {
		StreamExecutionEnvironment execEnv = getExecutionEnvironment();
		StreamGraph streamGraph = generateStreamGraph(transformations, jobName);
		return execEnv.execute(streamGraph);
	}

	/**
	 * Backup previous streamEnv config and set batch configs.
	 */
	private void backupAndUpdateStreamEnv(StreamExecutionEnvironment execEnv) {
		batchExecEnvConfig.backup(execEnv);
		ExecutionConfig executionConfig = execEnv.getConfig();
		executionConfig.enableObjectReuse();
		executionConfig.setLatencyTrackingInterval(-1);
		execEnv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
		execEnv.setBufferTimeout(-1);
		if (isShuffleModeAllBatch()) {
			executionConfig.setDefaultInputDependencyConstraint(InputDependencyConstraint.ALL);
		}
	}

	/**
	 * Translates transformationList to streamGraph.
	 */
	public StreamGraph generateStreamGraph(List<Transformation<?>> transformations, String jobName) {
		StreamExecutionEnvironment execEnv = getExecutionEnvironment();
		backupAndUpdateStreamEnv(execEnv);
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
		streamGraph.setScheduleMode(ScheduleMode.LAZY_FROM_SOURCES);
		streamGraph.setStateBackend(null);
		streamGraph.getCheckpointConfig().setCheckpointInterval(Long.MAX_VALUE);
		if (isShuffleModeAllBatch()) {
			streamGraph.setBlockingConnectionsBetweenChains(true);
		}
		batchExecEnvConfig.restore(execEnv);
		return streamGraph;
	}

	private boolean isShuffleModeAllBatch() {
		String value = tableConfig.getConfiguration().getString(ExecutionConfigOptions.SQL_EXEC_SHUFFLE_MODE);
		if (value.equalsIgnoreCase(ShuffleMode.BATCH.toString())) {
			return true;
		} else if (!value.equalsIgnoreCase(ShuffleMode.PIPELINED.toString())) {
			throw new IllegalArgumentException(ExecutionConfigOptions.SQL_EXEC_SHUFFLE_MODE.key() +
					" can only be set to " + ShuffleMode.BATCH.toString() + " or " + ShuffleMode.PIPELINED.toString());
		}
		return false;
	}

	/**
	 * Batch configs that are set in {@link StreamExecutionEnvironment}. We should backup and change
	 * these configs and restore finally.
	 */
	private static class BatchExecEnvConfig {

		private boolean enableObjectReuse;
		private long latencyTrackingInterval;
		private long bufferTimeout;
		private TimeCharacteristic timeCharacteristic;
		private InputDependencyConstraint inputDependencyConstraint;

		/**
		 * Backup previous streamEnv config.
		 */
		public void backup(StreamExecutionEnvironment execEnv) {
			ExecutionConfig executionConfig = execEnv.getConfig();
			enableObjectReuse = executionConfig.isObjectReuseEnabled();
			latencyTrackingInterval = executionConfig.getLatencyTrackingInterval();
			timeCharacteristic = execEnv.getStreamTimeCharacteristic();
			bufferTimeout = execEnv.getBufferTimeout();
			inputDependencyConstraint = executionConfig.getDefaultInputDependencyConstraint();
		}

		/**
		 * Restore previous streamEnv after execute batch jobs.
		 */
		public void restore(StreamExecutionEnvironment execEnv) {
			ExecutionConfig executionConfig = execEnv.getConfig();
			if (enableObjectReuse) {
				executionConfig.enableObjectReuse();
			} else {
				executionConfig.disableObjectReuse();
			}
			executionConfig.setLatencyTrackingInterval(latencyTrackingInterval);
			execEnv.setStreamTimeCharacteristic(timeCharacteristic);
			execEnv.setBufferTimeout(bufferTimeout);
			executionConfig.setDefaultInputDependencyConstraint(inputDependencyConstraint);
		}
	}
}
