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
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.util.InstantiationUtil;

import java.util.List;

/**
 * An implementation of {@link Executor} that is backed by a {@link StreamExecutionEnvironment}.
 * This is the only executor that {@link org.apache.flink.table.planner.BatchPlanner} supports.
 */
@Internal
public class BatchExecutor extends ExecutorBase {

	@VisibleForTesting
	public BatchExecutor(StreamExecutionEnvironment executionEnvironment) {
		super(executionEnvironment);
	}

	@Override
	public JobExecutionResult execute(String jobName) throws Exception {
		if (transformations.isEmpty()) {
			throw new TableException("No table sinks have been created yet. " +
				"A program needs at least one sink that consumes data. ");
		}
		StreamExecutionEnvironment execEnv = getExecutionEnvironment();
		StreamGraph streamGraph = generateStreamGraph(execEnv, transformations, getNonEmptyJobName(jobName));

		// TODO supports streamEnv.execute(streamGraph)
		try {
			return execEnv.execute(getNonEmptyJobName(jobName));
		} finally {
			transformations.clear();
		}
	}

	public static StreamGraph generateStreamGraph(
		StreamExecutionEnvironment execEnv,
		List<Transformation<?>> transformations,
		String jobName) throws Exception {
		// TODO avoid cloning ExecutionConfig
		ExecutionConfig executionConfig = InstantiationUtil.clone(execEnv.getConfig());
		executionConfig.enableObjectReuse();
		executionConfig.setLatencyTrackingInterval(-1);

		return new StreamGraphGenerator(transformations, executionConfig, new CheckpointConfig())
			.setChaining(execEnv.isChainingEnabled())
			.setStateBackend(execEnv.getStateBackend())
			.setDefaultBufferTimeout(-1)
			.setTimeCharacteristic(TimeCharacteristic.ProcessingTime)
			.setUserArtifacts(execEnv.getCachedFiles())
			.setSlotSharingEnabled(false)
			.setScheduleMode(ScheduleMode.LAZY_FROM_SOURCES)
			.setJobName(jobName)
			.generate();
	}

}
