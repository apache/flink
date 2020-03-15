/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategyFactory;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.BackPressureStatsTracker;
import org.apache.flink.runtime.shuffle.ShuffleMaster;

import org.slf4j.Logger;

import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

/**
 * A scheduler that delegates to the scheduling logic in the {@link ExecutionGraph}.
 *
 * @see ExecutionGraph#scheduleForExecution()
 */
public class LegacyScheduler extends SchedulerBase {

	public LegacyScheduler(
			final Logger log,
			final JobGraph jobGraph,
			final BackPressureStatsTracker backPressureStatsTracker,
			final Executor ioExecutor,
			final Configuration jobMasterConfiguration,
			final SlotProvider slotProvider,
			final ScheduledExecutorService futureExecutor,
			final ClassLoader userCodeLoader,
			final CheckpointRecoveryFactory checkpointRecoveryFactory,
			final Time rpcTimeout,
			final RestartStrategyFactory restartStrategyFactory,
			final BlobWriter blobWriter,
			final JobManagerJobMetricGroup jobManagerJobMetricGroup,
			final Time slotRequestTimeout,
			final ShuffleMaster<?> shuffleMaster,
			final JobMasterPartitionTracker partitionTracker) throws Exception {

		super(
			log,
			jobGraph,
			backPressureStatsTracker,
			ioExecutor,
			jobMasterConfiguration,
			slotProvider,
			futureExecutor,
			userCodeLoader,
			checkpointRecoveryFactory,
			rpcTimeout,
			restartStrategyFactory,
			blobWriter,
			jobManagerJobMetricGroup,
			slotRequestTimeout,
			shuffleMaster,
			partitionTracker,
			new ExecutionVertexVersioner(),
			true);
	}

	@Override
	protected long getNumberOfRestarts() {
		return getExecutionGraph().getNumberOfRestarts();
	}

	@Override
	protected void startSchedulingInternal() {
		final ExecutionGraph executionGraph = getExecutionGraph();
		try {
			executionGraph.scheduleForExecution();
		}
		catch (Throwable t) {
			executionGraph.failGlobal(t);
		}
	}

	@Override
	public void handleGlobalFailure(Throwable cause) {
		throw new IllegalStateException("Unexpected handleGlobalFailure(...) call");
	}
}
