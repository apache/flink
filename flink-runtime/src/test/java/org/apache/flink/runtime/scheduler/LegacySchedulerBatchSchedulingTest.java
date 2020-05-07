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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.VoidBlobWriter;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointRecoveryFactory;
import org.apache.flink.runtime.executiongraph.restart.NoRestartStrategy;
import org.apache.flink.runtime.io.network.partition.NoOpJobMasterPartitionTracker;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.VoidBackPressureStatsTracker;
import org.apache.flink.runtime.shuffle.NettyShuffleMaster;
import org.apache.flink.runtime.testingUtils.TestingUtils;

/**
 * Tests for the scheduling of batch jobs with {@link LegacyScheduler}.
 */
public class LegacySchedulerBatchSchedulingTest extends BatchSchedulingTestBase {

	@Override
	protected LegacyScheduler createScheduler(
			final JobGraph jobGraph,
			final SlotProvider slotProvider,
			final Time slotRequestTimeout) throws Exception {

		final LegacyScheduler legacyScheduler = new LegacyScheduler(
			log,
			jobGraph,
			VoidBackPressureStatsTracker.INSTANCE,
			TestingUtils.defaultExecutor(),
			new Configuration(),
			slotProvider,
			TestingUtils.defaultExecutor(),
			getClass().getClassLoader(),
			new StandaloneCheckpointRecoveryFactory(),
			TestingUtils.TIMEOUT(),
			new NoRestartStrategy.NoRestartStrategyFactory(),
			VoidBlobWriter.getInstance(),
			UnregisteredMetricGroups.createUnregisteredJobManagerJobMetricGroup(),
			slotRequestTimeout,
			NettyShuffleMaster.INSTANCE,
			NoOpJobMasterPartitionTracker.INSTANCE);

		return legacyScheduler;
	}
}
