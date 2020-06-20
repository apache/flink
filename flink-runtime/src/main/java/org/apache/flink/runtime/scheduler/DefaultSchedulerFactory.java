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
import org.apache.flink.runtime.concurrent.ScheduledExecutorServiceAdapter;
import org.apache.flink.runtime.executiongraph.SlotProviderStrategy;
import org.apache.flink.runtime.executiongraph.failover.flip1.FailoverStrategyFactoryLoader;
import org.apache.flink.runtime.executiongraph.failover.flip1.RestartBackoffTimeStrategy;
import org.apache.flink.runtime.executiongraph.failover.flip1.RestartBackoffTimeStrategyFactoryLoader;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.BackPressureStatsTracker;
import org.apache.flink.runtime.scheduler.strategy.EagerSchedulingStrategy;
import org.apache.flink.runtime.scheduler.strategy.LazyFromSourcesSchedulingStrategy;
import org.apache.flink.runtime.scheduler.strategy.PipelinedRegionSchedulingStrategy;
import org.apache.flink.runtime.scheduler.strategy.SchedulingStrategyFactory;
import org.apache.flink.runtime.shuffle.ShuffleMaster;

import org.slf4j.Logger;

import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Factory for {@link DefaultScheduler}.
 */
public class DefaultSchedulerFactory implements SchedulerNGFactory {

	@Override
	public SchedulerNG createInstance(
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
			final BlobWriter blobWriter,
			final JobManagerJobMetricGroup jobManagerJobMetricGroup,
			final Time slotRequestTimeout,
			final ShuffleMaster<?> shuffleMaster,
			final JobMasterPartitionTracker partitionTracker) throws Exception {

		final SchedulingStrategyFactory schedulingStrategyFactory = createSchedulingStrategyFactory(jobGraph.getScheduleMode());
		final RestartBackoffTimeStrategy restartBackoffTimeStrategy = RestartBackoffTimeStrategyFactoryLoader
			.createRestartBackoffTimeStrategyFactory(
				jobGraph
					.getSerializedExecutionConfig()
					.deserializeValue(userCodeLoader)
					.getRestartStrategy(),
				jobMasterConfiguration,
				jobGraph.isCheckpointingEnabled())
			.create();
		log.info("Using restart back off time strategy {} for {} ({}).", restartBackoffTimeStrategy, jobGraph.getName(), jobGraph.getJobID());

		final ExecutionSlotAllocatorFactory slotAllocatorFactory =
			createExecutionSlotAllocatorFactory(
				jobGraph.getScheduleMode(),
				slotProvider,
				slotRequestTimeout,
				schedulingStrategyFactory);

		return new DefaultScheduler(
			log,
			jobGraph,
			backPressureStatsTracker,
			ioExecutor,
			jobMasterConfiguration,
			futureExecutor,
			new ScheduledExecutorServiceAdapter(futureExecutor),
			userCodeLoader,
			checkpointRecoveryFactory,
			rpcTimeout,
			blobWriter,
			jobManagerJobMetricGroup,
			shuffleMaster,
			partitionTracker,
			schedulingStrategyFactory,
			FailoverStrategyFactoryLoader.loadFailoverStrategyFactory(jobMasterConfiguration),
			restartBackoffTimeStrategy,
			new DefaultExecutionVertexOperations(),
			new ExecutionVertexVersioner(),
			slotAllocatorFactory);
	}

	static SchedulingStrategyFactory createSchedulingStrategyFactory(final ScheduleMode scheduleMode) {
		switch (scheduleMode) {
			case EAGER:
				return new EagerSchedulingStrategy.Factory();
			case LAZY_FROM_SOURCES_WITH_BATCH_SLOT_REQUEST:
			case LAZY_FROM_SOURCES:
				return new LazyFromSourcesSchedulingStrategy.Factory();
			default:
				throw new IllegalStateException("Unsupported schedule mode " + scheduleMode);
		}
	}

	private static ExecutionSlotAllocatorFactory createExecutionSlotAllocatorFactory(
			final ScheduleMode scheduleMode,
			final SlotProvider slotProvider,
			final Time slotRequestTimeout,
			final SchedulingStrategyFactory schedulingStrategyFactory) {

		if (schedulingStrategyFactory instanceof PipelinedRegionSchedulingStrategy.Factory) {
			return new OneSlotPerExecutionSlotAllocatorFactory(
				slotProvider,
				scheduleMode != ScheduleMode.LAZY_FROM_SOURCES_WITH_BATCH_SLOT_REQUEST,
				slotRequestTimeout);
		} else {
			final SlotProviderStrategy slotProviderStrategy = SlotProviderStrategy.from(
				scheduleMode,
				slotProvider,
				slotRequestTimeout);

			return new DefaultExecutionSlotAllocatorFactory(slotProviderStrategy);
		}
	}
}
