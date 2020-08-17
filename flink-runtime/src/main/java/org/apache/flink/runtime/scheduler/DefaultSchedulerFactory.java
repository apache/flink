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
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ScheduledExecutorServiceAdapter;
import org.apache.flink.runtime.executiongraph.SlotProviderStrategy;
import org.apache.flink.runtime.executiongraph.failover.flip1.FailoverStrategyFactoryLoader;
import org.apache.flink.runtime.executiongraph.failover.flip1.RestartBackoffTimeStrategy;
import org.apache.flink.runtime.executiongraph.failover.flip1.RestartBackoffTimeStrategyFactoryLoader;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobmaster.ExecutionDeploymentTracker;
import org.apache.flink.runtime.jobmaster.slotpool.BulkSlotProvider;
import org.apache.flink.runtime.jobmaster.slotpool.BulkSlotProviderImpl;
import org.apache.flink.runtime.jobmaster.slotpool.LocationPreferenceSlotSelectionStrategy;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotRequestBulkChecker;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotRequestBulkCheckerImpl;
import org.apache.flink.runtime.jobmaster.slotpool.PreviousAllocationSlotSelectionStrategy;
import org.apache.flink.runtime.jobmaster.slotpool.Scheduler;
import org.apache.flink.runtime.jobmaster.slotpool.SchedulerImpl;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPool;
import org.apache.flink.runtime.jobmaster.slotpool.SlotSelectionStrategy;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.BackPressureStatsTracker;
import org.apache.flink.runtime.scheduler.strategy.EagerSchedulingStrategy;
import org.apache.flink.runtime.scheduler.strategy.LazyFromSourcesSchedulingStrategy;
import org.apache.flink.runtime.scheduler.strategy.PipelinedRegionSchedulingStrategy;
import org.apache.flink.runtime.scheduler.strategy.SchedulingStrategyFactory;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.util.clock.SystemClock;

import org.slf4j.Logger;

import javax.annotation.Nonnull;

import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;

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
			final SlotPool slotPool,
			final ScheduledExecutorService futureExecutor,
			final ClassLoader userCodeLoader,
			final CheckpointRecoveryFactory checkpointRecoveryFactory,
			final Time rpcTimeout,
			final BlobWriter blobWriter,
			final JobManagerJobMetricGroup jobManagerJobMetricGroup,
			final Time slotRequestTimeout,
			final ShuffleMaster<?> shuffleMaster,
			final JobMasterPartitionTracker partitionTracker,
			final ExecutionDeploymentTracker executionDeploymentTracker) throws Exception {

		final DefaultSchedulerComponents schedulerComponents = createDefaultSchedulerComponents(
			jobGraph.getScheduleMode(),
			jobMasterConfiguration,
			slotPool,
			slotRequestTimeout);
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

		return new DefaultScheduler(
			log,
			jobGraph,
			backPressureStatsTracker,
			ioExecutor,
			jobMasterConfiguration,
			schedulerComponents.startUpAction,
			futureExecutor,
			new ScheduledExecutorServiceAdapter(futureExecutor),
			userCodeLoader,
			checkpointRecoveryFactory,
			rpcTimeout,
			blobWriter,
			jobManagerJobMetricGroup,
			shuffleMaster,
			partitionTracker,
			schedulerComponents.schedulingStrategyFactory,
			FailoverStrategyFactoryLoader.loadFailoverStrategyFactory(jobMasterConfiguration),
			restartBackoffTimeStrategy,
			new DefaultExecutionVertexOperations(),
			new ExecutionVertexVersioner(),
			schedulerComponents.allocatorFactory,
			executionDeploymentTracker);
	}

	private static DefaultSchedulerComponents createDefaultSchedulerComponents(
			final ScheduleMode scheduleMode,
			final Configuration jobMasterConfiguration,
			final SlotPool slotPool,
			final Time slotRequestTimeout) {
		final SlotSelectionStrategy slotSelectionStrategy = selectSlotSelectionStrategy(jobMasterConfiguration);
		final Scheduler scheduler = new SchedulerImpl(slotSelectionStrategy, slotPool);
		final SlotProviderStrategy slotProviderStrategy = SlotProviderStrategy.from(
			scheduleMode,
			scheduler,
			slotRequestTimeout);
		return new DefaultSchedulerComponents(
			createSchedulingStrategyFactory(scheduleMode),
			scheduler::start,
			new DefaultExecutionSlotAllocatorFactory(slotProviderStrategy));
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

	private static DefaultSchedulerComponents createPipelinedRegionSchedulerComponents(
			final ScheduleMode scheduleMode,
			final Configuration jobMasterConfiguration,
			final SlotPool slotPool,
			final Time slotRequestTimeout) {
		final SlotSelectionStrategy slotSelectionStrategy = selectSlotSelectionStrategy(jobMasterConfiguration);
		final PhysicalSlotRequestBulkChecker bulkChecker = PhysicalSlotRequestBulkCheckerImpl
			.createFromSlotPool(slotPool, SystemClock.getInstance());
		final BulkSlotProvider bulkSlotProvider = new BulkSlotProviderImpl(slotSelectionStrategy, slotPool, bulkChecker);
		final ExecutionSlotAllocatorFactory allocatorFactory = new OneSlotPerExecutionSlotAllocatorFactory(
			bulkSlotProvider,
			scheduleMode != ScheduleMode.LAZY_FROM_SOURCES_WITH_BATCH_SLOT_REQUEST,
			slotRequestTimeout);
		return new DefaultSchedulerComponents(
			new PipelinedRegionSchedulingStrategy.Factory(),
			bulkChecker::start,
			allocatorFactory);
	}

	private static SlotSelectionStrategy selectSlotSelectionStrategy(@Nonnull Configuration configuration) {
		final boolean evenlySpreadOutSlots = configuration.getBoolean(ClusterOptions.EVENLY_SPREAD_OUT_SLOTS_STRATEGY);

		final SlotSelectionStrategy locationPreferenceSlotSelectionStrategy;

		locationPreferenceSlotSelectionStrategy = evenlySpreadOutSlots ?
			LocationPreferenceSlotSelectionStrategy.createEvenlySpreadOut() :
			LocationPreferenceSlotSelectionStrategy.createDefault();

		return configuration.getBoolean(CheckpointingOptions.LOCAL_RECOVERY) ?
			PreviousAllocationSlotSelectionStrategy.create(locationPreferenceSlotSelectionStrategy) :
			locationPreferenceSlotSelectionStrategy;
	}

	private static class DefaultSchedulerComponents {
		private final SchedulingStrategyFactory schedulingStrategyFactory;
		private final Consumer<ComponentMainThreadExecutor> startUpAction;
		private final ExecutionSlotAllocatorFactory allocatorFactory;

		private DefaultSchedulerComponents(
				final SchedulingStrategyFactory schedulingStrategyFactory,
				final Consumer<ComponentMainThreadExecutor> startUpAction,
				final ExecutionSlotAllocatorFactory allocatorFactory) {
			this.schedulingStrategyFactory = schedulingStrategyFactory;
			this.startUpAction = startUpAction;
			this.allocatorFactory = allocatorFactory;
		}
	}
}
