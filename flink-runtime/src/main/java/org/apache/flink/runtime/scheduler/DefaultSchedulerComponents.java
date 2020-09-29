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
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.executiongraph.SlotProviderStrategy;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobmaster.slotpool.LocationPreferenceSlotSelectionStrategy;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotProvider;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotProviderImpl;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotRequestBulkChecker;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotRequestBulkCheckerImpl;
import org.apache.flink.runtime.jobmaster.slotpool.PreviousAllocationSlotSelectionStrategy;
import org.apache.flink.runtime.jobmaster.slotpool.Scheduler;
import org.apache.flink.runtime.jobmaster.slotpool.SchedulerImpl;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPool;
import org.apache.flink.runtime.jobmaster.slotpool.SlotSelectionStrategy;
import org.apache.flink.runtime.scheduler.strategy.EagerSchedulingStrategy;
import org.apache.flink.runtime.scheduler.strategy.LazyFromSourcesSchedulingStrategy;
import org.apache.flink.runtime.scheduler.strategy.PipelinedRegionSchedulingStrategy;
import org.apache.flink.runtime.scheduler.strategy.SchedulingStrategyFactory;
import org.apache.flink.util.clock.SystemClock;

import java.util.function.Consumer;

/**
 * Components to create a {@link DefaultScheduler} which depends on the
 * configured {@link JobManagerOptions#SCHEDULING_STRATEGY}.
 */
public class DefaultSchedulerComponents {

	private static final String PIPELINED_REGION_SCHEDULING = "region";
	private static final String LEGACY_SCHEDULING = "legacy";

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

	SchedulingStrategyFactory getSchedulingStrategyFactory() {
		return schedulingStrategyFactory;
	}

	Consumer<ComponentMainThreadExecutor> getStartUpAction() {
		return startUpAction;
	}

	ExecutionSlotAllocatorFactory getAllocatorFactory() {
		return allocatorFactory;
	}

	static DefaultSchedulerComponents createSchedulerComponents(
			final ScheduleMode scheduleMode,
			final Configuration jobMasterConfiguration,
			final SlotPool slotPool,
			final Time slotRequestTimeout) {

		final String schedulingStrategy = jobMasterConfiguration.getString(JobManagerOptions.SCHEDULING_STRATEGY);
		switch (schedulingStrategy) {
			case PIPELINED_REGION_SCHEDULING:
				return createPipelinedRegionSchedulerComponents(
					scheduleMode,
					jobMasterConfiguration,
					slotPool,
					slotRequestTimeout);
			case LEGACY_SCHEDULING:
				return createLegacySchedulerComponents(
					scheduleMode,
					jobMasterConfiguration,
					slotPool,
					slotRequestTimeout);
			default:
				throw new IllegalStateException("Unsupported scheduling strategy " + schedulingStrategy);
		}
	}

	private static DefaultSchedulerComponents createLegacySchedulerComponents(
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
			createLegacySchedulingStrategyFactory(scheduleMode),
			scheduler::start,
			new DefaultExecutionSlotAllocatorFactory(slotProviderStrategy));
	}

	private static SchedulingStrategyFactory createLegacySchedulingStrategyFactory(final ScheduleMode scheduleMode) {
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
		final PhysicalSlotProvider physicalSlotProvider = new PhysicalSlotProviderImpl(slotSelectionStrategy, slotPool);
		final ExecutionSlotAllocatorFactory allocatorFactory = new SlotSharingExecutionSlotAllocatorFactory(
			physicalSlotProvider,
			scheduleMode != ScheduleMode.LAZY_FROM_SOURCES_WITH_BATCH_SLOT_REQUEST,
			bulkChecker,
			slotRequestTimeout);
		return new DefaultSchedulerComponents(
			new PipelinedRegionSchedulingStrategy.Factory(),
			bulkChecker::start,
			allocatorFactory);
	}

	private static SlotSelectionStrategy selectSlotSelectionStrategy(final Configuration configuration) {
		final boolean evenlySpreadOutSlots = configuration.getBoolean(ClusterOptions.EVENLY_SPREAD_OUT_SLOTS_STRATEGY);

		final SlotSelectionStrategy locationPreferenceSlotSelectionStrategy;

		locationPreferenceSlotSelectionStrategy = evenlySpreadOutSlots ?
			LocationPreferenceSlotSelectionStrategy.createEvenlySpreadOut() :
			LocationPreferenceSlotSelectionStrategy.createDefault();

		return configuration.getBoolean(CheckpointingOptions.LOCAL_RECOVERY) ?
			PreviousAllocationSlotSelectionStrategy.create(locationPreferenceSlotSelectionStrategy) :
			locationPreferenceSlotSelectionStrategy;
	}
}
