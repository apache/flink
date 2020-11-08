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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPool;
import org.apache.flink.runtime.jobmaster.slotpool.TestingSlotPoolImpl;
import org.apache.flink.runtime.scheduler.strategy.EagerSchedulingStrategy;
import org.apache.flink.runtime.scheduler.strategy.LazyFromSourcesSchedulingStrategy;
import org.apache.flink.runtime.scheduler.strategy.PipelinedRegionSchedulingStrategy;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.apache.flink.core.testutils.FlinkMatchers.containsMessage;
import static org.apache.flink.runtime.jobgraph.ScheduleMode.EAGER;
import static org.apache.flink.runtime.jobgraph.ScheduleMode.LAZY_FROM_SOURCES_WITH_BATCH_SLOT_REQUEST;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests for the factory method {@link DefaultSchedulerComponents#createSchedulerComponents(
 * ScheduleMode, boolean, Configuration, SlotPool, Time)}.
 */
public class DefaultSchedulerComponentsFactoryTest extends TestLogger {

	@Test
	public void testCreatingPipelinedSchedulingStrategyFactory() {
		final Configuration configuration = new Configuration();
		configuration.setString(JobManagerOptions.SCHEDULING_STRATEGY, "region");

		final DefaultSchedulerComponents components = createSchedulerComponents(configuration);
		assertThat(components.getSchedulingStrategyFactory(), instanceOf(PipelinedRegionSchedulingStrategy.Factory.class));
		assertThat(components.getAllocatorFactory(), instanceOf(SlotSharingExecutionSlotAllocatorFactory.class));
	}

	@Test
	public void testCreatingLegacySchedulingStrategyFactory() {
		final Configuration configuration = new Configuration();
		configuration.setString(JobManagerOptions.SCHEDULING_STRATEGY, "legacy");

		final DefaultSchedulerComponents components = createSchedulerComponents(configuration);
		assertThat(components.getSchedulingStrategyFactory(), instanceOf(LazyFromSourcesSchedulingStrategy.Factory.class));
		assertThat(components.getAllocatorFactory(), instanceOf(DefaultExecutionSlotAllocatorFactory.class));
	}

	@Test
	public void testCreatingPipelinedRegionSchedulingStrategyFactoryByDefault() {
		final DefaultSchedulerComponents components = createSchedulerComponents(new Configuration());
		assertThat(components.getSchedulingStrategyFactory(), instanceOf(PipelinedRegionSchedulingStrategy.Factory.class));
	}

	@Test
	public void testCreatingPipelinedRegionSchedulingStrategyFactoryWithApproximateLocalRecovery() {
		final Configuration configuration = new Configuration();
		configuration.setString(JobManagerOptions.SCHEDULING_STRATEGY, "region");

		try {
			createSchedulerComponents(configuration, true, EAGER);
			fail("expected failure");
		} catch (IllegalArgumentException e) {
			assertThat(e, containsMessage("Approximate local recovery can not be used together with PipelinedRegionScheduler for now"));
		}
	}

	@Test
	public void testCreatingLegacySchedulingStrategyFactoryWithApproximateLocalRecoveryInLazyMode() {
		final Configuration configuration = new Configuration();
		configuration.setString(JobManagerOptions.SCHEDULING_STRATEGY, "legacy");

		try {
			createSchedulerComponents(configuration, true, LAZY_FROM_SOURCES_WITH_BATCH_SLOT_REQUEST);
			fail("expected failure");
		} catch (IllegalArgumentException e) {
			assertThat(e, containsMessage("Approximate local recovery can only be used together with EAGER schedule mode"));
		}
	}

	@Test
	public void testCreatingLegacySchedulingStrategyFactoryWithApproximateLocalRecoveryInEagerMode() {
		final Configuration configuration = new Configuration();
		configuration.setString(JobManagerOptions.SCHEDULING_STRATEGY, "legacy");

		final DefaultSchedulerComponents components = createSchedulerComponents(configuration, true, EAGER);
		assertThat(components.getSchedulingStrategyFactory(), instanceOf(EagerSchedulingStrategy.Factory.class));
	}

	private static DefaultSchedulerComponents createSchedulerComponents(final Configuration configuration) {
		return createSchedulerComponents(configuration, false, LAZY_FROM_SOURCES_WITH_BATCH_SLOT_REQUEST);
	}

	private static DefaultSchedulerComponents createSchedulerComponents(
			final Configuration configuration,
			boolean iApproximateLocalRecoveryEnabled,
			ScheduleMode scheduleMode) {
		return DefaultSchedulerComponents.createSchedulerComponents(
			scheduleMode,
			iApproximateLocalRecoveryEnabled,
			configuration,
			new TestingSlotPoolImpl(new JobID()),
			Time.milliseconds(10L));
	}
}
