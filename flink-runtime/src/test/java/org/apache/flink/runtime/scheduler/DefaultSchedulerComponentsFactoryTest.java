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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.jobmaster.slotpool.LocationPreferenceSlotSelectionStrategy;
import org.apache.flink.runtime.jobmaster.slotpool.PreviousAllocationSlotSelectionStrategy;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPool;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPoolUtils;
import org.apache.flink.runtime.jobmaster.slotpool.SlotSelectionStrategy;
import org.apache.flink.runtime.scheduler.strategy.PipelinedRegionSchedulingStrategy;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.apache.flink.core.testutils.FlinkMatchers.containsMessage;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests for the factory method {@link DefaultSchedulerComponents#createSchedulerComponents(
 * JobType, boolean, Configuration, SlotPool, Time)}.
 */
public class DefaultSchedulerComponentsFactoryTest extends TestLogger {

    @Test
    public void testCreatingPipelinedSchedulingStrategyFactory() {

        final DefaultSchedulerComponents components =
                createSchedulerComponents(new Configuration());
        assertThat(
                components.getSchedulingStrategyFactory(),
                instanceOf(PipelinedRegionSchedulingStrategy.Factory.class));
        assertThat(
                components.getAllocatorFactory(),
                instanceOf(SlotSharingExecutionSlotAllocatorFactory.class));
    }

    @Test
    public void testCreatingPipelinedRegionSchedulingStrategyFactoryByDefault() {
        final DefaultSchedulerComponents components =
                createSchedulerComponents(new Configuration());
        assertThat(
                components.getSchedulingStrategyFactory(),
                instanceOf(PipelinedRegionSchedulingStrategy.Factory.class));
    }

    @Test
    public void testCreatingPipelinedRegionSchedulingStrategyFactoryWithApproximateLocalRecovery() {
        final Configuration configuration = new Configuration();

        try {
            createSchedulerComponents(configuration, true, JobType.STREAMING);
            fail("expected failure");
        } catch (IllegalArgumentException e) {
            assertThat(
                    e,
                    containsMessage(
                            "Approximate local recovery can not be used together with PipelinedRegionScheduler for now"));
        }
    }

    @Test
    public void testCreatePreviousAllocationSlotSelectionStrategyForLocalRecoveryStreamingJob() {
        final Configuration configuration = new Configuration();
        configuration.set(CheckpointingOptions.LOCAL_RECOVERY, true);

        final SlotSelectionStrategy slotSelectionStrategy =
                DefaultSchedulerComponents.selectSlotSelectionStrategy(
                        JobType.STREAMING, configuration);

        assertThat(
                slotSelectionStrategy, instanceOf(PreviousAllocationSlotSelectionStrategy.class));
    }

    @Test
    public void testCreateLocationPreferenceSlotSelectionStrategyForLocalRecoveryBatchJob() {
        final Configuration configuration = new Configuration();
        configuration.set(CheckpointingOptions.LOCAL_RECOVERY, true);

        final SlotSelectionStrategy slotSelectionStrategy =
                DefaultSchedulerComponents.selectSlotSelectionStrategy(
                        JobType.BATCH, configuration);

        assertThat(
                slotSelectionStrategy, instanceOf(LocationPreferenceSlotSelectionStrategy.class));
    }

    private static DefaultSchedulerComponents createSchedulerComponents(
            final Configuration configuration) {
        return createSchedulerComponents(configuration, false, JobType.BATCH);
    }

    private static DefaultSchedulerComponents createSchedulerComponents(
            final Configuration configuration,
            boolean iApproximateLocalRecoveryEnabled,
            JobType jobType) {
        return DefaultSchedulerComponents.createSchedulerComponents(
                jobType,
                iApproximateLocalRecoveryEnabled,
                configuration,
                SlotPoolUtils.createDeclarativeSlotPoolBridge(),
                Time.milliseconds(10L));
    }
}
