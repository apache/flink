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
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPool;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPoolUtils;
import org.apache.flink.runtime.scheduler.strategy.PipelinedRegionSchedulingStrategy;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the factory method {@link DefaultSchedulerComponents#createSchedulerComponents(
 * JobType, boolean, Configuration, SlotPool, Time)}.
 */
class DefaultSchedulerComponentsFactoryTest {

    @Test
    void testCreatingPipelinedSchedulingStrategyFactory() {

        final DefaultSchedulerComponents components =
                createSchedulerComponents(new Configuration());
        assertThat(components.getSchedulingStrategyFactory())
                .isInstanceOf(PipelinedRegionSchedulingStrategy.Factory.class);
        assertThat(components.getAllocatorFactory())
                .isInstanceOf(SlotSharingExecutionSlotAllocatorFactory.class);
    }

    @Test
    void testCreatingPipelinedRegionSchedulingStrategyFactoryByDefault() {
        final DefaultSchedulerComponents components =
                createSchedulerComponents(new Configuration());
        assertThat(components.getSchedulingStrategyFactory())
                .isInstanceOf(PipelinedRegionSchedulingStrategy.Factory.class);
    }

    @Test
    void testCreatingPipelinedRegionSchedulingStrategyFactoryWithApproximateLocalRecovery() {
        final Configuration configuration = new Configuration();

        assertThatThrownBy(() -> createSchedulerComponents(configuration, true, JobType.STREAMING))
                .withFailMessage("expected failure")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Approximate local recovery can not be used together with PipelinedRegionScheduler for now");
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
