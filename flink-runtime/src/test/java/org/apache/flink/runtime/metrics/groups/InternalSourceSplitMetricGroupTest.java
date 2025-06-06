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

package org.apache.flink.runtime.metrics.groups;

import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.util.TestingMetricRegistry;
import org.apache.flink.util.clock.ManualClock;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link InternalSourceSplitMetricGroup}. */
class InternalSourceSplitMetricGroupTest {
    private static final MetricRegistry registry = TestingMetricRegistry.builder().build();

    InternalSourceSplitMetricGroup getMetricGroupWithClock(ManualClock clock) {
        return InternalSourceSplitMetricGroup.wrap(
                UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup(),
                clock,
                "splitId",
                null);
    }

    @Test
    void testClocksStartTickingAfterSplitStarted() throws InterruptedException {
        ManualClock clock = new ManualClock(System.currentTimeMillis());
        InternalSourceSplitMetricGroup metricGroup = getMetricGroupWithClock(clock);
        long timeBeforeSplitStart = 4L;
        clock.advanceTime(Duration.ofMillis(timeBeforeSplitStart));
        // assert clocks aren't ticking before the split has started:
        assertThat(
                        metricGroup.getAccumulatedIdleTime()
                                + metricGroup.getAccumulatedPausedTime()
                                + metricGroup.getAccumulatedActiveTime())
                .isEqualTo(0);

        // start split
        metricGroup.markSplitStart();
        long timeAfterSplitStart = 6L;
        clock.advanceTime(Duration.ofMillis(timeAfterSplitStart));
        assertThat(
                        metricGroup.getAccumulatedIdleTime()
                                + metricGroup.getAccumulatedPausedTime()
                                + metricGroup.getAccumulatedActiveTime())
                .isEqualTo(timeAfterSplitStart);
    }

    @Test
    void testConsistencyOfTime() throws InterruptedException {
        ManualClock clock = new ManualClock(System.currentTimeMillis());
        InternalSourceSplitMetricGroup metricGroup = getMetricGroupWithClock(clock);
        metricGroup.markSplitStart();
        final long startTime = clock.absoluteTimeMillis();

        long pausedTime = 2L;
        metricGroup.markPaused();
        clock.advanceTime(Duration.ofMillis(pausedTime));
        metricGroup.markNotPaused();

        long activeTime = 4L;
        clock.advanceTime(Duration.ofMillis(activeTime));

        long idleTime = 1000 - pausedTime - activeTime;
        metricGroup.markIdle();
        clock.advanceTime(Duration.ofMillis(idleTime));
        metricGroup.markNotIdle();

        assertThat(metricGroup.getAccumulatedPausedTime()).isEqualTo(pausedTime);
        assertThat(metricGroup.getAccumulatedActiveTime()).isEqualTo(activeTime);
        assertThat(metricGroup.getAccumulatedIdleTime()).isEqualTo(idleTime);

        long totalDuration = clock.absoluteTimeMillis() - startTime;
        assertThat((double) metricGroup.getAccumulatedPausedTime())
                .isEqualTo(
                        totalDuration
                                - metricGroup.getAccumulatedActiveTime()
                                - metricGroup.getAccumulatedIdleTime());
    }
}
