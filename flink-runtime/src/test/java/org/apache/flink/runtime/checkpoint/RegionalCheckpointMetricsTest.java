/*
 * Licensed to the Apache Software Foundation (ASF)
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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the regional checkpoint count metric.
 *
 * <p>Per FLIP-600, the metric is named {@code regional_checkpoint_count} (snake_case) and is a
 * {@link Counter} (not a {@link org.apache.flink.metrics.Gauge}).
 */
class RegionalCheckpointMetricsTest {

    @Test
    void testRegionalCheckpointCounterIncrements() {
        final Map<String, Counter> registeredCounters = new HashMap<>();
        JobManagerJobMetricGroup metricGroup =
                new UnregisteredMetricGroups.UnregisteredJobManagerJobMetricGroup() {
                    @Override
                    public Counter counter(String name) {
                        Counter counter = new SimpleCounter();
                        registeredCounters.put(name, counter);
                        return counter;
                    }
                };

        DefaultCheckpointStatsTracker tracker = new DefaultCheckpointStatsTracker(10, metricGroup);

        Counter regionalCounter =
                registeredCounters.get(
                        DefaultCheckpointStatsTracker.NUMBER_OF_REGIONAL_CHECKPOINTS_METRIC);
        assertThat(regionalCounter).isNotNull();

        // Initially zero
        assertThat(regionalCounter.getCount()).isEqualTo(0L);

        // After one regional checkpoint completes
        tracker.reportRegionalCheckpointCompleted();
        assertThat(regionalCounter.getCount()).isEqualTo(1L);

        // After another regional checkpoint completes
        tracker.reportRegionalCheckpointCompleted();
        assertThat(regionalCounter.getCount()).isEqualTo(2L);
    }

    @Test
    void testRegionalCheckpointCounterStaysZeroWithoutRegionalCheckpoints() {
        final Map<String, Counter> registeredCounters = new HashMap<>();
        JobManagerJobMetricGroup metricGroup =
                new UnregisteredMetricGroups.UnregisteredJobManagerJobMetricGroup() {
                    @Override
                    public Counter counter(String name) {
                        Counter counter = new SimpleCounter();
                        registeredCounters.put(name, counter);
                        return counter;
                    }
                };

        DefaultCheckpointStatsTracker tracker = new DefaultCheckpointStatsTracker(10, metricGroup);

        Counter regionalCounter =
                registeredCounters.get(
                        DefaultCheckpointStatsTracker.NUMBER_OF_REGIONAL_CHECKPOINTS_METRIC);
        assertThat(regionalCounter).isNotNull();

        // Without any regional checkpoint reported, counter stays at 0
        assertThat(regionalCounter.getCount()).isEqualTo(0L);
    }

    @Test
    void testNoOpTrackerRegionalCheckpoint() {
        // NoOpCheckpointStatsTracker should not throw when called
        NoOpCheckpointStatsTracker.INSTANCE.reportRegionalCheckpointCompleted();
    }

    /** Simple Counter implementation for testing. */
    private static class SimpleCounter implements Counter {
        private long count = 0;

        @Override
        public void inc() {
            count++;
        }

        @Override
        public void inc(long n) {
            count += n;
        }

        @Override
        public void dec() {
            count--;
        }

        @Override
        public void dec(long n) {
            count -= n;
        }

        @Override
        public long getCount() {
            return count;
        }
    }
}
