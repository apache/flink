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

package org.apache.flink.runtime.scheduler.metrics;

import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.util.clock.ManualClock;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.function.Supplier;

import static org.apache.flink.runtime.scheduler.metrics.StateTimeMetricTest.enable;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for SubStateTime metrics. */
public class SubStateTimeMetricsTest {
    private static final MetricOptions.JobStatusMetricsSettings settings =
            enable(
                    MetricOptions.JobStatusMetrics.STATE,
                    MetricOptions.JobStatusMetrics.CURRENT_TIME,
                    MetricOptions.JobStatusMetrics.TOTAL_TIME);

    @Test
    void testInitialValues() {
        final ManualClock clock = new ManualClock(Duration.ofMillis(5).toNanos());

        final SubStateTimeMetrics metrics =
                new SubStateTimeMetrics(settings, "test", new SettableSupplier(false), clock);

        assertThat(metrics.getCurrentTime()).isEqualTo(0L);
        assertThat(metrics.getTotalTime()).isEqualTo(0L);
        assertThat(metrics.getBinary()).isEqualTo(0L);
    }

    @Test
    void testGetBinary() {
        final SettableSupplier predicate = new SettableSupplier(false);
        final SubStateTimeMetrics metrics = new SubStateTimeMetrics(settings, "test", predicate);

        assertThat(metrics.getBinary()).isEqualTo(0L);

        predicate.setActive(true);

        metrics.onStateUpdate();
        assertThat(metrics.getBinary()).isEqualTo(1L);

        predicate.setActive(false);
        metrics.onStateUpdate();
        assertThat(metrics.getBinary()).isEqualTo(0L);
    }

    @Test
    void testGetCurrentTime() {
        final ManualClock clock = new ManualClock(Duration.ofMillis(5).toNanos());
        final SettableSupplier predicate = new SettableSupplier(false);

        final SubStateTimeMetrics metrics =
                new SubStateTimeMetrics(settings, "test", predicate, clock);

        predicate.setActive(true);

        metrics.onStateUpdate();
        assertThat(metrics.getCurrentTime()).isEqualTo(0L);

        clock.advanceTime(Duration.ofMillis(5));
        assertThat(metrics.getCurrentTime()).isEqualTo(5L);

        predicate.setActive(false);
        metrics.onStateUpdate();
        assertThat(metrics.getCurrentTime()).isEqualTo(0L);
    }

    @Test
    void testGetCurrentNotResetWhenStateStaysActive() {
        final ManualClock clock = new ManualClock(Duration.ofMillis(5).toNanos());
        final SettableSupplier predicate = new SettableSupplier(false);

        final SubStateTimeMetrics metrics =
                new SubStateTimeMetrics(settings, "test", predicate, clock);

        // enter state for 5 seconds
        predicate.setActive(true);

        metrics.onStateUpdate();
        clock.advanceTime(Duration.ofMillis(5));

        // another state update, reinforcing the current state

        metrics.onStateUpdate();
        assertThat(metrics.getCurrentTime()).isEqualTo(5L);
    }

    @Test
    void testGetCurrentTimeResetOnStateEnd() {
        final ManualClock clock = new ManualClock(Duration.ofMillis(5).toNanos());
        final SettableSupplier predicate = new SettableSupplier(false);

        final SubStateTimeMetrics metrics =
                new SubStateTimeMetrics(settings, "test", predicate, clock);

        predicate.setActive(true);

        metrics.onStateUpdate();
        clock.advanceTime(Duration.ofMillis(5));

        predicate.setActive(false);
        metrics.onStateUpdate();
        assertThat(metrics.getCurrentTime()).isEqualTo(0L);
    }

    @Test
    void testGetTotalTime() {
        final ManualClock clock = new ManualClock(Duration.ofMillis(5).toNanos());
        final SettableSupplier predicate = new SettableSupplier(false);

        final SubStateTimeMetrics metrics =
                new SubStateTimeMetrics(settings, "test", predicate, clock);

        // enter the state for 5 milliseconds and leave state
        predicate.setActive(true);

        metrics.onStateUpdate();
        clock.advanceTime(Duration.ofMillis(5));
        predicate.setActive(false);
        metrics.onStateUpdate();
        assertThat(metrics.getTotalTime()).isEqualTo(5L);

        // enter the state for 5 milliseconds and leave state
        predicate.setActive(true);

        metrics.onStateUpdate();
        clock.advanceTime(Duration.ofMillis(5));
        predicate.setActive(false);
        metrics.onStateUpdate();
        assertThat(metrics.getTotalTime()).isEqualTo(10L);
    }

    @Test
    void testGetTotalTimeDoesNotAdvanceOutsideTargetState() {
        final ManualClock clock = new ManualClock(Duration.ofMillis(5).toNanos());
        final SettableSupplier predicate = new SettableSupplier(false);

        final SubStateTimeMetrics metrics =
                new SubStateTimeMetrics(settings, "test", predicate, clock);

        // enter the state for 5 milliseconds and leave state
        predicate.setActive(true);

        metrics.onStateUpdate();
        clock.advanceTime(Duration.ofMillis(5));
        predicate.setActive(false);
        metrics.onStateUpdate();
        assertThat(metrics.getTotalTime()).isEqualTo(5L);

        // advance time; total time should not change because we're not in target state
        clock.advanceTime(Duration.ofMillis(5));
        assertThat(metrics.getTotalTime()).isEqualTo(5L);
    }

    @Test
    void testGetTotalTimeIncludesCurrentTime() {
        final ManualClock clock = new ManualClock(Duration.ofMillis(5).toNanos());
        final SettableSupplier predicate = new SettableSupplier(false);

        final SubStateTimeMetrics metrics =
                new SubStateTimeMetrics(settings, "test", predicate, clock);

        // enter the state for 5 milliseconds
        predicate.setActive(true);

        metrics.onStateUpdate();
        clock.advanceTime(Duration.ofMillis(5));
        assertThat(metrics.getTotalTime()).isEqualTo(5L);
    }

    @Test
    void testCleanStateAfterEarlyFailure() {
        final ManualClock clock = new ManualClock(Duration.ofMillis(5).toNanos());
        final SettableSupplier predicate = new SettableSupplier(false);

        final SubStateTimeMetrics metrics =
                new SubStateTimeMetrics(settings, "test", predicate, clock);

        metrics.onStateUpdate();
        predicate.setActive(true);
        predicate.setActive(false);
        metrics.onStateUpdate();

        assertThat(metrics.hasCleanState()).isEqualTo(true);
    }

    private static class SettableSupplier implements Supplier<Boolean> {

        private boolean isActive;

        public SettableSupplier(boolean isActive) {
            this.isActive = isActive;
        }

        public void setActive(boolean isActive) {
            this.isActive = isActive;
        }

        @Override
        public Boolean get() {
            return isActive;
        }
    }
}
