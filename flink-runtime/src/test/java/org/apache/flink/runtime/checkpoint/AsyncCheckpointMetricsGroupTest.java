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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link AsyncCheckpointMetricsGroup} class. */
public class AsyncCheckpointMetricsGroupTest {

    @Test
    public void testCheckpointMetricsRegistration() {
        // Construct a CheckpointMetricsGroup.
        final Map<String, Gauge<?>> registeredGauges = new HashMap<>();
        MetricGroup metricGroup =
                new UnregisteredMetricsGroup() {
                    @Override
                    public <T, G extends Gauge<T>> G gauge(String name, G gauge) {
                        registeredGauges.put(name, gauge);
                        return gauge;
                    }
                };
        AsyncCheckpointMetricsGroup asyncCheckpointMetricsGroup =
                new AsyncCheckpointMetricsGroup(metricGroup);

        // Construct a CheckpointMetrics.
        CheckpointMetrics checkpointMetrics =
                new CheckpointMetricsBuilder()
                        .setBytesProcessedDuringAlignment(12L)
                        .setAlignmentDurationNanos(23L)
                        .setBytesPersistedOfThisCheckpoint(34L)
                        .setTotalBytesPersisted(45L)
                        .setAsyncDurationMillis(56L)
                        .setSyncDurationMillis(67L)
                        .setCheckpointStartDelayNanos(78L)
                        .build();

        // Update and Verify the status of AsyncCheckpointMetricsGroup.
        asyncCheckpointMetricsGroup.reportCheckpointMetrics(checkpointMetrics);
        assertThat(
                        registeredGauges
                                .get(AsyncCheckpointMetricsGroup.BYTES_PERSISTED_OF_THIS_CHECKPOINT)
                                .getValue())
                .isEqualTo(checkpointMetrics.getBytesPersistedOfThisCheckpoint());
        assertThat(
                        registeredGauges
                                .get(AsyncCheckpointMetricsGroup.TOTAL_BYTES_PERSISTED)
                                .getValue())
                .isEqualTo(checkpointMetrics.getTotalBytesPersisted());
        assertThat(
                        registeredGauges
                                .get(AsyncCheckpointMetricsGroup.ASYNC_DURATION_MILLIS)
                                .getValue())
                .isEqualTo(checkpointMetrics.getAsyncDurationMillis());
    }
}
