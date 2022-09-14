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

package org.apache.flink.runtime.rest.messages;

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.rest.messages.job.SubtaskExecutionAttemptDetailsInfo;
import org.apache.flink.runtime.rest.messages.job.metrics.IOMetricsInfo;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Tests for the {@link AggregatedTaskDetailsInfo}. */
public class AggregatedTaskDetailsInfoTest
        extends RestResponseMarshallingTestBase<AggregatedTaskDetailsInfo> {
    @Override
    protected Class<AggregatedTaskDetailsInfo> getTestResponseClass() {
        return AggregatedTaskDetailsInfo.class;
    }

    @Override
    protected AggregatedTaskDetailsInfo getTestResponseInstance() throws Exception {
        final Random random = new Random();

        final IOMetricsInfo ioMetricsInfo =
                new IOMetricsInfo(
                        Math.abs(random.nextLong()),
                        random.nextBoolean(),
                        Math.abs(random.nextLong()),
                        random.nextBoolean(),
                        Math.abs(random.nextLong()),
                        random.nextBoolean(),
                        Math.abs(random.nextLong()),
                        random.nextBoolean(),
                        Math.abs(random.nextLong()),
                        Math.abs(random.nextLong()),
                        Math.abs(random.nextDouble()));

        final Map<ExecutionState, Long> statusDuration = new HashMap<>();
        statusDuration.put(ExecutionState.CREATED, 10L);
        statusDuration.put(ExecutionState.SCHEDULED, 20L);
        statusDuration.put(ExecutionState.DEPLOYING, 30L);
        statusDuration.put(ExecutionState.INITIALIZING, 40L);
        statusDuration.put(ExecutionState.RUNNING, 50L);

        return AggregatedTaskDetailsInfo.create(
                Collections.singletonList(
                        new SubtaskExecutionAttemptDetailsInfo(
                                Math.abs(random.nextInt()),
                                ExecutionState.values()[
                                        random.nextInt(ExecutionState.values().length)],
                                Math.abs(random.nextInt()),
                                "localhost:" + random.nextInt(65536),
                                Math.abs(random.nextLong()),
                                Math.abs(random.nextLong()),
                                Math.abs(random.nextLong()),
                                ioMetricsInfo,
                                "taskmanagerId",
                                statusDuration,
                                null)));
    }

    @Test
    public void testMetricsStatistics() {
        final AggregatedTaskDetailsInfo.MetricsStatistics metricsStatistics =
                new AggregatedTaskDetailsInfo.MetricsStatistics("test");
        for (int i = 0; i < 100; ++i) {
            metricsStatistics.addValue(i);
        }
        assertEquals(0L, metricsStatistics.getMin());
        assertEquals(99L, metricsStatistics.getMax());
        assertEquals(49L, metricsStatistics.getPercentile(50));
        assertEquals(24L, metricsStatistics.getPercentile(25));
        assertEquals(74L, metricsStatistics.getPercentile(75));
        assertEquals(94L, metricsStatistics.getPercentile(95));
        assertEquals(4950L, metricsStatistics.getSum());
        assertEquals(49L, metricsStatistics.getAvg());
    }
}
