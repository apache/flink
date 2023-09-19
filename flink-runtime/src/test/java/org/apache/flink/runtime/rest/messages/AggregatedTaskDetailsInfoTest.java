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
import org.apache.flink.testutils.junit.extensions.parameterized.NoOpTestExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link AggregatedTaskDetailsInfo}. */
@ExtendWith(NoOpTestExtension.class)
class AggregatedTaskDetailsInfoTest
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
    void testMetricsStatistics() {
        final AggregatedTaskDetailsInfo.MetricsStatistics metricsStatistics =
                new AggregatedTaskDetailsInfo.MetricsStatistics("test");
        for (int i = 0; i < 100; ++i) {
            metricsStatistics.addValue(i);
        }
        assertThat(metricsStatistics.getMin()).isZero();
        assertThat(metricsStatistics.getMax()).isEqualTo(99L);
        assertThat(metricsStatistics.getPercentile(50)).isEqualTo(49L);
        assertThat(metricsStatistics.getPercentile(25)).isEqualTo(24L);
        assertThat(metricsStatistics.getPercentile(75)).isEqualTo(74L);
        assertThat(metricsStatistics.getPercentile(95)).isEqualTo(94L);
        assertThat(metricsStatistics.getSum()).isEqualTo(4950L);
        assertThat(metricsStatistics.getAvg()).isEqualTo(49L);
    }
}
