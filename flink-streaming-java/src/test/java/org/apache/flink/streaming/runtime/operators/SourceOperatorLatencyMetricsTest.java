/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.operators;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.mocks.MockSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.util.TestingTaskManagerRuntimeInfo;
import org.apache.flink.streaming.api.operators.SourceOperator;
import org.apache.flink.streaming.api.operators.SourceOperatorFactory;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.util.SourceOperatorTestHarness;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertTrue;

/** Tests for the emission of latency markers by {@link SourceOperator} operators. */
public class SourceOperatorLatencyMetricsTest extends TestLogger {

    private static final long MAX_PROCESSING_TIME = 100L;
    private static final long LATENCY_MARK_INTERVAL = 10L;

    /** Verifies that by default no latency metrics are emitted. */
    @Test
    public void testLatencyMarkEmissionDisabled() throws Exception {
        testLatencyMarkEmission(false, new Configuration(), new ExecutionConfig());
    }

    /** Verifies that latency metrics can be enabled via the {@link ExecutionConfig}. */
    @Test
    public void testLatencyMarkEmissionEnabledViaExecutionConfig() throws Exception {

        Configuration taskConfiguration = new Configuration();
        ExecutionConfig executionConfig = new ExecutionConfig();
        executionConfig.setLatencyTrackingInterval(LATENCY_MARK_INTERVAL);

        testLatencyMarkEmission(true, taskConfiguration, executionConfig);
    }

    /** Verifies that latency metrics can be enabled via the configuration. */
    @Test
    public void testLatencyMarkEmissionEnabledViaFlinkConfig() throws Exception {
        Configuration taskConfiguration = new Configuration();
        taskConfiguration.setLong(MetricOptions.LATENCY_INTERVAL, LATENCY_MARK_INTERVAL);
        ExecutionConfig executionConfig = new ExecutionConfig();

        testLatencyMarkEmission(true, taskConfiguration, executionConfig);
    }

    /**
     * Verifies that latency metrics can be enabled via the {@link ExecutionConfig} even if they are
     * disabled via the configuration.
     */
    @Test
    public void testLatencyMarkEmissionEnabledOverrideViaExecutionConfig() throws Exception {
        Configuration taskConfiguration = new Configuration();
        taskConfiguration.setLong(MetricOptions.LATENCY_INTERVAL, 0);
        ExecutionConfig executionConfig = new ExecutionConfig();
        executionConfig.setLatencyTrackingInterval(LATENCY_MARK_INTERVAL);

        testLatencyMarkEmission(true, taskConfiguration, executionConfig);
    }

    /**
     * Verifies that latency metrics can be disabled via the {@link ExecutionConfig} even if they
     * are enabled via the configuration.
     */
    @Test
    public void testLatencyMarkEmissionDisabledOverrideViaExecutionConfig() throws Exception {
        Configuration taskConfiguration = new Configuration();
        taskConfiguration.setLong(MetricOptions.LATENCY_INTERVAL, LATENCY_MARK_INTERVAL);
        ExecutionConfig executionConfig = new ExecutionConfig();
        executionConfig.setLatencyTrackingInterval(0);

        testLatencyMarkEmission(false, taskConfiguration, executionConfig);
    }

    private interface OperatorSetupOperation {
        void setupSourceOperator(SourceOperator<Integer, ?> operator) throws Exception;
    }

    private void testLatencyMarkEmission(
            boolean shouldExpectLatencyMarkers,
            Configuration taskManagerConfig,
            ExecutionConfig executionConfig)
            throws Exception {

        try (SourceOperatorTestHarness testHarness =
                new SourceOperatorTestHarness(
                        new SourceOperatorFactory(
                                new MockSource(Boundedness.CONTINUOUS_UNBOUNDED, 1),
                                WatermarkStrategy.noWatermarks()),
                        new MockEnvironmentBuilder()
                                .setTaskManagerRuntimeInfo(
                                        new TestingTaskManagerRuntimeInfo(taskManagerConfig))
                                .setExecutionConfig(executionConfig)
                                .build())) {
            testHarness.open();
            testHarness.setup();
            for (long processingTime = 0; processingTime <= MAX_PROCESSING_TIME; processingTime++) {
                testHarness.getProcessingTimeService().setCurrentTime(processingTime);
                testHarness.emitNext();
            }

            List<LatencyMarker> expectedOutput = new ArrayList<>();
            if (!shouldExpectLatencyMarkers) {
                assertTrue(testHarness.getOutput().isEmpty());
            } else {
                expectedOutput.add(
                        new LatencyMarker(1, testHarness.getOperator().getOperatorID(), 0));
                for (long markedTime = LATENCY_MARK_INTERVAL;
                        markedTime <= MAX_PROCESSING_TIME;
                        markedTime += LATENCY_MARK_INTERVAL) {
                    expectedOutput.add(
                            new LatencyMarker(
                                    markedTime, testHarness.getOperator().getOperatorID(), 0));
                }
                assertThat(
                        (Collection<Object>) testHarness.getOutput(),
                        contains(expectedOutput.toArray()));
            }
        }
    }
}
