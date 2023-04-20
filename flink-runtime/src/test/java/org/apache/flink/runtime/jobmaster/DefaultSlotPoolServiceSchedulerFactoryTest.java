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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.SchedulerExecutionMode;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.jobmaster.slotpool.PreferredAllocationRequestSlotMatchingStrategy;
import org.apache.flink.runtime.jobmaster.slotpool.RequestSlotMatchingStrategy;
import org.apache.flink.runtime.jobmaster.slotpool.SimpleRequestSlotMatchingStrategy;
import org.apache.flink.runtime.scheduler.DefaultSchedulerFactory;
import org.apache.flink.runtime.scheduler.adaptive.AdaptiveSchedulerFactory;
import org.apache.flink.runtime.scheduler.adaptivebatch.AdaptiveBatchSchedulerFactory;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link DefaultSlotPoolServiceSchedulerFactory}. */
@ExtendWith(TestLoggerExtension.class)
public class DefaultSlotPoolServiceSchedulerFactoryTest {

    @Test
    public void testFallsBackToDefaultSchedulerIfAdaptiveSchedulerInBatchJob() {
        final Configuration configuration = new Configuration();
        configuration.set(JobManagerOptions.SCHEDULER, JobManagerOptions.SchedulerType.Adaptive);

        final DefaultSlotPoolServiceSchedulerFactory defaultSlotPoolServiceSchedulerFactory =
                DefaultSlotPoolServiceSchedulerFactory.fromConfiguration(
                        configuration, JobType.BATCH, true);

        assertThat(defaultSlotPoolServiceSchedulerFactory.getSchedulerNGFactory())
                .isInstanceOf(AdaptiveBatchSchedulerFactory.class);
        assertThat(defaultSlotPoolServiceSchedulerFactory.getSchedulerType())
                .isEqualTo(JobManagerOptions.SchedulerType.AdaptiveBatch);
    }

    @Test
    public void testAdaptiveSchedulerForReactiveMode() {
        final Configuration configuration = new Configuration();
        configuration.set(JobManagerOptions.SCHEDULER_MODE, SchedulerExecutionMode.REACTIVE);

        final DefaultSlotPoolServiceSchedulerFactory defaultSlotPoolServiceSchedulerFactory =
                DefaultSlotPoolServiceSchedulerFactory.fromConfiguration(
                        configuration, JobType.STREAMING, false);

        assertThat(defaultSlotPoolServiceSchedulerFactory.getSchedulerNGFactory())
                .isInstanceOf(AdaptiveSchedulerFactory.class);
        assertThat(defaultSlotPoolServiceSchedulerFactory.getSchedulerType())
                .isEqualTo(JobManagerOptions.SchedulerType.Adaptive);
    }

    @Test
    public void testFallBackSchedulerWithAdaptiveSchedulerTestProperty() {
        String propertyValue = saveAdaptiveSchedulerTestPropertyValue();
        System.setProperty("flink.tests.enable-adaptive-scheduler", "true");

        DefaultSlotPoolServiceSchedulerFactory defaultSlotPoolServiceSchedulerFactory =
                DefaultSlotPoolServiceSchedulerFactory.fromConfiguration(
                        new Configuration(), JobType.BATCH, true);

        assertThat(defaultSlotPoolServiceSchedulerFactory.getSchedulerNGFactory())
                .isInstanceOf(AdaptiveBatchSchedulerFactory.class);
        assertThat(defaultSlotPoolServiceSchedulerFactory.getSchedulerType())
                .isEqualTo(JobManagerOptions.SchedulerType.AdaptiveBatch);

        defaultSlotPoolServiceSchedulerFactory =
                DefaultSlotPoolServiceSchedulerFactory.fromConfiguration(
                        new Configuration(), JobType.STREAMING, false);

        assertThat(defaultSlotPoolServiceSchedulerFactory.getSchedulerNGFactory())
                .isInstanceOf(AdaptiveSchedulerFactory.class);
        assertThat(defaultSlotPoolServiceSchedulerFactory.getSchedulerType())
                .isEqualTo(JobManagerOptions.SchedulerType.Adaptive);

        restoreAdaptiveSchedulerTestPropertiesValue(propertyValue);
    }

    @Test
    public void testFallBackSchedulerWithoutAdaptiveSchedulerTestProperty() {
        String propertyValue = saveAdaptiveSchedulerTestPropertyValue();
        System.clearProperty("flink.tests.enable-adaptive-scheduler");

        DefaultSlotPoolServiceSchedulerFactory defaultSlotPoolServiceSchedulerFactory =
                DefaultSlotPoolServiceSchedulerFactory.fromConfiguration(
                        new Configuration(), JobType.BATCH, true);

        assertThat(defaultSlotPoolServiceSchedulerFactory.getSchedulerNGFactory())
                .isInstanceOf(AdaptiveBatchSchedulerFactory.class);
        assertThat(defaultSlotPoolServiceSchedulerFactory.getSchedulerType())
                .isEqualTo(JobManagerOptions.SchedulerType.AdaptiveBatch);

        defaultSlotPoolServiceSchedulerFactory =
                DefaultSlotPoolServiceSchedulerFactory.fromConfiguration(
                        new Configuration(), JobType.STREAMING, false);

        assertThat(defaultSlotPoolServiceSchedulerFactory.getSchedulerNGFactory())
                .isInstanceOf(DefaultSchedulerFactory.class);
        assertThat(defaultSlotPoolServiceSchedulerFactory.getSchedulerType())
                .isEqualTo(JobManagerOptions.SchedulerType.Default);

        restoreAdaptiveSchedulerTestPropertiesValue(propertyValue);
    }

    @ParameterizedTest
    @MethodSource("testGetRequestSlotMatchingStrategy")
    public void testGetRequestSlotMatchingStrategy(
            boolean isLocalRecoveryEnabled, JobType jobType, RequestSlotMatchingStrategy expected) {
        final Configuration configuration = new Configuration();
        configuration.set(CheckpointingOptions.LOCAL_RECOVERY, isLocalRecoveryEnabled);
        assertThat(
                        DefaultSlotPoolServiceSchedulerFactory.getRequestSlotMatchingStrategy(
                                configuration, jobType))
                .isSameAs(expected);
    }

    private static Stream<Arguments> testGetRequestSlotMatchingStrategy() {
        return Stream.of(
                Arguments.of(false, JobType.BATCH, SimpleRequestSlotMatchingStrategy.INSTANCE),
                Arguments.of(false, JobType.STREAMING, SimpleRequestSlotMatchingStrategy.INSTANCE),
                Arguments.of(true, JobType.BATCH, SimpleRequestSlotMatchingStrategy.INSTANCE),
                Arguments.of(
                        true,
                        JobType.STREAMING,
                        PreferredAllocationRequestSlotMatchingStrategy.INSTANCE));
    }

    private String saveAdaptiveSchedulerTestPropertyValue() {
        return (String) System.getProperty("flink.tests.enable-adaptive-scheduler");
    }

    private void restoreAdaptiveSchedulerTestPropertiesValue(String savedPropertyValue) {
        if (savedPropertyValue == null) {
            System.clearProperty("flink.tests.enable-adaptive-scheduler");
        } else {
            System.setProperty("flink.tests.enable-adaptive-scheduler", savedPropertyValue);
        }
    }
}
