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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.SchedulerExecutionMode;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.scheduler.DefaultSchedulerFactory;
import org.apache.flink.runtime.scheduler.adaptive.AdaptiveSchedulerFactory;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link DefaultSlotPoolServiceSchedulerFactory}. */
@ExtendWith(TestLoggerExtension.class)
public class DefaultSlotPoolServiceSchedulerFactoryTest {

    @Test
    public void testFallsBackToDefaultSchedulerIfBatchJob() {
        final Configuration configuration = new Configuration();
        configuration.set(JobManagerOptions.SCHEDULER, JobManagerOptions.SchedulerType.Adaptive);

        final DefaultSlotPoolServiceSchedulerFactory defaultSlotPoolServiceSchedulerFactory =
                DefaultSlotPoolServiceSchedulerFactory.fromConfiguration(
                        configuration, JobType.BATCH);

        assertThat(defaultSlotPoolServiceSchedulerFactory.getSchedulerNGFactory())
                .isInstanceOf(DefaultSchedulerFactory.class);
        assertThat(defaultSlotPoolServiceSchedulerFactory.getSchedulerType())
                .isEqualTo(JobManagerOptions.SchedulerType.Ng);
    }

    @Test
    public void testAdaptiveSchedulerForReactiveMode() {
        final Configuration configuration = new Configuration();
        configuration.set(JobManagerOptions.SCHEDULER_MODE, SchedulerExecutionMode.REACTIVE);

        final DefaultSlotPoolServiceSchedulerFactory defaultSlotPoolServiceSchedulerFactory =
                DefaultSlotPoolServiceSchedulerFactory.fromConfiguration(
                        configuration, JobType.STREAMING);

        assertThat(defaultSlotPoolServiceSchedulerFactory.getSchedulerNGFactory())
                .isInstanceOf(AdaptiveSchedulerFactory.class);
        assertThat(defaultSlotPoolServiceSchedulerFactory.getSchedulerType())
                .isEqualTo(JobManagerOptions.SchedulerType.Adaptive);
    }
}
