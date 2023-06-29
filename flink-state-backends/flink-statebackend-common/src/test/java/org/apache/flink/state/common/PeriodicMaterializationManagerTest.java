/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.common;

import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutorService;
import org.apache.flink.runtime.mailbox.SyncMailboxExecutor;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.state.common.PeriodicMaterializationManager.MaterializationTarget;

import org.junit.jupiter.api.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.flink.shaded.guava31.com.google.common.collect.Iterators.getOnlyElement;
import static org.apache.flink.util.concurrent.Executors.newDirectExecutorService;
import static org.assertj.core.api.Assertions.assertThat;

/** {@link PeriodicMaterializationManager} test. */
class PeriodicMaterializationManagerTest {

    @Test
    void testInitialDelay() {
        ManuallyTriggeredScheduledExecutorService scheduledExecutorService =
                new ManuallyTriggeredScheduledExecutorService();
        long periodicMaterializeDelay = 10_000L;

        try (PeriodicMaterializationManager test =
                new PeriodicMaterializationManager(
                        new SyncMailboxExecutor(),
                        newDirectExecutorService(),
                        "test",
                        (message, exception) -> {},
                        MaterializationTarget.NO_OP,
                        new ChangelogMaterializationMetricGroup(
                                UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup()),
                        periodicMaterializeDelay,
                        0,
                        "subtask-0",
                        scheduledExecutorService)) {
            test.start();

            assertThat(
                            getOnlyElement(
                                            scheduledExecutorService
                                                    .getAllScheduledTasks()
                                                    .iterator())
                                    .getDelay(MILLISECONDS))
                    .as(
                            String.format(
                                    "task for initial materialization should be scheduled with a 0..%d delay",
                                    periodicMaterializeDelay))
                    .isLessThanOrEqualTo(periodicMaterializeDelay);
        }
    }
}
