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

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collection;

/** Common test cases parameters for class {@link DeclarativeSlotPoolBridge}. */
abstract class DeclarativeSlotPoolBridgeTestBase {

    static final Time DEFAULT_SLOT_REQUEST_MAX_INTERVAL = Time.milliseconds(30);
    static final JobMasterId JOB_MASTER_ID = JobMasterId.generate();

    final ComponentMainThreadExecutor mainThreadExecutor =
            ComponentMainThreadExecutorServiceAdapter.forMainThread();

    @Parameter boolean slotBatchAllocatable;

    @Parameter(1)
    Time slotRequestMaxInterval;

    @Parameter(2)
    RequestSlotMatchingStrategy requestSlotMatchingStrategy;

    @Parameters(
            name =
                    "slotBatchAllocatable and slotRequestByBatch: {0}, slotRequestMaxInterval: {1}, requestSlotMatchingStrategy: {2}")
    static Collection<Object[]> getParametersCouples() {
        return Arrays.asList(
                new Object[] {
                    true,
                    DEFAULT_SLOT_REQUEST_MAX_INTERVAL,
                    SimpleRequestSlotMatchingStrategy.INSTANCE
                },
                new Object[] {
                    true,
                    DEFAULT_SLOT_REQUEST_MAX_INTERVAL,
                    PreferredAllocationRequestSlotMatchingStrategy.create(
                            SimpleRequestSlotMatchingStrategy.INSTANCE)
                },
                new Object[] {false, null, SimpleRequestSlotMatchingStrategy.INSTANCE},
                new Object[] {
                    false,
                    null,
                    PreferredAllocationRequestSlotMatchingStrategy.create(
                            SimpleRequestSlotMatchingStrategy.INSTANCE)
                });
    }

    static void waitSlotRequestMaxIntervalIfNeeded(@Nullable Time slotRequestMaxInterval) {
        if (slotRequestMaxInterval == null) {
            return;
        }
        try {
            // With 10L milliseconds additional time-cost.
            Thread.sleep(slotRequestMaxInterval.toMilliseconds() + 10L);
        } catch (InterruptedException e) {
            throw new RuntimeException("Failed in waiting slot request max interval.", e);
        }
    }
}
