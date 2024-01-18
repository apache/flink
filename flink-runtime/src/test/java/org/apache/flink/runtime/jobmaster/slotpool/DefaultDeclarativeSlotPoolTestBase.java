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

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import org.assertj.core.util.Lists;

import javax.annotation.Nullable;

import java.util.List;
import java.util.concurrent.Callable;

/**
 * Tests base class for the {@link DefaultDeclarativeSlotPool} & {@link
 * BlocklistDeclarativeSlotPool}.
 */
abstract class DefaultDeclarativeSlotPoolTestBase {

    // The bool attribute is used only for readable for the parameterized test cases.
    @Parameter boolean enableSlotRequestByBatch;

    @Parameter(1)
    @Nullable
    Time slotRequestMaxInterval;

    @Parameter(2)
    ComponentMainThreadExecutor componentMainThreadExecutor;

    @Parameters(
            name =
                    "enableSlotRequestByBatch: {0}, slotRequestMaxInterval: {1}, componentMainThreadExecutor: {2}")
    static List<Object[]> getParametersCouples() {
        return Lists.newArrayList(
                new Object[] {
                    true,
                    Time.milliseconds(50L),
                    ComponentMainThreadExecutorServiceAdapter.forMainThread()
                },
                new Object[] {
                    false, null, ComponentMainThreadExecutorServiceAdapter.forMainThread()
                });
    }

    void waitSlotRequestMaxIntervalIfNeeded() {
        waitSlotRequestMaxIntervalIfNeeded(slotRequestMaxInterval);
    }

    static void waitSlotRequestMaxIntervalIfNeeded(Time slotRequestMaxInterval) {
        if (slotRequestMaxInterval == null) {
            return;
        }
        try {
            Thread.sleep(slotRequestMaxInterval.toMilliseconds());
        } catch (InterruptedException e) {
            throw new RuntimeException("Failed in waiting slot request max interval.", e);
        }
    }

    Callable<Void> getSlotRequestMaxIntervalWaiter() {
        return getSlotRequestMaxIntervalWaiter(slotRequestMaxInterval);
    }

    static Callable<Void> getSlotRequestMaxIntervalWaiter(Time slotRequestMaxInterval) {
        return () -> {
            if (slotRequestMaxInterval != null) {
                Thread.sleep(slotRequestMaxInterval.toMilliseconds());
            }
            return null;
        };
    }
}
