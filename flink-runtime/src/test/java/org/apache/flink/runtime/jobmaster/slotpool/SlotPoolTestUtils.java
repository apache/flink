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

import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.RpcTaskManagerGateway;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Collection;

/** Utilities for testing slot pool implementations. */
public final class SlotPoolTestUtils {

    private SlotPoolTestUtils() {
        throw new UnsupportedOperationException("This class should never be instantiated.");
    }

    public static TaskManagerGateway createTaskManagerGateway(
            @Nullable TaskExecutorGateway taskExecutorGateway) {
        return new RpcTaskManagerGateway(
                taskExecutorGateway == null
                        ? new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway()
                        : taskExecutorGateway,
                JobMasterId.generate());
    }

    @Nonnull
    public static Collection<SlotOffer> offerSlots(
            DeclarativeSlotPool slotPool, Collection<? extends SlotOffer> slotOffers) {
        return offerSlots(slotPool, slotOffers, createTaskManagerGateway(null));
    }

    @Nonnull
    public static Collection<SlotOffer> offerSlots(
            DeclarativeSlotPool slotPool,
            Collection<? extends SlotOffer> slotOffers,
            TaskManagerGateway taskManagerGateway) {
        return slotPool.offerSlots(
                slotOffers, new LocalTaskManagerLocation(), taskManagerGateway, 0);
    }
}
