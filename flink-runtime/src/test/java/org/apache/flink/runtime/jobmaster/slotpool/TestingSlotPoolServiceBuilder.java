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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.AllocatedSlotReport;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.function.TriConsumer;
import org.apache.flink.util.function.TriFunction;

import javax.annotation.Nonnull;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/** {@link SlotPoolServiceFactory} which creates a {@link TestingSlotPoolService}. */
public class TestingSlotPoolServiceBuilder implements SlotPoolServiceFactory {

    private TriConsumer<? super JobMasterId, ? super String, ? super ComponentMainThreadExecutor>
            startConsumer = (ignoredA, ignoredB, ignoredC) -> {};
    private Runnable closeRunnable = () -> {};
    private TriFunction<
                    ? super TaskManagerLocation,
                    ? super TaskManagerGateway,
                    ? super Collection<SlotOffer>,
                    ? extends Collection<SlotOffer>>
            offerSlotsFunction = (ignoredA, ignoredB, ignoredC) -> Collections.emptyList();
    private TriFunction<
                    ? super ResourceID,
                    ? super AllocationID,
                    ? super Exception,
                    Optional<ResourceID>>
            failAllocationFunction = (ignoredA, ignoredB, ignoredC) -> Optional.empty();
    private Function<? super ResourceID, Boolean> registerTaskManagerFunction = ignored -> false;
    private BiFunction<? super ResourceID, ? super Exception, Boolean> releaseTaskManagerFunction =
            (ignoredA, ignoredB) -> false;
    private Consumer<? super ResourceManagerGateway> connectToResourceManagerConsumer =
            ignored -> {};
    private Runnable disconnectResourceManagerRunnable = () -> {};
    private BiFunction<? super JobID, ? super ResourceID, ? extends AllocatedSlotReport>
            createAllocatedSlotReportFunction =
                    (jobId, ignored) -> new AllocatedSlotReport(jobId, Collections.emptyList());

    @Nonnull
    @Override
    public SlotPoolService createSlotPoolService(@Nonnull JobID jobId) {
        return new TestingSlotPoolService(
                jobId,
                startConsumer,
                closeRunnable,
                offerSlotsFunction,
                failAllocationFunction,
                registerTaskManagerFunction,
                releaseTaskManagerFunction,
                connectToResourceManagerConsumer,
                disconnectResourceManagerRunnable,
                createAllocatedSlotReportFunction);
    }

    public static TestingSlotPoolServiceBuilder newBuilder() {
        return new TestingSlotPoolServiceBuilder();
    }
}
