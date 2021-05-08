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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createRandomExecutionVertexId;
import static org.apache.flink.runtime.scheduler.SharedSlotTestingUtils.createExecutionSlotSharingGroup;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;

/** Test suite for {@link SharingPhysicalSlotRequestBulk}. */
public class SharingPhysicalSlotRequestBulkTest extends TestLogger {
    private static final ExecutionVertexID EV1 = createRandomExecutionVertexId();
    private static final ExecutionVertexID EV2 = createRandomExecutionVertexId();
    private static final ExecutionVertexID EV3 = createRandomExecutionVertexId();
    private static final ExecutionVertexID EV4 = createRandomExecutionVertexId();
    private static final ExecutionVertexID EV5 = createRandomExecutionVertexId();

    private static final ExecutionSlotSharingGroup SG1 =
            createExecutionSlotSharingGroup(EV1, EV2, EV3);
    private static final ExecutionSlotSharingGroup SG2 = createExecutionSlotSharingGroup(EV4, EV5);

    private static final ResourceProfile RP1 =
            ResourceProfile.newBuilder().setCpuCores(3.0).build();
    private static final ResourceProfile RP2 =
            ResourceProfile.newBuilder().setCpuCores(2.0).build();

    @Test
    public void testCreation() {
        SharingPhysicalSlotRequestBulk bulk = createBulk();
        assertThat(bulk.getPendingRequests(), containsInAnyOrder(RP1, RP2));
        assertThat(bulk.getAllocationIdsOfFulfilledRequests(), hasSize(0));
    }

    @Test
    public void testMarkFulfilled() {
        SharingPhysicalSlotRequestBulk bulk = createBulk();
        AllocationID allocationId = new AllocationID();
        bulk.markFulfilled(SG1, allocationId);
        assertThat(bulk.getPendingRequests(), containsInAnyOrder(RP2));
        assertThat(bulk.getAllocationIdsOfFulfilledRequests(), containsInAnyOrder(allocationId));
    }

    @Test
    public void testCancel() {
        LogicalSlotRequestCanceller canceller = new LogicalSlotRequestCanceller();
        SharingPhysicalSlotRequestBulk bulk = createBulk(canceller);
        bulk.markFulfilled(SG1, new AllocationID());
        Throwable cause = new Throwable();
        bulk.cancel(cause);
        assertThat(
                canceller.cancellations,
                containsInAnyOrder(
                        Tuple2.of(EV1, cause), Tuple2.of(EV2, cause), Tuple2.of(EV4, cause)));
    }

    @Test
    public void testClearPendingRequests() {
        SharingPhysicalSlotRequestBulk bulk = createBulk();
        bulk.clearPendingRequests();
        assertThat(bulk.getPendingRequests(), hasSize(0));
    }

    private static class LogicalSlotRequestCanceller
            implements BiConsumer<ExecutionVertexID, Throwable> {
        private final List<Tuple2<ExecutionVertexID, Throwable>> cancellations = new ArrayList<>();

        @Override
        public void accept(ExecutionVertexID executionVertexID, Throwable throwable) {
            cancellations.add(Tuple2.of(executionVertexID, throwable));
        }
    }

    private static SharingPhysicalSlotRequestBulk createBulk() {
        return createBulk((executionVertexID, throwable) -> {});
    }

    private static SharingPhysicalSlotRequestBulk createBulk(
            BiConsumer<ExecutionVertexID, Throwable> canceller) {
        Map<ExecutionSlotSharingGroup, ResourceProfile> pendingRequests = new IdentityHashMap<>();
        pendingRequests.put(SG1, RP1);
        pendingRequests.put(SG2, RP2);

        Map<ExecutionSlotSharingGroup, List<ExecutionVertexID>> executions = new HashMap<>();
        executions.put(SG1, Arrays.asList(EV1, EV2));
        executions.put(SG2, Collections.singletonList(EV4));

        return new SharingPhysicalSlotRequestBulk(executions, pendingRequests, canceller);
    }
}
