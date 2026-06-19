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

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.scheduler.ExecutionSlotSharingGroup;
import org.apache.flink.runtime.scheduler.SharingPhysicalSlotRequestBulk;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

class TestingPhysicalSlotRequestBulkBuilder {
    private static final BiConsumer<ExecutionVertexID, Throwable> EMPTY_CANCELLER = (r, t) -> {};
    private final Map<ExecutionSlotSharingGroup, List<ExecutionVertexID>> executions =
            new HashMap<>();
    private final Map<ExecutionSlotSharingGroup, ResourceProfile> pendingRequests = new HashMap<>();
    private BiConsumer<ExecutionVertexID, Throwable> canceller = EMPTY_CANCELLER;

    TestingPhysicalSlotRequestBulkBuilder addPendingRequest(
            ExecutionSlotSharingGroup executionSlotSharingGroup, ResourceProfile resourceProfile) {
        pendingRequests.put(executionSlotSharingGroup, resourceProfile);
        executions.put(
                executionSlotSharingGroup,
                new ArrayList<>(executionSlotSharingGroup.getExecutionVertexIds()));
        return this;
    }

    TestingPhysicalSlotRequestBulkBuilder setCanceller(
            BiConsumer<ExecutionVertexID, Throwable> canceller) {
        this.canceller = canceller;
        return this;
    }

    SharingPhysicalSlotRequestBulk buildSharingPhysicalSlotRequestBulk() {
        return new SharingPhysicalSlotRequestBulk(executions, pendingRequests, canceller);
    }

    PhysicalSlotRequestBulkWithTimestamp buildPhysicalSlotRequestBulkWithTimestamp() {
        return new PhysicalSlotRequestBulkWithTimestamp(buildSharingPhysicalSlotRequestBulk());
    }

    static TestingPhysicalSlotRequestBulkBuilder newBuilder() {
        return new TestingPhysicalSlotRequestBulkBuilder();
    }
}
