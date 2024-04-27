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

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.jobmanager.scheduler.Locality;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.slotpool.DummyPayload;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlot;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createRandomExecutionVertexId;
import static org.apache.flink.runtime.scheduler.SharedSlotTestingUtils.createExecutionSlotSharingGroup;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test suite for {@link SharedSlot}. */
class SharedSlotTest {
    private static final ExecutionVertexID EV1 = createRandomExecutionVertexId();
    private static final ExecutionVertexID EV2 = createRandomExecutionVertexId();
    private static final ExecutionSlotSharingGroup SG = createExecutionSlotSharingGroup(EV1, EV2);
    private static final SlotRequestId PHYSICAL_SLOT_REQUEST_ID = new SlotRequestId();
    private static final ResourceProfile RP = ResourceProfile.newBuilder().setCpuCores(2.0).build();

    @Test
    void testCreation() {
        SharedSlot sharedSlot =
                SharedSlotBuilder.newBuilder().slotWillBeOccupiedIndefinitely().build();
        assertThat(sharedSlot.getPhysicalSlotRequestId()).isEqualTo(PHYSICAL_SLOT_REQUEST_ID);
        assertThat(sharedSlot.getPhysicalSlotResourceProfile()).isEqualTo(RP);
        assertThat(sharedSlot.willOccupySlotIndefinitely()).isTrue();
        assertThat(sharedSlot.isEmpty()).isTrue();
    }

    @Test
    void testAssignAsPayloadToPhysicalSlot() {
        CompletableFuture<PhysicalSlot> slotContextFuture = new CompletableFuture<>();
        SharedSlot sharedSlot =
                SharedSlotBuilder.newBuilder().withSlotContextFuture(slotContextFuture).build();
        TestingPhysicalSlot physicalSlot = new TestingPhysicalSlot(RP, new AllocationID());
        slotContextFuture.complete(physicalSlot);
        assertThat(physicalSlot.getPayload()).isEqualTo(sharedSlot);
    }

    @Test
    void testLogicalSlotAllocation() {
        CompletableFuture<PhysicalSlot> slotContextFuture = new CompletableFuture<>();
        CompletableFuture<ExecutionSlotSharingGroup> released = new CompletableFuture<>();
        SharedSlot sharedSlot =
                SharedSlotBuilder.newBuilder()
                        .withSlotContextFuture(slotContextFuture)
                        .slotWillBeOccupiedIndefinitely()
                        .withExternalReleaseCallback(released::complete)
                        .build();
        CompletableFuture<LogicalSlot> logicalSlotFuture = sharedSlot.allocateLogicalSlot(EV1);

        assertThatFuture(logicalSlotFuture).isNotDone();

        AllocationID allocationId = new AllocationID();
        LocalTaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();
        SimpleAckingTaskManagerGateway taskManagerGateway = new SimpleAckingTaskManagerGateway();
        slotContextFuture.complete(
                new TestingPhysicalSlot(
                        allocationId, taskManagerLocation, 3, taskManagerGateway, RP));

        assertThat(sharedSlot.isEmpty()).isFalse();
        assertThatFuture(released).isNotDone();
        assertThat(logicalSlotFuture).isDone();
        LogicalSlot logicalSlot = logicalSlotFuture.join();
        assertThat(logicalSlot.getAllocationId()).isEqualTo(allocationId);
        assertThat(logicalSlot.getTaskManagerLocation()).isEqualTo(taskManagerLocation);
        assertThat(logicalSlot.getTaskManagerGateway()).isEqualTo(taskManagerGateway);
        assertThat(logicalSlot.getLocality()).isEqualTo(Locality.UNKNOWN);
    }

    @Test
    void testLogicalSlotFailureDueToPhysicalSlotFailure() {
        CompletableFuture<PhysicalSlot> slotContextFuture = new CompletableFuture<>();
        CompletableFuture<ExecutionSlotSharingGroup> released = new CompletableFuture<>();
        SharedSlot sharedSlot =
                SharedSlotBuilder.newBuilder()
                        .withSlotContextFuture(slotContextFuture)
                        .withExternalReleaseCallback(released::complete)
                        .build();
        CompletableFuture<LogicalSlot> logicalSlotFuture = sharedSlot.allocateLogicalSlot(EV1);
        Throwable cause = new Throwable();
        slotContextFuture.completeExceptionally(cause);

        assertThat(logicalSlotFuture.isCompletedExceptionally()).isTrue();
        assertThatThrownBy(logicalSlotFuture::get)
                .isInstanceOf(ExecutionException.class)
                .hasCause(cause);
        assertThat(sharedSlot.isEmpty()).isTrue();
        assertThat(released).isDone();
    }

    @Test
    void testCancelCompletedLogicalSlotRequest() {
        CompletableFuture<PhysicalSlot> slotContextFuture = new CompletableFuture<>();
        CompletableFuture<ExecutionSlotSharingGroup> released = new CompletableFuture<>();
        SharedSlot sharedSlot =
                SharedSlotBuilder.newBuilder()
                        .withSlotContextFuture(slotContextFuture)
                        .withExternalReleaseCallback(released::complete)
                        .build();
        CompletableFuture<LogicalSlot> logicalSlotFuture = sharedSlot.allocateLogicalSlot(EV1);
        slotContextFuture.complete(new TestingPhysicalSlot(RP, new AllocationID()));
        sharedSlot.cancelLogicalSlotRequest(EV1, new Throwable());

        assertThatFuture(logicalSlotFuture).isNotCompletedExceptionally();
        assertThat(sharedSlot.isEmpty()).isFalse();
        assertThat(released).isNotDone();
    }

    @Test
    void testCancelPendingLogicalSlotRequest() {
        CompletableFuture<ExecutionSlotSharingGroup> released = new CompletableFuture<>();
        SharedSlot sharedSlot =
                SharedSlotBuilder.newBuilder()
                        .withExternalReleaseCallback(released::complete)
                        .build();
        CompletableFuture<LogicalSlot> logicalSlotFuture = sharedSlot.allocateLogicalSlot(EV1);
        Throwable cause = new Throwable();
        sharedSlot.cancelLogicalSlotRequest(EV1, cause);

        assertThat(logicalSlotFuture.isCompletedExceptionally()).isTrue();
        assertThatThrownBy(logicalSlotFuture::get)
                .isInstanceOf(ExecutionException.class)
                .hasCause(cause);
        assertThat(sharedSlot.isEmpty()).isTrue();
        assertThatFuture(released).isDone();
    }

    @Test
    void testReturnAllocatedLogicalSlot() {
        CompletableFuture<PhysicalSlot> slotContextFuture = new CompletableFuture<>();
        CompletableFuture<ExecutionSlotSharingGroup> released = new CompletableFuture<>();
        SharedSlot sharedSlot =
                SharedSlotBuilder.newBuilder()
                        .withSlotContextFuture(slotContextFuture)
                        .withExternalReleaseCallback(released::complete)
                        .build();
        CompletableFuture<LogicalSlot> logicalSlotFuture = sharedSlot.allocateLogicalSlot(EV1);
        slotContextFuture.complete(new TestingPhysicalSlot(RP, new AllocationID()));
        sharedSlot.returnLogicalSlot(logicalSlotFuture.join());

        assertThat(sharedSlot.isEmpty()).isTrue();
        assertThatFuture(released).isDone();
    }

    @Test
    void testReleaseIfPhysicalSlotRequestIsIncomplete() {
        CompletableFuture<PhysicalSlot> slotContextFuture = new CompletableFuture<>();
        CompletableFuture<ExecutionSlotSharingGroup> released = new CompletableFuture<>();
        SharedSlot sharedSlot =
                SharedSlotBuilder.newBuilder()
                        .withSlotContextFuture(slotContextFuture)
                        .withExternalReleaseCallback(released::complete)
                        .build();
        sharedSlot.allocateLogicalSlot(EV1);

        assertThatThrownBy(() -> sharedSlot.release(new Throwable()))
                .withFailMessage(
                        "IllegalStateException is expected trying to release a shared slot with incomplete physical slot request")
                .isInstanceOf(IllegalStateException.class);
        assertThat(sharedSlot.isEmpty()).isFalse();
        assertThatFuture(released).isNotDone();
    }

    @Test
    void testReleaseIfPhysicalSlotIsAllocated() {
        CompletableFuture<PhysicalSlot> slotContextFuture =
                CompletableFuture.completedFuture(new TestingPhysicalSlot(RP, new AllocationID()));
        CompletableFuture<ExecutionSlotSharingGroup> released = new CompletableFuture<>();
        SharedSlot sharedSlot =
                SharedSlotBuilder.newBuilder()
                        .withSlotContextFuture(slotContextFuture)
                        .withExternalReleaseCallback(released::complete)
                        .build();
        LogicalSlot logicalSlot = sharedSlot.allocateLogicalSlot(EV1).join();
        CompletableFuture<Object> terminalFuture = new CompletableFuture<>();
        logicalSlot.tryAssignPayload(new DummyPayload(terminalFuture));
        assertThatFuture(terminalFuture).isNotDone();
        sharedSlot.release(new Throwable());

        assertThatFuture(terminalFuture).isDone();
        assertThat(sharedSlot.isEmpty()).isTrue();
        assertThatFuture(released).isDone();
    }

    @Test
    void tesDuplicatedReturnLogicalSlotFails() {
        CompletableFuture<PhysicalSlot> slotContextFuture =
                CompletableFuture.completedFuture(new TestingPhysicalSlot(RP, new AllocationID()));
        AtomicInteger released = new AtomicInteger(0);
        SharedSlot sharedSlot =
                SharedSlotBuilder.newBuilder()
                        .withSlotContextFuture(slotContextFuture)
                        .withExternalReleaseCallback(g -> released.incrementAndGet())
                        .build();
        LogicalSlot logicalSlot = sharedSlot.allocateLogicalSlot(EV1).join();
        sharedSlot.returnLogicalSlot(logicalSlot);
        assertThatThrownBy(() -> sharedSlot.returnLogicalSlot(logicalSlot))
                .withFailMessage(
                        "Duplicated 'returnLogicalSlot' call should fail with IllegalStateException")
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testReleaseEmptyDoesNotCallAllocatorReleaseBack() {
        CompletableFuture<PhysicalSlot> slotContextFuture =
                CompletableFuture.completedFuture(new TestingPhysicalSlot(RP, new AllocationID()));
        CompletableFuture<SharedSlot> sharedSlotReleaseFuture = new CompletableFuture<>();
        AtomicInteger released = new AtomicInteger(0);
        SharedSlot sharedSlot =
                SharedSlotBuilder.newBuilder()
                        .withSlotContextFuture(slotContextFuture)
                        .withExternalReleaseCallback(
                                g -> {
                                    // checks that release -> externalReleaseCallback -> release
                                    // does not lead to infinite recursion
                                    // due to SharedSlot.state.RELEASED check
                                    sharedSlotReleaseFuture.join().release(new Throwable());
                                    released.incrementAndGet();
                                })
                        .build();
        sharedSlotReleaseFuture.complete(sharedSlot);
        LogicalSlot logicalSlot = sharedSlot.allocateLogicalSlot(EV1).join();

        assertThat(released).hasValue(0);
        // returns the only and last slot, calling the external release callback
        sharedSlot.returnLogicalSlot(logicalSlot);
        assertThat(released).hasValue(1);

        // slot is already released, it should not get released again
        sharedSlot.release(new Throwable());
        assertThat(released).hasValue(1);
    }

    @Test
    void testReturnLogicalSlotWhileReleasingDoesNotCauseConcurrentModificationException() {
        CompletableFuture<PhysicalSlot> slotContextFuture =
                CompletableFuture.completedFuture(new TestingPhysicalSlot(RP, new AllocationID()));
        SharedSlot sharedSlot =
                SharedSlotBuilder.newBuilder().withSlotContextFuture(slotContextFuture).build();
        LogicalSlot logicalSlot1 = sharedSlot.allocateLogicalSlot(EV1).join();
        LogicalSlot logicalSlot2 = sharedSlot.allocateLogicalSlot(EV2).join();
        logicalSlot1.tryAssignPayload(
                new LogicalSlot.Payload() {
                    @Override
                    public void fail(Throwable cause) {
                        sharedSlot.returnLogicalSlot(logicalSlot2);
                    }

                    @Override
                    public CompletableFuture<?> getTerminalStateFuture() {
                        return CompletableFuture.completedFuture(null);
                    }
                });
        sharedSlot.release(new Throwable());
    }

    private static class SharedSlotBuilder {
        private CompletableFuture<PhysicalSlot> slotContextFuture = new CompletableFuture<>();
        private boolean slotWillBeOccupiedIndefinitely = false;
        private Consumer<ExecutionSlotSharingGroup> externalReleaseCallback = group -> {};

        private SharedSlotBuilder withSlotContextFuture(
                CompletableFuture<PhysicalSlot> slotContextFuture) {
            this.slotContextFuture = slotContextFuture;
            return this;
        }

        private SharedSlotBuilder slotWillBeOccupiedIndefinitely() {
            this.slotWillBeOccupiedIndefinitely = true;
            return this;
        }

        private SharedSlotBuilder withExternalReleaseCallback(
                Consumer<ExecutionSlotSharingGroup> releaseCallback) {
            this.externalReleaseCallback = releaseCallback;
            return this;
        }

        private SharedSlot build() {
            return new SharedSlot(
                    PHYSICAL_SLOT_REQUEST_ID,
                    RP,
                    SG,
                    slotContextFuture,
                    slotWillBeOccupiedIndefinitely,
                    externalReleaseCallback);
        }

        private static SharedSlotBuilder newBuilder() {
            return new SharedSlotBuilder();
        }
    }
}
