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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.jobmanager.scheduler.Locality;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;

/**
 * A logical slot represents a resource on a TaskManager into which a single task can be deployed.
 */
public interface LogicalSlot {

    Payload TERMINATED_PAYLOAD =
            new Payload() {

                private final CompletableFuture<?> completedTerminationFuture =
                        CompletableFuture.completedFuture(null);

                @Override
                public void fail(Throwable cause) {
                    // ignore
                }

                @Override
                public CompletableFuture<?> getTerminalStateFuture() {
                    return completedTerminationFuture;
                }
            };

    /**
     * Return the TaskManager location of this slot.
     *
     * @return TaskManager location of this slot
     */
    TaskManagerLocation getTaskManagerLocation();

    /**
     * Return the TaskManager gateway to talk to the TaskManager.
     *
     * @return TaskManager gateway to talk to the TaskManager
     */
    TaskManagerGateway getTaskManagerGateway();

    /**
     * Gets the locality of this slot.
     *
     * @return locality of this slot
     */
    Locality getLocality();

    /**
     * True if the slot is alive and has not been released.
     *
     * @return True if the slot is alive, otherwise false if the slot is released
     */
    boolean isAlive();

    /**
     * Tries to assign a payload to this slot. One can only assign a single payload once.
     *
     * @param payload to be assigned to this slot.
     * @return true if the payload could be assigned, otherwise false
     */
    boolean tryAssignPayload(Payload payload);

    /**
     * Returns the set payload or null if none.
     *
     * @return Payload of this slot of null if none
     */
    @Nullable
    Payload getPayload();

    /**
     * Releases this slot.
     *
     * @return Future which is completed once the slot has been released, in case of a failure it is
     *     completed exceptionally
     * @deprecated Added because extended the actual releaseSlot method with cause parameter.
     */
    default CompletableFuture<?> releaseSlot() {
        return releaseSlot(null);
    }

    /**
     * Releases this slot.
     *
     * @param cause why the slot was released or null if none
     * @return future which is completed once the slot has been released
     */
    CompletableFuture<?> releaseSlot(@Nullable Throwable cause);

    /**
     * Gets the allocation id of this slot. Multiple logical slots can share the same allocation id.
     *
     * @return allocation id of this slot
     */
    AllocationID getAllocationId();

    /**
     * Gets the slot request id uniquely identifying the request with which this slot has been
     * allocated.
     *
     * @return Unique id identifying the slot request with which this slot was allocated
     */
    SlotRequestId getSlotRequestId();

    /** Payload for a logical slot. */
    interface Payload {

        /**
         * Fail the payload with the given cause.
         *
         * @param cause of the failure
         */
        void fail(Throwable cause);

        /**
         * Gets the terminal state future which is completed once the payload has reached a terminal
         * state.
         *
         * @return Terminal state future
         */
        CompletableFuture<?> getTerminalStateFuture();
    }
}
