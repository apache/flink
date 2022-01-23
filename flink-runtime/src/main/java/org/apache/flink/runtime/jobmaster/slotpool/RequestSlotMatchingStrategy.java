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

import java.util.Collection;

/** Strategy to match slot requests to slots. */
public interface RequestSlotMatchingStrategy {

    /**
     * Match the given slots with the given collection of pending requests.
     *
     * @param slots slots to match
     * @param pendingRequests slot requests to match
     * @return resulting matches of this operation
     */
    Collection<RequestSlotMatch> matchRequestsAndSlots(
            Collection<? extends PhysicalSlot> slots, Collection<PendingRequest> pendingRequests);

    /** Result class representing matches. */
    final class RequestSlotMatch {
        private final PendingRequest pendingRequest;
        private final PhysicalSlot matchedSlot;

        private RequestSlotMatch(PendingRequest pendingRequest, PhysicalSlot matchedSlot) {
            this.pendingRequest = pendingRequest;
            this.matchedSlot = matchedSlot;
        }

        PhysicalSlot getSlot() {
            return matchedSlot;
        }

        PendingRequest getPendingRequest() {
            return pendingRequest;
        }

        static RequestSlotMatch createFor(PendingRequest pendingRequest, PhysicalSlot newSlot) {
            return new RequestSlotMatch(pendingRequest, newSlot);
        }
    }
}
