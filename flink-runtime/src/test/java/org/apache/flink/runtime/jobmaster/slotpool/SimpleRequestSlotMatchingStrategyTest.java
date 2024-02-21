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

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.scheduler.TestingPhysicalSlot;
import org.apache.flink.util.TestLoggerExtension;

import org.apache.flink.shaded.guava31.com.google.common.collect.Iterators;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link SimpleRequestSlotMatchingStrategy}. */
@ExtendWith(TestLoggerExtension.class)
public class SimpleRequestSlotMatchingStrategyTest {

    @Test
    public void testSlotRequestsAreMatchedInOrder() {
        final SimpleRequestSlotMatchingStrategy simpleRequestSlotMatchingStrategy =
                SimpleRequestSlotMatchingStrategy.INSTANCE;

        final Collection<PhysicalSlot> slots = Arrays.asList(TestingPhysicalSlot.builder().build());
        final PendingRequest pendingRequest1 =
                PendingRequest.createNormalRequest(
                        new SlotRequestId(), ResourceProfile.UNKNOWN, Collections.emptyList());
        final PendingRequest pendingRequest2 =
                PendingRequest.createNormalRequest(
                        new SlotRequestId(), ResourceProfile.UNKNOWN, Collections.emptyList());
        final Collection<PendingRequest> pendingRequests =
                Arrays.asList(pendingRequest1, pendingRequest2);

        final Collection<RequestSlotMatchingStrategy.RequestSlotMatch> requestSlotMatches =
                simpleRequestSlotMatchingStrategy.matchRequestsAndSlots(slots, pendingRequests);

        assertThat(requestSlotMatches).hasSize(1);
        assertThat(
                        Iterators.getOnlyElement(requestSlotMatches.iterator())
                                .getPendingRequest()
                                .getSlotRequestId())
                .isEqualTo(pendingRequest1.getSlotRequestId());
    }

    @Test
    public void testSlotRequestsThatCanBeFulfilledAreMatched() {
        final SimpleRequestSlotMatchingStrategy simpleRequestSlotMatchingStrategy =
                SimpleRequestSlotMatchingStrategy.INSTANCE;

        final ResourceProfile small = ResourceProfile.newBuilder().setCpuCores(1.0).build();
        final ResourceProfile large = ResourceProfile.newBuilder().setCpuCores(2.0).build();

        final Collection<PhysicalSlot> slots =
                Arrays.asList(
                        TestingPhysicalSlot.builder().withResourceProfile(small).build(),
                        TestingPhysicalSlot.builder().withResourceProfile(small).build());

        final PendingRequest pendingRequest1 =
                PendingRequest.createNormalRequest(
                        new SlotRequestId(), large, Collections.emptyList());
        final PendingRequest pendingRequest2 =
                PendingRequest.createNormalRequest(
                        new SlotRequestId(), small, Collections.emptyList());
        final Collection<PendingRequest> pendingRequests =
                Arrays.asList(pendingRequest1, pendingRequest2);

        final Collection<RequestSlotMatchingStrategy.RequestSlotMatch> requestSlotMatches =
                simpleRequestSlotMatchingStrategy.matchRequestsAndSlots(slots, pendingRequests);

        assertThat(requestSlotMatches).hasSize(1);
        assertThat(
                        Iterators.getOnlyElement(requestSlotMatches.iterator())
                                .getPendingRequest()
                                .getSlotRequestId())
                .isEqualTo(pendingRequest2.getSlotRequestId());
    }
}
