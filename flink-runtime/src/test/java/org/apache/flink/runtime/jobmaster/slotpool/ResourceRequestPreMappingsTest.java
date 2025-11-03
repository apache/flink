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
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.scheduler.adaptive.allocator.TestingSlot;
import org.apache.flink.runtime.scheduler.loading.DefaultLoadingWeight;
import org.apache.flink.util.Preconditions;

import org.junit.jupiter.api.Test;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link ResourceRequestPreMappings}. */
class ResourceRequestPreMappingsTest {

    private static final ResourceProfile smallFineGrainedResourceProfile =
            ResourceProfile.newBuilder().setManagedMemoryMB(10).build();

    private static final ResourceProfile bigGrainedResourceProfile =
            ResourceProfile.newBuilder().setManagedMemoryMB(20).build();

    @Test
    void testIncludeInvalidProfileOfRequestOrResource() {
        // For invalid resource.
        ResourceProfile[] profiles =
                new ResourceProfile[] {ResourceProfile.UNKNOWN, ResourceProfile.ZERO};
        for (ResourceProfile profile : profiles) {
            assertThatThrownBy(
                            () ->
                                    ResourceRequestPreMappings.createFrom(
                                            Collections.emptyList(), newTestingSlots(profile)))
                    .isInstanceOf(IllegalStateException.class);
        }

        // For invalid request.
        profiles = new ResourceProfile[] {ResourceProfile.ANY, ResourceProfile.ZERO};
        for (ResourceProfile profile : profiles) {
            assertThatThrownBy(
                            () ->
                                    ResourceRequestPreMappings.createFrom(
                                            newPendingRequests(profile), Collections.emptyList()))
                    .isInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    void testBuildWhenUnavailableTotalResourcesOrEmptyRequestsResources() {
        // Testing for unavailable total resource
        ResourceRequestPreMappings preMappings =
                ResourceRequestPreMappings.createFrom(
                        newPendingRequests(ResourceProfile.UNKNOWN), Collections.emptyList());
        assertThat(preMappings.isMatchingFulfilled()).isFalse();
        assertThat(preMappings.getBaseRequiredResourcePreMappings()).isEmpty();

        // Testing for empty slots or requests
        preMappings =
                ResourceRequestPreMappings.createFrom(
                        Collections.emptyList(), Collections.emptyList());
        assertNotMatchable(preMappings);
    }

    @Test
    void testBuildWhenMissingResourceToMatchFineGrainedRequest() {

        // Testing for missing available fine-grained resources when only fine-grained request
        ResourceRequestPreMappings preMappings =
                ResourceRequestPreMappings.createFrom(
                        newPendingRequests(
                                smallFineGrainedResourceProfile,
                                smallFineGrainedResourceProfile,
                                bigGrainedResourceProfile),
                        newTestingSlots(
                                smallFineGrainedResourceProfile,
                                smallFineGrainedResourceProfile,
                                smallFineGrainedResourceProfile));
        assertNotMatchable(preMappings);

        // Testing for missing available fine-grained resources when fine-grained and unknown
        // requests.
        preMappings =
                ResourceRequestPreMappings.createFrom(
                        newPendingRequests(
                                ResourceProfile.UNKNOWN,
                                smallFineGrainedResourceProfile,
                                bigGrainedResourceProfile),
                        newTestingSlots(
                                smallFineGrainedResourceProfile,
                                smallFineGrainedResourceProfile,
                                smallFineGrainedResourceProfile));
        assertNotMatchable(preMappings);
    }

    @Test
    void testBuildSuccessfullyThatFinedGrainedMatchedExactly() {
        ResourceRequestPreMappings preMappings =
                ResourceRequestPreMappings.createFrom(
                        newPendingRequests(
                                smallFineGrainedResourceProfile,
                                smallFineGrainedResourceProfile,
                                bigGrainedResourceProfile),
                        newTestingSlots(
                                bigGrainedResourceProfile,
                                smallFineGrainedResourceProfile,
                                smallFineGrainedResourceProfile,
                                smallFineGrainedResourceProfile));
        assertThat(preMappings.isMatchingFulfilled()).isTrue();
        assertThat(preMappings.getBaseRequiredResourcePreMappings())
                .hasSize(2)
                .contains(
                        new AbstractMap.SimpleEntry<>(
                                smallFineGrainedResourceProfile,
                                new HashMap<>() {
                                    {
                                        put(smallFineGrainedResourceProfile, 2);
                                    }
                                }),
                        new AbstractMap.SimpleEntry<>(
                                bigGrainedResourceProfile,
                                new HashMap<>() {
                                    {
                                        put(bigGrainedResourceProfile, 1);
                                    }
                                }));
        assertThat(preMappings.getRemainingFlexibleResources())
                .contains(new AbstractMap.SimpleEntry<>(smallFineGrainedResourceProfile, 1));
    }

    @Test
    void testBuildSuccessfullyThatFinedGrainedToMatchedUnknownRequests() {

        // Testing for available all resources and no UNKNOWN required resource.
        ResourceRequestPreMappings preMappings =
                ResourceRequestPreMappings.createFrom(
                        newPendingRequests(
                                ResourceProfile.UNKNOWN,
                                ResourceProfile.UNKNOWN,
                                smallFineGrainedResourceProfile,
                                bigGrainedResourceProfile),
                        newTestingSlots(
                                bigGrainedResourceProfile,
                                bigGrainedResourceProfile,
                                bigGrainedResourceProfile,
                                ResourceProfile.ANY,
                                smallFineGrainedResourceProfile,
                                smallFineGrainedResourceProfile));
        assertThat(preMappings.isMatchingFulfilled()).isTrue();
        assertThat(preMappings.getBaseRequiredResourcePreMappings())
                .hasSize(3)
                .contains(
                        new AbstractMap.SimpleEntry<>(
                                smallFineGrainedResourceProfile,
                                new HashMap<>() {
                                    {
                                        put(smallFineGrainedResourceProfile, 1);
                                    }
                                }),
                        new AbstractMap.SimpleEntry<>(
                                bigGrainedResourceProfile,
                                new HashMap<>() {
                                    {
                                        put(bigGrainedResourceProfile, 1);
                                    }
                                }));
        Map<ResourceProfile, Integer> unknownBaseMapping =
                preMappings.getBaseRequiredResourcePreMappings().get(ResourceProfile.UNKNOWN);
        assertThat(unknownBaseMapping.values().stream().reduce(0, Integer::sum)).isEqualTo(2);
        assertThat(
                        preMappings.getRemainingFlexibleResources().values().stream()
                                .reduce(0, Integer::sum))
                .isEqualTo(2);
    }

    @Test
    void testBuildSuccessfullyThatAnyToMatchedUnknownAndFineGrainedRequests() {

        // Testing for available all resources and no UNKNOWN required resource.
        ResourceRequestPreMappings preMappings =
                ResourceRequestPreMappings.createFrom(
                        newPendingRequests(
                                ResourceProfile.UNKNOWN,
                                ResourceProfile.UNKNOWN,
                                smallFineGrainedResourceProfile,
                                smallFineGrainedResourceProfile,
                                bigGrainedResourceProfile,
                                bigGrainedResourceProfile),
                        newTestingSlots(
                                bigGrainedResourceProfile,
                                smallFineGrainedResourceProfile,
                                ResourceProfile.ANY,
                                ResourceProfile.ANY,
                                ResourceProfile.ANY,
                                ResourceProfile.ANY));
        assertThat(preMappings.isMatchingFulfilled()).isTrue();
        assertThat(preMappings.getBaseRequiredResourcePreMappings())
                .hasSize(3)
                .contains(
                        new AbstractMap.SimpleEntry<>(
                                smallFineGrainedResourceProfile,
                                new HashMap<>() {
                                    {
                                        put(smallFineGrainedResourceProfile, 1);
                                        put(ResourceProfile.ANY, 1);
                                    }
                                }),
                        new AbstractMap.SimpleEntry<>(
                                bigGrainedResourceProfile,
                                new HashMap<>() {
                                    {
                                        put(bigGrainedResourceProfile, 1);
                                        put(ResourceProfile.ANY, 1);
                                    }
                                }),
                        new AbstractMap.SimpleEntry<>(
                                ResourceProfile.UNKNOWN,
                                new HashMap<>() {
                                    {
                                        put(ResourceProfile.ANY, 2);
                                    }
                                }));
        assertThat(
                        preMappings.getRemainingFlexibleResources().values().stream()
                                .reduce(0, Integer::sum))
                .isZero();
    }

    @Test
    void testHasAvailableProfile() {
        ResourceRequestPreMappings mappings =
                ResourceRequestPreMappings.createFrom(
                        newPendingRequests(ResourceProfile.UNKNOWN, ResourceProfile.UNKNOWN),
                        newTestingSlots(
                                smallFineGrainedResourceProfile,
                                smallFineGrainedResourceProfile,
                                smallFineGrainedResourceProfile));

        // Testing available resource in flexible resources
        assertThat(
                        mappings.hasAvailableProfile(
                                smallFineGrainedResourceProfile, smallFineGrainedResourceProfile))
                .isTrue();
        assertThat(
                        mappings.hasAvailableProfile(
                                smallFineGrainedResourceProfile, bigGrainedResourceProfile))
                .isFalse();

        // Testing available resource in base mapping resources
        assertThat(
                        mappings.hasAvailableProfile(
                                ResourceProfile.UNKNOWN, smallFineGrainedResourceProfile))
                .isTrue();
        assertThat(mappings.hasAvailableProfile(ResourceProfile.UNKNOWN, bigGrainedResourceProfile))
                .isFalse();
    }

    @Test
    void testDecrease() {
        // Testing decrease resource in base mapping
        ResourceRequestPreMappings mappings =
                ResourceRequestPreMappings.createFrom(
                        newPendingRequests(ResourceProfile.UNKNOWN, ResourceProfile.UNKNOWN),
                        newTestingSlots(
                                smallFineGrainedResourceProfile,
                                smallFineGrainedResourceProfile,
                                smallFineGrainedResourceProfile));

        // Testing decrease resource in base mapping resources successfully
        mappings.decrease(ResourceProfile.UNKNOWN, smallFineGrainedResourceProfile);
        assertThat(
                        mappings.getAvailableResourceCntOfBasePreMappings(
                                ResourceProfile.UNKNOWN, smallFineGrainedResourceProfile))
                .isOne();
        // Testing decrease resource in base mapping resources failed
        assertThatThrownBy(
                        () ->
                                mappings.decrease(
                                        smallFineGrainedResourceProfile,
                                        smallFineGrainedResourceProfile))
                .isInstanceOf(IllegalStateException.class);

        // Testing decrease resource in flexible resources
        ResourceRequestPreMappings mappings2 =
                ResourceRequestPreMappings.createFrom(
                        true,
                        new HashMap<>() {
                            {
                                put(
                                        ResourceProfile.UNKNOWN,
                                        new HashMap<>() {
                                            {
                                                put(smallFineGrainedResourceProfile, 2);
                                            }
                                        });
                            }
                        },
                        new HashMap<>() {
                            {
                                put(smallFineGrainedResourceProfile, 1);
                                put(bigGrainedResourceProfile, 2);
                            }
                        });
        // Testing decrease resource in flexible resources successfully
        mappings2.decrease(ResourceProfile.UNKNOWN, bigGrainedResourceProfile);
        assertThat(
                        mappings2.getAvailableResourceCntOfRemainingFlexibleMapping(
                                bigGrainedResourceProfile))
                .isOne();
        assertThat(
                        mappings2.getAvailableResourceCntOfBasePreMappings(
                                ResourceProfile.UNKNOWN, smallFineGrainedResourceProfile))
                .isOne();
        assertThat(
                        mappings2.getAvailableResourceCntOfRemainingFlexibleMapping(
                                smallFineGrainedResourceProfile))
                .isEqualTo(2);

        // Testing decrease resource in flexible resources failed
        mappings2.decrease(ResourceProfile.UNKNOWN, smallFineGrainedResourceProfile);
        assertThatThrownBy(
                        () ->
                                mappings2.decrease(
                                        ResourceProfile.UNKNOWN, smallFineGrainedResourceProfile))
                .isInstanceOf(IllegalStateException.class);
    }

    private List<PendingRequest> newPendingRequests(ResourceProfile... requiredProfiles) {
        ArrayList<PendingRequest> pendingRequests = new ArrayList<>();
        if (requiredProfiles == null || requiredProfiles.length == 0) {
            return pendingRequests;
        }
        for (ResourceProfile requiredProfile : requiredProfiles) {
            pendingRequests.add(
                    PendingRequest.createNormalRequest(
                            new SlotRequestId(),
                            Preconditions.checkNotNull(requiredProfile),
                            DefaultLoadingWeight.EMPTY,
                            Collections.emptyList()));
        }
        return pendingRequests;
    }

    private List<TestingSlot> newTestingSlots(ResourceProfile... slotProfiles) {
        ArrayList<TestingSlot> slots = new ArrayList<>();
        if (slotProfiles == null || slotProfiles.length == 0) {
            return slots;
        }
        for (ResourceProfile slotProfile : slotProfiles) {
            slots.add(new TestingSlot(Preconditions.checkNotNull(slotProfile)));
        }
        return slots;
    }

    private void assertNotMatchable(ResourceRequestPreMappings preMappings) {
        assertThat(preMappings.isMatchingFulfilled()).isFalse();
        assertThat(preMappings.getBaseRequiredResourcePreMappings()).isEmpty();
    }
}
