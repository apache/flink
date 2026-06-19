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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Preconditions;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * This class is designed to handle the pre-matching of resource requests in the context of balanced
 * task scheduling for streaming jobs. During the batch allocation of resources, where resource
 * requests are allocated in a single, non-interleaved operation, it is impossible to make immediate
 * individual adjustments to unmatched resource requests. This may lead to situations where not all
 * resource requests can be successfully fulfilled. For example:
 *
 * <pre>
 * resource requests:
 *  - resource request-1: ResourceProfile-1(UNKNOWN)
 *  - resource request-2: ResourceProfile-2(cpu=2 core, memory=2G)
 *
 * available slots:
 *  - slot-a: ResourceProfile-a(cpu=1 core, memory=1G)
 *  - slot-b: ResourceProfile-b(cpu=2 core, memory=2G)
 * </pre>
 *
 * When the strategy {@link TasksBalancedRequestSlotMatchingStrategy} performs resource allocation,
 * the following matching mapping might occur, preventing all slot requests from being successfully
 * assigned in a consistent manner and thus hindering the scheduling of the entire job:
 *
 * <pre>
 * the unexpected mapping case:
 *   - resource request-1: ResourceProfile-1(UNKNOWN) was matched with slot-b: ResourceProfile-b(cpu=2 core, memory=2G)
 *   - resource request-2: ResourceProfile-2(cpu=2 core, memory=2G) was not matched
 * </pre>
 *
 * Therefore, it is crucial to determine how ResourceProfiles should match before the batch
 * allocation of resource requests, aiming to assure the allocation successfully at least. An ideal
 * matching relationship would be:
 *
 * <pre>
 * - ResourceProfile-1(UNKNOWN)               -> ResourceProfile-a(cpu=1 core, memory=1G)
 * - ResourceProfile-2(cpu=2 core, memory=2G) -> ResourceProfile-b(cpu=2 core, memory=2G)
 * </pre>
 *
 * This is the motivation for introducing the current class.
 */
final class ResourceRequestPreMappings {

    private final boolean matchingFulfilled;
    // The variable to keep base mappings result related information, which can assure that
    // the allocation for all requests could be run successfully at least.
    private final Map<ResourceProfile, Map<ResourceProfile, Integer>>
            baseRequiredResourcePreMappings;
    // The variable to keep the remaining available flexible resources besides the
    // baseRequiredResourcePreMappings.
    private final Map<ResourceProfile, Integer> remainingFlexibleResources;

    private ResourceRequestPreMappings(
            boolean matchingFulfilled,
            final Map<ResourceProfile, Map<ResourceProfile, Integer>>
                    baseRequiredResourcePreMappings,
            final Map<ResourceProfile, Integer> remainingFlexibleResources) {
        this.matchingFulfilled = matchingFulfilled;

        this.baseRequiredResourcePreMappings =
                CollectionUtil.newHashMapWithExpectedSize(baseRequiredResourcePreMappings.size());
        this.baseRequiredResourcePreMappings.putAll(baseRequiredResourcePreMappings);

        this.remainingFlexibleResources =
                CollectionUtil.newHashMapWithExpectedSize(remainingFlexibleResources.size());
        this.remainingFlexibleResources.putAll(remainingFlexibleResources);
    }

    static ResourceRequestPreMappings createFrom(
            Collection<PendingRequest> pendingRequests, Collection<? extends PhysicalSlot> slots) {
        return new ResourceRequestPreMappingsBuilder(pendingRequests, slots).build();
    }

    boolean isMatchingFulfilled() {
        return matchingFulfilled;
    }

    boolean hasAvailableProfile(
            ResourceProfile requiredResourceProfile, ResourceProfile acquirableResourceProfile) {
        // Check for base mappings first
        Map<ResourceProfile, Integer> basePreMapping =
                baseRequiredResourcePreMappings.getOrDefault(
                        requiredResourceProfile, new HashMap<>());
        Integer remainingCnt = basePreMapping.getOrDefault(acquirableResourceProfile, 0);

        if (remainingCnt > 0) {
            return true;
        } else {
            return remainingFlexibleResources.getOrDefault(acquirableResourceProfile, 0) > 0;
        }
    }

    void decrease(
            ResourceProfile requiredResourceProfile, ResourceProfile acquiredResourceProfile) {
        Map<ResourceProfile, Integer> basePreMapping =
                baseRequiredResourcePreMappings.getOrDefault(
                        requiredResourceProfile, new HashMap<>());
        Integer remainingCntOfBaseMappings =
                basePreMapping.getOrDefault(acquiredResourceProfile, 0);
        Integer remainingCntOfFlexibleResources =
                remainingFlexibleResources.getOrDefault(acquiredResourceProfile, 0);

        Preconditions.checkState(
                remainingCntOfBaseMappings > 0 || remainingCntOfFlexibleResources > 0,
                "Remaining acquired resource profile %s to match %s is not enough.",
                acquiredResourceProfile,
                requiredResourceProfile);

        if (remainingCntOfBaseMappings > 0) {
            basePreMapping.put(acquiredResourceProfile, remainingCntOfBaseMappings - 1);
            return;
        }

        if (remainingCntOfFlexibleResources > 0) {
            remainingFlexibleResources.put(
                    acquiredResourceProfile, remainingCntOfFlexibleResources - 1);
            // release a resource back to remainingFlexibleResources.
            adjustBaseToRemainingFlexibleResources(basePreMapping);
        }
    }

    private void adjustBaseToRemainingFlexibleResources(
            Map<ResourceProfile, Integer> basePreMapping) {
        Optional<Map.Entry<ResourceProfile, Integer>> releasableOptOfBaseMappings =
                basePreMapping.entrySet().stream()
                        .filter(entry -> entry.getValue() > 0)
                        .findFirst();
        Preconditions.checkState(
                releasableOptOfBaseMappings.isPresent(),
                "No releasable mapping found in the base mappings between resources and requests.");
        Map.Entry<ResourceProfile, Integer> releasable = releasableOptOfBaseMappings.get();
        ResourceProfile releasableResourceProfile = releasable.getKey();

        basePreMapping.put(releasableResourceProfile, releasable.getValue() - 1);

        remainingFlexibleResources.compute(
                releasableResourceProfile,
                (resourceProfile, oldValue) -> oldValue == null ? 1 : oldValue + 1);
    }

    @VisibleForTesting
    static ResourceRequestPreMappings createFrom(
            boolean allMatchable,
            final Map<ResourceProfile, Map<ResourceProfile, Integer>>
                    baseRequiredResourcePreMappings,
            final Map<ResourceProfile, Integer> remainingFlexibleResources) {
        return new ResourceRequestPreMappings(
                allMatchable, baseRequiredResourcePreMappings, remainingFlexibleResources);
    }

    @VisibleForTesting
    Map<ResourceProfile, Map<ResourceProfile, Integer>> getBaseRequiredResourcePreMappings() {
        return Collections.unmodifiableMap(baseRequiredResourcePreMappings);
    }

    @VisibleForTesting
    int getAvailableResourceCntOfBasePreMappings(
            ResourceProfile requiredResourceProfile, ResourceProfile acquirableResourceProfile) {
        return baseRequiredResourcePreMappings
                .getOrDefault(requiredResourceProfile, new HashMap<>())
                .getOrDefault(acquirableResourceProfile, 0);
    }

    @VisibleForTesting
    Map<ResourceProfile, Integer> getRemainingFlexibleResources() {
        return Collections.unmodifiableMap(remainingFlexibleResources);
    }

    @VisibleForTesting
    int getAvailableResourceCntOfRemainingFlexibleMapping(
            ResourceProfile availableResourceProfile) {
        return remainingFlexibleResources.getOrDefault(availableResourceProfile, 0);
    }

    private static final class ResourceRequestPreMappingsBuilder {

        private final Map<ResourceProfile, Integer> unfulfilledRequired;
        private final Map<ResourceProfile, Integer> availableResources;

        // The variable to maintain the base mappings result related information, which can
        // assure that the allocation for all requests could be run successfully at least.
        private final Map<ResourceProfile, Map<ResourceProfile, Integer>>
                baseRequiredResourcePreMappings;

        private ResourceRequestPreMappingsBuilder(
                Collection<PendingRequest> pendingRequests,
                Collection<? extends PhysicalSlot> slots) {
            this.unfulfilledRequired =
                    pendingRequests.stream()
                            .collect(
                                    Collectors.groupingBy(
                                            PendingRequest::getResourceProfile,
                                            Collectors.summingInt(ignored -> 1)));
            this.unfulfilledRequired
                    .keySet()
                    .forEach(
                            rp ->
                                    Preconditions.checkState(
                                            !rp.equals(ResourceProfile.ZERO)
                                                    && !rp.equals(ResourceProfile.ANY),
                                            "The required resource must not be ResourceProfile.ZERO and ResourceProfile.ANY."));
            this.availableResources =
                    slots.stream()
                            .collect(
                                    Collectors.groupingBy(
                                            PhysicalSlot::getResourceProfile,
                                            Collectors.summingInt(ignored -> 1)));
            this.availableResources
                    .keySet()
                    .forEach(
                            rp ->
                                    Preconditions.checkState(
                                            !rp.equals(ResourceProfile.UNKNOWN)
                                                    && !rp.equals(ResourceProfile.ZERO),
                                            "The resource profile of a slot must not be ResourceProfile.UNKNOWN and ResourceProfile.ZERO."));
            this.baseRequiredResourcePreMappings =
                    CollectionUtil.newHashMapWithExpectedSize(slots.size());
        }

        private ResourceRequestPreMappings build() {
            if (unfulfilledRequired.isEmpty()
                    || availableResources.isEmpty()
                    || !canFulfillDesiredResources()) {
                return currentPreMappings(false);
            }

            buildFineGrainedRequestFulfilledExactMapping();
            if (isMatchingFulfilled()) {
                return currentPreMappings(true);
            }

            buildRemainingFineGrainedRequestFulfilledAnyMapping();
            if (isMatchingFulfilled()) {
                return currentPreMappings(true);
            }

            buildUnknownRequestFulfilledMapping();
            return currentPreMappings(isMatchingFulfilled());
        }

        private void buildFineGrainedRequestFulfilledExactMapping() {
            for (Map.Entry<ResourceProfile, Integer> unfulfilledEntry :
                    new HashMap<>(unfulfilledRequired).entrySet()) {
                ResourceProfile requiredFineGrainedResourceProfile = unfulfilledEntry.getKey();
                if (ResourceProfile.UNKNOWN.equals(requiredFineGrainedResourceProfile)) {
                    continue;
                }
                // checking fine-grained
                int unfulfilledFineGrainedRequiredCnt = unfulfilledEntry.getValue();
                int availableFineGrainedResourceCnt =
                        availableResources.getOrDefault(requiredFineGrainedResourceProfile, 0);
                if (unfulfilledFineGrainedRequiredCnt <= 0
                        || availableFineGrainedResourceCnt <= 0) {
                    continue;
                }

                int diff = unfulfilledFineGrainedRequiredCnt - availableFineGrainedResourceCnt;

                Map<ResourceProfile, Integer> fulfilledProfileCount =
                        baseRequiredResourcePreMappings.computeIfAbsent(
                                requiredFineGrainedResourceProfile, ignored -> new HashMap<>());
                fulfilledProfileCount.put(
                        requiredFineGrainedResourceProfile,
                        diff > 0
                                ? availableFineGrainedResourceCnt
                                : unfulfilledFineGrainedRequiredCnt);

                int newUnfulfilledFineGrainedRequiredCnt = Math.max(diff, 0);
                int unAvailableFineGrainedResourceCnt = Math.max(-diff, 0);
                availableResources.put(
                        requiredFineGrainedResourceProfile, unAvailableFineGrainedResourceCnt);
                unfulfilledRequired.put(
                        requiredFineGrainedResourceProfile, newUnfulfilledFineGrainedRequiredCnt);
            }
        }

        private void buildRemainingFineGrainedRequestFulfilledAnyMapping() {
            Integer availableResourceProfileANYCount =
                    availableResources.getOrDefault(ResourceProfile.ANY, 0);
            if (availableResourceProfileANYCount <= 0) {
                return;
            }

            for (Map.Entry<ResourceProfile, Integer> unfulfilledEntry :
                    new HashMap<>(unfulfilledRequired).entrySet()) {
                availableResourceProfileANYCount =
                        availableResources.getOrDefault(ResourceProfile.ANY, 0);

                if (availableResourceProfileANYCount <= 0) {
                    return;
                }
                ResourceProfile fineGrainedRequestResourceProfile = unfulfilledEntry.getKey();
                if (ResourceProfile.UNKNOWN.equals(fineGrainedRequestResourceProfile)) {
                    continue;
                }
                // checking fine-grained
                int unfulfilledFineGrainedRequiredCnt =
                        unfulfilledRequired.getOrDefault(fineGrainedRequestResourceProfile, 0);
                if (unfulfilledFineGrainedRequiredCnt <= 0) {
                    continue;
                }

                int diff = unfulfilledFineGrainedRequiredCnt - availableResourceProfileANYCount;

                Map<ResourceProfile, Integer> fulfilledProfileCount =
                        baseRequiredResourcePreMappings.computeIfAbsent(
                                fineGrainedRequestResourceProfile, ignored -> new HashMap<>());
                fulfilledProfileCount.put(
                        ResourceProfile.ANY,
                        diff > 0
                                ? availableResourceProfileANYCount
                                : unfulfilledFineGrainedRequiredCnt);

                int newUnfulfilledFineGrainedRequiredCnt = Math.max(diff, 0);
                int newAvailableResourceProfileANYCount = Math.max(-diff, 0);
                availableResources.put(ResourceProfile.ANY, newAvailableResourceProfileANYCount);
                unfulfilledRequired.put(
                        fineGrainedRequestResourceProfile, newUnfulfilledFineGrainedRequiredCnt);
            }
        }

        private void buildUnknownRequestFulfilledMapping() {
            if (unfulfilledRequired.getOrDefault(ResourceProfile.UNKNOWN, 0) <= 0) {
                return;
            }

            for (Map.Entry<ResourceProfile, Integer> availableResourceEntry :
                    new HashMap<>(availableResources).entrySet()) {
                Integer unfulfilledUnknownRequiredCnt =
                        unfulfilledRequired.getOrDefault(ResourceProfile.UNKNOWN, 0);
                ResourceProfile availableResourceProfile = availableResourceEntry.getKey();
                int availableResourceCnt =
                        availableResources.getOrDefault(availableResourceProfile, 0);
                if (availableResourceCnt <= 0) {
                    continue;
                }
                if (unfulfilledUnknownRequiredCnt <= 0) {
                    return;
                }
                int diff = unfulfilledUnknownRequiredCnt - availableResourceCnt;
                Map<ResourceProfile, Integer> fulfilledProfileCount =
                        baseRequiredResourcePreMappings.computeIfAbsent(
                                ResourceProfile.UNKNOWN, ignored -> new HashMap<>());
                fulfilledProfileCount.put(
                        availableResourceProfile,
                        diff > 0 ? availableResourceCnt : unfulfilledUnknownRequiredCnt);

                int newUnfulfilledUnknownRequiredCnt = Math.max(diff, 0);
                int newAvailableResourceCnt = Math.max(-diff, 0);
                availableResources.put(availableResourceProfile, newAvailableResourceCnt);
                unfulfilledRequired.put(ResourceProfile.UNKNOWN, newUnfulfilledUnknownRequiredCnt);
            }
        }

        private ResourceRequestPreMappings currentPreMappings(boolean matchingFulfilled) {
            if (!matchingFulfilled) {
                return new ResourceRequestPreMappings(false, new HashMap<>(), new HashMap<>());
            }
            return new ResourceRequestPreMappings(
                    true,
                    Collections.unmodifiableMap(baseRequiredResourcePreMappings),
                    Collections.unmodifiableMap(availableResources));
        }

        private boolean isMatchingFulfilled() {
            for (ResourceProfile unfulfilledProfile : unfulfilledRequired.keySet()) {
                Integer unfulfilled = unfulfilledRequired.getOrDefault(unfulfilledProfile, 0);
                if (unfulfilled > 0) {
                    return false;
                }
            }
            return true;
        }

        private boolean canFulfillDesiredResources() {
            Integer totalUnfulfilledCnt =
                    unfulfilledRequired.values().stream().reduce(0, Integer::sum);
            Integer totalAvailableCnt =
                    availableResources.values().stream().reduce(0, Integer::sum);
            return totalAvailableCnt >= totalUnfulfilledCnt;
        }
    }
}
