/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.slots.ResourceRequirement;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link JobScopedResourceTracker}. */
class JobScopedResourceTrackerTest {

    private static final ResourceProfile PROFILE_1 =
            ResourceProfile.newBuilder().setCpuCores(1).build();
    private static final ResourceProfile PROFILE_2 =
            ResourceProfile.newBuilder().setCpuCores(2).build();

    @Test
    void testInitialBehavior() {
        JobScopedResourceTracker tracker = new JobScopedResourceTracker(JobID.generate());

        assertThat(tracker.isEmpty()).isTrue();
        assertThat(tracker.getAcquiredResources()).isEmpty();
        assertThat(tracker.getMissingResources()).isEmpty();
    }

    @Test
    void testLossOfUntrackedResourceThrowsException() {
        JobScopedResourceTracker tracker = new JobScopedResourceTracker(JobID.generate());

        assertThatThrownBy(() -> tracker.notifyLostResource(ResourceProfile.UNKNOWN))
                .withFailMessage(
                        "If no resource were acquired, then a loss of resource should fail with an exception.")
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testIsEmptyForRequirementNotifications() {
        JobScopedResourceTracker tracker = new JobScopedResourceTracker(JobID.generate());

        tracker.notifyAcquiredResource(ResourceProfile.ANY);
        assertThat(tracker.isEmpty()).isFalse();
        tracker.notifyLostResource(ResourceProfile.ANY);
        assertThat(tracker.isEmpty()).isTrue();
    }

    @Test
    void testIsEmptyForResourceNotifications() {
        JobScopedResourceTracker tracker = new JobScopedResourceTracker(JobID.generate());

        tracker.notifyResourceRequirements(
                Collections.singleton(ResourceRequirement.create(ResourceProfile.UNKNOWN, 1)));
        assertThat(tracker.isEmpty()).isFalse();
        tracker.notifyResourceRequirements(Collections.emptyList());
        assertThat(tracker.isEmpty()).isTrue();
    }

    @Test
    void testRequirementsNotificationWithoutResources() {
        JobScopedResourceTracker tracker = new JobScopedResourceTracker(JobID.generate());

        ResourceRequirement[][] resourceRequirements =
                new ResourceRequirement[][] {
                    new ResourceRequirement[] {
                        ResourceRequirement.create(PROFILE_1, 4),
                        ResourceRequirement.create(PROFILE_2, 2)
                    },
                    new ResourceRequirement[] {
                        ResourceRequirement.create(PROFILE_1, 8),
                        ResourceRequirement.create(PROFILE_2, 2)
                    },
                    new ResourceRequirement[] {
                        ResourceRequirement.create(PROFILE_1, 4),
                        ResourceRequirement.create(PROFILE_2, 2)
                    }
                };

        for (ResourceRequirement[] resourceRequirement : resourceRequirements) {
            tracker.notifyResourceRequirements(Arrays.asList(resourceRequirement));

            assertThat(tracker.isEmpty()).isFalse();
            assertThat(tracker.getAcquiredResources()).isEmpty();
            assertThat(tracker.getMissingResources())
                    .containsExactlyInAnyOrder(resourceRequirement);
        }

        tracker.notifyResourceRequirements(Collections.emptyList());

        assertThat(tracker.getAcquiredResources()).isEmpty();
        assertThat(tracker.getMissingResources()).isEmpty();
    }

    @Test
    void testRequirementsNotificationWithResources() {
        JobScopedResourceTracker tracker = new JobScopedResourceTracker(JobID.generate());

        ResourceRequirement[][] resourceRequirements =
                new ResourceRequirement[][] {
                    new ResourceRequirement[] {
                        ResourceRequirement.create(PROFILE_1, 4),
                        ResourceRequirement.create(PROFILE_2, 2)
                    },
                    new ResourceRequirement[] {
                        ResourceRequirement.create(PROFILE_1, 8),
                        ResourceRequirement.create(PROFILE_2, 2)
                    },
                    new ResourceRequirement[] {
                        ResourceRequirement.create(PROFILE_1, 4),
                        ResourceRequirement.create(PROFILE_2, 2)
                    }
                };

        int numAcquiredSlotsP1 = resourceRequirements[0][0].getNumberOfRequiredSlots() - 1;
        int numAcquiredSlotsP2 = resourceRequirements[0][1].getNumberOfRequiredSlots();

        for (int x = 0; x < numAcquiredSlotsP1; x++) {
            tracker.notifyAcquiredResource(PROFILE_1);
        }
        for (int x = 0; x < numAcquiredSlotsP2; x++) {
            tracker.notifyAcquiredResource(PROFILE_2);
        }

        for (ResourceRequirement[] resourceRequirement : resourceRequirements) {
            tracker.notifyResourceRequirements(Arrays.asList(resourceRequirement));

            assertThat(tracker.getAcquiredResources())
                    .containsExactlyInAnyOrder(
                            ResourceRequirement.create(PROFILE_1, numAcquiredSlotsP1),
                            ResourceRequirement.create(PROFILE_2, numAcquiredSlotsP2));
            assertThat(tracker.getMissingResources())
                    .containsExactlyInAnyOrder(
                            ResourceRequirement.create(
                                    PROFILE_1,
                                    resourceRequirement[0].getNumberOfRequiredSlots()
                                            - numAcquiredSlotsP1));
        }

        tracker.notifyResourceRequirements(Collections.emptyList());

        assertThat(tracker.getAcquiredResources())
                .containsExactlyInAnyOrder(
                        ResourceRequirement.create(PROFILE_1, numAcquiredSlotsP1),
                        ResourceRequirement.create(PROFILE_2, numAcquiredSlotsP2));
        assertThat(tracker.getMissingResources()).isEmpty();
        ;
    }

    @Test
    void testMatchingWithResourceExceedingRequirement() {
        JobScopedResourceTracker tracker = new JobScopedResourceTracker(JobID.generate());

        tracker.notifyResourceRequirements(Arrays.asList(ResourceRequirement.create(PROFILE_1, 1)));

        tracker.notifyAcquiredResource(PROFILE_2);

        assertThat(tracker.getAcquiredResources())
                .contains(ResourceRequirement.create(PROFILE_2, 1));
    }

    @Test
    void testMatchingWithResourceLessThanRequirement() {
        JobScopedResourceTracker tracker = new JobScopedResourceTracker(JobID.generate());

        tracker.notifyResourceRequirements(Arrays.asList(ResourceRequirement.create(PROFILE_2, 1)));

        tracker.notifyAcquiredResource(PROFILE_1);

        assertThat(tracker.getAcquiredResources())
                .contains(ResourceRequirement.create(PROFILE_1, 1));
        assertThat(tracker.getMissingResources())
                .contains(ResourceRequirement.create(PROFILE_2, 1));
    }

    @Test
    void testResourceNotificationsWithoutRequirements() {
        JobScopedResourceTracker tracker = new JobScopedResourceTracker(JobID.generate());

        tracker.notifyAcquiredResource(ResourceProfile.ANY);

        assertThat(tracker.isEmpty()).isFalse();
        assertThat(tracker.getAcquiredResources())
                .contains(ResourceRequirement.create(ResourceProfile.ANY, 1));
        assertThat(tracker.getMissingResources()).isEmpty();

        tracker.notifyAcquiredResource(ResourceProfile.ANY);

        assertThat(tracker.isEmpty()).isFalse();
        assertThat(tracker.getAcquiredResources())
                .contains(ResourceRequirement.create(ResourceProfile.ANY, 2));
        assertThat(tracker.getMissingResources()).isEmpty();

        tracker.notifyLostResource(ResourceProfile.ANY);

        assertThat(tracker.isEmpty()).isFalse();
        assertThat(tracker.getAcquiredResources())
                .contains(ResourceRequirement.create(ResourceProfile.ANY, 1));
        assertThat(tracker.getMissingResources()).isEmpty();

        tracker.notifyLostResource(ResourceProfile.ANY);

        assertThat(tracker.isEmpty()).isTrue();
        assertThat(tracker.getAcquiredResources()).isEmpty();
        assertThat(tracker.getMissingResources()).isEmpty();
    }

    @Test
    void testResourceNotificationsWithRequirements() {
        JobScopedResourceTracker tracker = new JobScopedResourceTracker(JobID.generate());

        ResourceRequirement[] resourceRequirementsArray =
                new ResourceRequirement[] {
                    ResourceRequirement.create(PROFILE_1, 2),
                    ResourceRequirement.create(PROFILE_2, 1)
                };

        tracker.notifyResourceRequirements(Arrays.asList(resourceRequirementsArray));

        for (int x = 0; x < 2; x++) {
            tracker.notifyAcquiredResource(PROFILE_1);
        }

        assertThat(tracker.getAcquiredResources())
                .contains(ResourceRequirement.create(PROFILE_1, 2));
        assertThat(tracker.getMissingResources())
                .contains(ResourceRequirement.create(PROFILE_2, 1));

        tracker.notifyLostResource(PROFILE_1);

        assertThat(tracker.getAcquiredResources())
                .contains(ResourceRequirement.create(PROFILE_1, 1));
        assertThat(tracker.getMissingResources())
                .containsExactlyInAnyOrder(
                        ResourceRequirement.create(PROFILE_1, 1),
                        ResourceRequirement.create(PROFILE_2, 1));
    }

    @Test
    void testRequirementReductionRetainsExceedingResources() {
        JobScopedResourceTracker tracker = new JobScopedResourceTracker(JobID.generate());

        tracker.notifyResourceRequirements(
                Collections.singleton(ResourceRequirement.create(ResourceProfile.UNKNOWN, 1)));

        tracker.notifyAcquiredResource(ResourceProfile.ANY);

        tracker.notifyResourceRequirements(Collections.emptyList());

        assertThat(tracker.getAcquiredResources())
                .contains(ResourceRequirement.create(ResourceProfile.ANY, 1));
        assertThat(tracker.getMissingResources()).isEmpty();

        tracker.notifyResourceRequirements(
                Collections.singleton(ResourceRequirement.create(ResourceProfile.UNKNOWN, 1)));

        assertThat(tracker.getAcquiredResources())
                .contains(ResourceRequirement.create(ResourceProfile.ANY, 1));
        assertThat(tracker.getMissingResources()).isEmpty();
    }

    @Test
    void testExcessResourcesAreAssignedOnRequirementIncrease() {
        JobScopedResourceTracker tracker = new JobScopedResourceTracker(JobID.generate());

        tracker.notifyAcquiredResource(ResourceProfile.ANY);

        tracker.notifyResourceRequirements(
                Collections.singleton(ResourceRequirement.create(ResourceProfile.UNKNOWN, 1)));

        assertThat(tracker.getAcquiredResources())
                .contains(ResourceRequirement.create(ResourceProfile.ANY, 1));
        assertThat(tracker.getMissingResources()).isEmpty();
    }

    @Test
    void testExcessResourcesAreAssignedOnResourceLoss() {
        JobScopedResourceTracker tracker = new JobScopedResourceTracker(JobID.generate());

        tracker.notifyAcquiredResource(ResourceProfile.ANY);
        tracker.notifyAcquiredResource(ResourceProfile.ANY);

        tracker.notifyResourceRequirements(
                Collections.singleton(ResourceRequirement.create(ResourceProfile.UNKNOWN, 1)));

        tracker.notifyLostResource(ResourceProfile.ANY);

        assertThat(tracker.getAcquiredResources())
                .contains(ResourceRequirement.create(ResourceProfile.ANY, 1));
        assertThat(tracker.getMissingResources()).isEmpty();
    }
}
