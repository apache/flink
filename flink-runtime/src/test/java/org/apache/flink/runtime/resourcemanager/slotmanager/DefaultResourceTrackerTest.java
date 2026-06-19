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

import org.assertj.core.api.Condition;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the {@link DefaultResourceTracker}.
 *
 * <p>Note: The majority is of the tracking logic is covered by the {@link
 * JobScopedResourceTrackerTest}.
 */
class DefaultResourceTrackerTest {

    private static final JobID JOB_ID_1 = JobID.generate();
    private static final JobID JOB_ID_2 = JobID.generate();

    @Test
    void testInitialBehavior() {
        DefaultResourceTracker tracker = new DefaultResourceTracker();

        assertThat(tracker.isEmpty()).isTrue();
        tracker.notifyLostResource(JobID.generate(), ResourceProfile.ANY);
    }

    @Test
    void testClearDoesNotThrowException() {
        DefaultResourceTracker tracker = new DefaultResourceTracker();

        tracker.clear();
    }

    @Test
    void testGetRequiredResources() {
        DefaultResourceTracker tracker = new DefaultResourceTracker();

        ResourceRequirement requirement1 = ResourceRequirement.create(ResourceProfile.ANY, 1);
        ResourceRequirement requirement2 = ResourceRequirement.create(ResourceProfile.ANY, 2);

        tracker.notifyResourceRequirements(JOB_ID_1, Collections.singletonList(requirement1));
        tracker.notifyResourceRequirements(JOB_ID_2, Collections.singletonList(requirement2));

        Map<JobID, Collection<ResourceRequirement>> requiredResources =
                tracker.getMissingResources();
        assertMapKeyedEntriesContainsValue(requiredResources, JOB_ID_1, requirement1);

        assertMapKeyedEntriesContainsValue(requiredResources, JOB_ID_2, requirement2);
    }

    private static <K, VE> void assertMapKeyedEntriesContainsValue(
            Map<K, Collection<VE>> requiredResources, K jobID, VE resourceRequirementToContain) {
        assertThat(requiredResources)
                .hasEntrySatisfying(
                        new Condition<K>() {
                            @Override
                            public boolean matches(K key) {
                                return jobID.equals(key);
                            }
                        },
                        new Condition<Collection<VE>>() {
                            @Override
                            public boolean matches(Collection<VE> value) {
                                if (value == null || value.isEmpty()) {
                                    return false;
                                }
                                return value.contains(resourceRequirementToContain);
                            }
                        });
    }

    @Test
    void testGetAcquiredResources() {
        DefaultResourceTracker tracker = new DefaultResourceTracker();

        ResourceRequirement requirement1 = ResourceRequirement.create(ResourceProfile.ANY, 1);
        ResourceRequirement requirement2 = ResourceRequirement.create(ResourceProfile.ANY, 2);

        tracker.notifyAcquiredResource(JOB_ID_1, requirement1.getResourceProfile());
        for (int x = 0; x < requirement2.getNumberOfRequiredSlots(); x++) {
            tracker.notifyAcquiredResource(JOB_ID_2, requirement2.getResourceProfile());
        }

        assertThat(tracker.getAcquiredResources(JOB_ID_1)).contains(requirement1);
        assertThat(tracker.getAcquiredResources(JOB_ID_2)).contains(requirement2);

        tracker.notifyLostResource(JOB_ID_1, requirement1.getResourceProfile());
        assertThat(tracker.getAcquiredResources(JOB_ID_1)).isEmpty();
    }

    @Test
    void testTrackerRemovedOnRequirementReset() {
        DefaultResourceTracker tracker = new DefaultResourceTracker();

        tracker.notifyResourceRequirements(
                JOB_ID_1,
                Collections.singletonList(ResourceRequirement.create(ResourceProfile.ANY, 1)));
        assertThat(tracker.isEmpty()).isFalse();

        tracker.notifyResourceRequirements(JOB_ID_1, Collections.emptyList());
        assertThat(tracker.isEmpty()).isTrue();
    }

    @Test
    void testTrackerRemovedOnResourceLoss() {
        DefaultResourceTracker tracker = new DefaultResourceTracker();

        tracker.notifyAcquiredResource(JOB_ID_1, ResourceProfile.ANY);
        assertThat(tracker.isEmpty()).isFalse();

        tracker.notifyLostResource(JOB_ID_1, ResourceProfile.ANY);
        assertThat(tracker.isEmpty()).isTrue();
    }

    @Test
    void testTrackerRetainedOnResourceLossIfRequirementExists() {
        DefaultResourceTracker tracker = new DefaultResourceTracker();

        tracker.notifyAcquiredResource(JOB_ID_1, ResourceProfile.ANY);
        tracker.notifyResourceRequirements(
                JOB_ID_1,
                Collections.singletonList(ResourceRequirement.create(ResourceProfile.ANY, 1)));

        tracker.notifyLostResource(JOB_ID_1, ResourceProfile.ANY);
        assertThat(tracker.isEmpty()).isFalse();

        tracker.notifyResourceRequirements(JOB_ID_1, Collections.emptyList());
        assertThat(tracker.isEmpty()).isTrue();
    }

    @Test
    void testTrackerRetainedOnRequirementResetIfResourceExists() {
        DefaultResourceTracker tracker = new DefaultResourceTracker();

        tracker.notifyAcquiredResource(JOB_ID_1, ResourceProfile.ANY);
        tracker.notifyResourceRequirements(
                JOB_ID_1,
                Collections.singletonList(ResourceRequirement.create(ResourceProfile.ANY, 1)));

        tracker.notifyResourceRequirements(JOB_ID_1, Collections.emptyList());
        assertThat(tracker.isEmpty()).isFalse();

        tracker.notifyLostResource(JOB_ID_1, ResourceProfile.ANY);
        assertThat(tracker.isEmpty()).isTrue();
    }
}
