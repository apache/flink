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

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.slots.ResourceCounter;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/** Tests for the {@link DefaultResourceAllocationStrategy}. */
public class DefaultResourceAllocationStrategyTest extends TestLogger {
    private static final ResourceProfile DEFAULT_SLOT_RESOURCE =
            ResourceProfile.fromResources(1, 100);
    private static final int NUM_OF_SLOTS = 5;
    private static final DefaultResourceAllocationStrategy STRATEGY =
            new DefaultResourceAllocationStrategy(DEFAULT_SLOT_RESOURCE, NUM_OF_SLOTS);

    @Test
    public void testFulfillRequirementWithRegisteredResources() {
        final Map<InstanceID, Tuple2<ResourceProfile, ResourceProfile>> registeredResources =
                new HashMap<>();
        final JobID jobId = new JobID();
        final InstanceID instanceId = new InstanceID();
        final List<ResourceRequirement> requirements = new ArrayList<>();
        final ResourceProfile largeResource = DEFAULT_SLOT_RESOURCE.multiply(8);
        requirements.add(ResourceRequirement.create(largeResource, 1));
        requirements.add(ResourceRequirement.create(ResourceProfile.UNKNOWN, 2));
        registeredResources.put(
                instanceId, Tuple2.of(DEFAULT_SLOT_RESOURCE.multiply(10), DEFAULT_SLOT_RESOURCE));

        final ResourceAllocationResult result =
                STRATEGY.tryFulfillRequirements(
                        Collections.singletonMap(jobId, requirements),
                        registeredResources,
                        Collections.emptyList());
        assertThat(result.getUnfulfillableJobs(), is(empty()));
        assertThat(result.getAllocationsOnPendingResources().keySet(), is(empty()));
        assertThat(result.getPendingTaskManagersToAllocate(), is(empty()));
        assertThat(
                result.getAllocationsOnRegisteredResources()
                        .get(jobId)
                        .get(instanceId)
                        .getResourceCount(DEFAULT_SLOT_RESOURCE),
                is(2));
        assertThat(
                result.getAllocationsOnRegisteredResources()
                        .get(jobId)
                        .get(instanceId)
                        .getResourceCount(largeResource),
                is(1));
    }

    @Test
    public void testFulfillRequirementWithPendingResources() {
        final List<PendingTaskManager> pendingTaskManagers = new ArrayList<>();
        final JobID jobId = new JobID();
        final List<ResourceRequirement> requirements = new ArrayList<>();
        final ResourceProfile largeResource = DEFAULT_SLOT_RESOURCE.multiply(3);
        final PendingTaskManagerId pendingTaskManagerId = PendingTaskManagerId.generate();
        requirements.add(ResourceRequirement.create(largeResource, 1));
        requirements.add(ResourceRequirement.create(ResourceProfile.UNKNOWN, 7));
        pendingTaskManagers.add(
                new PendingTaskManager(
                        pendingTaskManagerId,
                        DEFAULT_SLOT_RESOURCE.multiply(NUM_OF_SLOTS),
                        DEFAULT_SLOT_RESOURCE));

        final ResourceAllocationResult result =
                STRATEGY.tryFulfillRequirements(
                        Collections.singletonMap(jobId, requirements),
                        Collections.emptyMap(),
                        pendingTaskManagers);
        assertThat(result.getUnfulfillableJobs(), is(empty()));
        assertThat(result.getAllocationsOnRegisteredResources().keySet(), is(empty()));
        assertThat(result.getPendingTaskManagersToAllocate().size(), is(1));
        final PendingTaskManagerId newAllocated =
                result.getPendingTaskManagersToAllocate().get(0).getPendingTaskManagerId();
        final ResourceCounter allFulfilledRequirements = new ResourceCounter();
        result.getAllocationsOnPendingResources()
                .get(pendingTaskManagerId)
                .get(jobId)
                .getResourceProfilesWithCount()
                .forEach(allFulfilledRequirements::incrementCount);
        result.getAllocationsOnPendingResources()
                .get(newAllocated)
                .get(jobId)
                .getResourceProfilesWithCount()
                .forEach(allFulfilledRequirements::incrementCount);

        assertThat(allFulfilledRequirements.getResourceCount(DEFAULT_SLOT_RESOURCE), is(7));
        assertThat(allFulfilledRequirements.getResourceCount(largeResource), is(1));
    }

    @Test
    public void testUnfulfillableRequirement() {
        final Map<InstanceID, Tuple2<ResourceProfile, ResourceProfile>> registeredResources =
                new HashMap<>();
        final JobID jobId = new JobID();
        final List<ResourceRequirement> requirements = new ArrayList<>();
        final ResourceProfile unfulfillableResource = DEFAULT_SLOT_RESOURCE.multiply(8);
        requirements.add(ResourceRequirement.create(unfulfillableResource, 1));
        registeredResources.put(
                new InstanceID(),
                Tuple2.of(DEFAULT_SLOT_RESOURCE.multiply(NUM_OF_SLOTS), DEFAULT_SLOT_RESOURCE));

        final ResourceAllocationResult result =
                STRATEGY.tryFulfillRequirements(
                        Collections.singletonMap(jobId, requirements),
                        registeredResources,
                        Collections.emptyList());
        assertThat(result.getUnfulfillableJobs(), contains(jobId));
        assertThat(result.getPendingTaskManagersToAllocate(), is(empty()));
    }
}
