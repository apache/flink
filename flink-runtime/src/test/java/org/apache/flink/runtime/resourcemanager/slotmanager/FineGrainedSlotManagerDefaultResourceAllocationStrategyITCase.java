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
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;

import org.junit.Test;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * IT Cases of {@link FineGrainedSlotManager}, with the {@link DefaultResourceAllocationStrategy}.
 */
public class FineGrainedSlotManagerDefaultResourceAllocationStrategyITCase
        extends AbstractFineGrainedSlotManagerITCase {
    private static final WorkerResourceSpec DEFAULT_WORKER_RESOURCE_SPEC =
            new WorkerResourceSpec.Builder()
                    .setCpuCores(10.0)
                    .setTaskHeapMemoryMB(1000)
                    .setTaskOffHeapMemoryMB(1000)
                    .setNetworkMemoryMB(1000)
                    .setManagedMemoryMB(1000)
                    .build();
    private static final WorkerResourceSpec LARGE_WORKER_RESOURCE_SPEC =
            new WorkerResourceSpec.Builder()
                    .setCpuCores(100.0)
                    .setTaskHeapMemoryMB(10000)
                    .setTaskOffHeapMemoryMB(10000)
                    .setNetworkMemoryMB(10000)
                    .setManagedMemoryMB(10000)
                    .build();
    private static final int DEFAULT_NUM_SLOTS_PER_WORKER = 2;
    private static final ResourceProfile DEFAULT_TOTAL_RESOURCE_PROFILE =
            SlotManagerUtils.generateTaskManagerTotalResourceProfile(DEFAULT_WORKER_RESOURCE_SPEC);
    private static final ResourceProfile DEFAULT_SLOT_RESOURCE_PROFILE =
            SlotManagerUtils.generateDefaultSlotResourceProfile(
                    DEFAULT_WORKER_RESOURCE_SPEC, DEFAULT_NUM_SLOTS_PER_WORKER);
    private static final ResourceProfile LARGE_TOTAL_RESOURCE_PROFILE =
            SlotManagerUtils.generateTaskManagerTotalResourceProfile(LARGE_WORKER_RESOURCE_SPEC);
    private static final ResourceProfile LARGE_SLOT_RESOURCE_PROFILE =
            SlotManagerUtils.generateDefaultSlotResourceProfile(
                    LARGE_WORKER_RESOURCE_SPEC, DEFAULT_NUM_SLOTS_PER_WORKER);

    @Override
    protected ResourceProfile getDefaultTaskManagerResourceProfile() {
        return DEFAULT_TOTAL_RESOURCE_PROFILE;
    }

    @Override
    protected ResourceProfile getDefaultSlotResourceProfile() {
        return DEFAULT_SLOT_RESOURCE_PROFILE;
    }

    @Override
    protected int getDefaultNumberSlotsPerWorker() {
        return DEFAULT_NUM_SLOTS_PER_WORKER;
    }

    @Override
    protected ResourceProfile getLargeTaskManagerResourceProfile() {
        return LARGE_TOTAL_RESOURCE_PROFILE;
    }

    @Override
    protected ResourceProfile getLargeSlotResourceProfile() {
        return LARGE_SLOT_RESOURCE_PROFILE;
    }

    @Override
    protected Optional<ResourceAllocationStrategy> getResourceAllocationStrategy() {
        return Optional.of(
                new DefaultResourceAllocationStrategy(
                        DEFAULT_SLOT_RESOURCE_PROFILE, DEFAULT_NUM_SLOTS_PER_WORKER));
    }

    /**
     * Test that the slot manager only allocates new workers if their worker spec can fulfill the
     * requested resource profile.
     */
    @Test
    public void testWorkerOnlyAllocatedIfRequestedSlotCouldBeFulfilled() throws Exception {
        final AtomicInteger resourceRequests = new AtomicInteger(0);

        new Context() {
            {
                resourceActionsBuilder.setAllocateResourceFunction(
                        ignored -> {
                            resourceRequests.incrementAndGet();
                            return true;
                        });
                runTest(
                        () -> {
                            getSlotManager()
                                    .processResourceRequirements(
                                            createResourceRequirements(
                                                    new JobID(), 1, getLargeSlotResourceProfile()));
                            assertThat(resourceRequests.get(), is(0));
                        });
            }
        };
    }
}
