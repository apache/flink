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
    private static final ResourceProfile OTHER_SLOT_RESOURCE_PROFILE =
            DEFAULT_TOTAL_RESOURCE_PROFILE.multiply(2);

    @Override
    protected Optional<ResourceAllocationStrategy> getResourceAllocationStrategy() {
        return Optional.of(
                new DefaultResourceAllocationStrategy(
                        DEFAULT_TOTAL_RESOURCE_PROFILE, DEFAULT_NUM_SLOTS_PER_WORKER));
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
                            runInMainThread(
                                    () ->
                                            getSlotManager()
                                                    .processResourceRequirements(
                                                            createResourceRequirements(
                                                                    new JobID(),
                                                                    1,
                                                                    OTHER_SLOT_RESOURCE_PROFILE)));
                            assertThat(resourceRequests.get(), is(0));
                        });
            }
        };
    }
}
