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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.RpcTaskManagerGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.util.clock.SystemClock;

import javax.annotation.Nonnull;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;

import static org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter.forMainThread;

/** Test base class for {@link DeclarativeSlotPoolBridge}. */
abstract class AbstractDeclarativeSlotPoolBridgeTest {

    protected static final Duration RPC_TIMEOUT = Duration.ofSeconds(20);
    protected static final JobID JOB_ID = new JobID();
    protected static final JobMasterId JOB_MASTER_ID = JobMasterId.generate();
    protected final ComponentMainThreadExecutor componentMainThreadExecutor = forMainThread();

    @Parameter protected RequestSlotMatchingStrategy requestSlotMatchingStrategy;

    @Parameter(1)
    protected Duration slotRequestMaxInterval;

    @Parameter(2)
    boolean deferSlotAllocation;

    @Parameters(
            name =
                    "requestSlotMatchingStrategy: {0}, slotRequestMaxInterval: {1}, deferSlotAllocation: {2}")
    private static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[] {SimpleRequestSlotMatchingStrategy.INSTANCE, Duration.ZERO, false},
                new Object[] {SimpleRequestSlotMatchingStrategy.INSTANCE, Duration.ZERO, true},
                new Object[] {
                    SimpleRequestSlotMatchingStrategy.INSTANCE, Duration.ofMillis(20), false
                },
                new Object[] {
                    SimpleRequestSlotMatchingStrategy.INSTANCE, Duration.ofMillis(20), true
                },
                new Object[] {
                    PreferredAllocationRequestSlotMatchingStrategy.create(
                            SimpleRequestSlotMatchingStrategy.INSTANCE),
                    Duration.ZERO,
                    false
                },
                new Object[] {
                    PreferredAllocationRequestSlotMatchingStrategy.create(
                            SimpleRequestSlotMatchingStrategy.INSTANCE),
                    Duration.ZERO,
                    true
                },
                new Object[] {
                    PreferredAllocationRequestSlotMatchingStrategy.create(
                            SimpleRequestSlotMatchingStrategy.INSTANCE),
                    Duration.ofMillis(20),
                    false
                },
                new Object[] {
                    PreferredAllocationRequestSlotMatchingStrategy.create(
                            SimpleRequestSlotMatchingStrategy.INSTANCE),
                    Duration.ofMillis(20),
                    true
                });
    }

    @Nonnull
    DeclarativeSlotPoolBridge createDeclarativeSlotPoolBridge(
            DeclarativeSlotPoolFactory declarativeSlotPoolFactory) {
        return createDeclarativeSlotPoolBridge(
                declarativeSlotPoolFactory, componentMainThreadExecutor);
    }

    @Nonnull
    DeclarativeSlotPoolBridge createDeclarativeSlotPoolBridge(
            DeclarativeSlotPoolFactory declarativeSlotPoolFactory,
            ComponentMainThreadExecutor componentMainThreadExecutor) {
        return new DeclarativeSlotPoolBridge(
                JOB_ID,
                declarativeSlotPoolFactory,
                SystemClock.getInstance(),
                RPC_TIMEOUT,
                Duration.ofSeconds(20),
                Duration.ofSeconds(20),
                requestSlotMatchingStrategy,
                slotRequestMaxInterval,
                deferSlotAllocation,
                componentMainThreadExecutor);
    }

    static PhysicalSlot createAllocatedSlot(AllocationID allocationID) {
        return new AllocatedSlot(
                allocationID,
                new LocalTaskManagerLocation(),
                0,
                ResourceProfile.ANY,
                new RpcTaskManagerGateway(
                        new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway(),
                        JobMasterId.generate()));
    }
}
