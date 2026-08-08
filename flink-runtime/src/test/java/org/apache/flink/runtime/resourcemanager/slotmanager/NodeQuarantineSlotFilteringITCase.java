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
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.resourcemanager.health.DefaultNodeHealthManager;
import org.apache.flink.runtime.resourcemanager.health.NodeHealthManager;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.SlotStatus;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** IT Cases for node quarantine slot filtering functionality in {@link FineGrainedSlotManager}. */
class NodeQuarantineSlotFilteringITCase extends AbstractFineGrainedSlotManagerITCase {

    @Override
    protected Optional<ResourceAllocationStrategy> getResourceAllocationStrategy(
            SlotManagerConfiguration slotManagerConfiguration) {
        return Optional.of(
                new DefaultResourceAllocationStrategy(
                        DEFAULT_TOTAL_RESOURCE_PROFILE,
                        DEFAULT_NUM_SLOTS_PER_WORKER,
                        slotManagerConfiguration.getTaskManagerLoadBalanceMode(),
                        slotManagerConfiguration.getTaskManagerTimeout(),
                        slotManagerConfiguration.getRedundantTaskManagerNum(),
                        slotManagerConfiguration.getMinTotalCpu(),
                        slotManagerConfiguration.getMinTotalMem()));
    }

    @Override
    protected Optional<NodeHealthManager> getNodeHealthManager() {
        return Optional.of(new DefaultNodeHealthManager());
    }

    /** Tests that slots on quarantined nodes are not allocated. */
    @Test
    void testSlotAllocationSkipsQuarantinedNodes() throws Exception {
        final JobID jobId = new JobID();
        final AtomicInteger slotRequestCount = new AtomicInteger(0);

        final TaskExecutorGateway taskExecutorGateway1 =
                new TestingTaskExecutorGatewayBuilder()
                        .setRequestSlotFunction(
                                tuple6 -> {
                                    slotRequestCount.incrementAndGet();
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .createTestingTaskExecutorGateway();

        final TaskExecutorGateway taskExecutorGateway2 =
                new TestingTaskExecutorGatewayBuilder()
                        .setRequestSlotFunction(
                                tuple6 -> {
                                    slotRequestCount.incrementAndGet();
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .createTestingTaskExecutorGateway();

        new Context() {
            {
                runTest(
                        () -> {
                            // Register two task managers
                            final ResourceID resourceId1 = ResourceID.generate();
                            final ResourceID resourceId2 = ResourceID.generate();
                            final InstanceID instanceId1 = new InstanceID();
                            final InstanceID instanceId2 = new InstanceID();

                            final TaskExecutorConnection taskExecutorConnection1 =
                                    new TaskExecutorConnection(resourceId1, taskExecutorGateway1);
                            final TaskExecutorConnection taskExecutorConnection2 =
                                    new TaskExecutorConnection(resourceId2, taskExecutorGateway2);

                            final SlotID slotId1 = new SlotID(resourceId1, 0);
                            final SlotID slotId2 = new SlotID(resourceId2, 0);
                            final SlotReport slotReport1 =
                                    new SlotReport(
                                            new SlotStatus(slotId1, DEFAULT_SLOT_RESOURCE_PROFILE));
                            final SlotReport slotReport2 =
                                    new SlotReport(
                                            new SlotStatus(slotId2, DEFAULT_SLOT_RESOURCE_PROFILE));

                            runInMainThread(
                                    () -> {
                                        // Register both task managers
                                        getSlotManager()
                                                .registerTaskManager(
                                                        taskExecutorConnection1,
                                                        slotReport1,
                                                        DEFAULT_TOTAL_RESOURCE_PROFILE,
                                                        DEFAULT_SLOT_RESOURCE_PROFILE);
                                        getSlotManager()
                                                .registerTaskManager(
                                                        taskExecutorConnection2,
                                                        slotReport2,
                                                        DEFAULT_TOTAL_RESOURCE_PROFILE,
                                                        DEFAULT_SLOT_RESOURCE_PROFILE);

                                        // Quarantine the first task manager
                                        getNodeHealthManager()
                                                .markQuarantined(
                                                        resourceId1,
                                                        "localhost",
                                                        "test quarantine",
                                                        Duration.ofMinutes(10));

                                        // Request a slot
                                        getSlotManager()
                                                .processResourceRequirements(
                                                        createResourceRequirements(jobId, 1));
                                    });

                            // Wait for slot allocation
                            Thread.sleep(100);

                            // Verify that only one slot request was made (to the healthy node)
                            assertThat(slotRequestCount.get()).isEqualTo(1);

                            // Verify that the quarantined node is indeed unhealthy
                            assertThat(getNodeHealthManager().isHealthy(resourceId1)).isFalse();
                            assertThat(getNodeHealthManager().isHealthy(resourceId2)).isTrue();
                        });
            }
        };
    }

    /** Tests that slots become available again after quarantine expires. */
    @Test
    void testSlotAllocationAfterQuarantineExpires() throws Exception {
        final JobID jobId = new JobID();
        final AtomicInteger slotRequestCount = new AtomicInteger(0);

        final TaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setRequestSlotFunction(
                                tuple6 -> {
                                    slotRequestCount.incrementAndGet();
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .createTestingTaskExecutorGateway();

        new Context() {
            {
                runTest(
                        () -> {
                            // Register a task manager
                            final ResourceID resourceId = ResourceID.generate();
                            final InstanceID instanceId = new InstanceID();

                            final TaskExecutorConnection taskExecutorConnection =
                                    new TaskExecutorConnection(resourceId, taskExecutorGateway);

                            final SlotID slotId = new SlotID(resourceId, 0);
                            final SlotReport slotReport =
                                    new SlotReport(
                                            new SlotStatus(slotId, DEFAULT_SLOT_RESOURCE_PROFILE));

                            runInMainThread(
                                    () -> {
                                        // Register the task manager
                                        getSlotManager()
                                                .registerTaskManager(
                                                        taskExecutorConnection,
                                                        slotReport,
                                                        DEFAULT_TOTAL_RESOURCE_PROFILE,
                                                        DEFAULT_SLOT_RESOURCE_PROFILE);

                                        // Quarantine the task manager for a short duration
                                        getNodeHealthManager()
                                                .markQuarantined(
                                                        resourceId,
                                                        "localhost",
                                                        "test quarantine",
                                                        Duration.ofMillis(50));

                                        // Request a slot while quarantined
                                        getSlotManager()
                                                .processResourceRequirements(
                                                        createResourceRequirements(jobId, 1));
                                    });

                            // Wait for quarantine to expire
                            Thread.sleep(100);

                            runInMainThread(
                                    () -> {
                                        // Request a slot after quarantine expires
                                        getSlotManager()
                                                .processResourceRequirements(
                                                        createResourceRequirements(jobId, 1));
                                    });

                            // Wait for slot allocation
                            Thread.sleep(100);

                            // Verify that the node is healthy again and slot was allocated
                            assertThat(getNodeHealthManager().isHealthy(resourceId)).isTrue();
                            assertThat(slotRequestCount.get()).isEqualTo(1);
                        });
            }
        };
    }

    /** Tests that quarantine can be manually removed. */
    @Test
    void testManualQuarantineRemoval() throws Exception {
        final JobID jobId = new JobID();
        final AtomicInteger slotRequestCount = new AtomicInteger(0);

        final TaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setRequestSlotFunction(
                                tuple6 -> {
                                    slotRequestCount.incrementAndGet();
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .createTestingTaskExecutorGateway();

        new Context() {
            {
                runTest(
                        () -> {
                            // Register a task manager
                            final ResourceID resourceId = ResourceID.generate();
                            final InstanceID instanceId = new InstanceID();

                            final TaskExecutorConnection taskExecutorConnection =
                                    new TaskExecutorConnection(resourceId, taskExecutorGateway);

                            final SlotID slotId = new SlotID(resourceId, 0);
                            final SlotReport slotReport =
                                    new SlotReport(
                                            new SlotStatus(slotId, DEFAULT_SLOT_RESOURCE_PROFILE));

                            runInMainThread(
                                    () -> {
                                        // Register the task manager
                                        getSlotManager()
                                                .registerTaskManager(
                                                        taskExecutorConnection,
                                                        slotReport,
                                                        DEFAULT_TOTAL_RESOURCE_PROFILE,
                                                        DEFAULT_SLOT_RESOURCE_PROFILE);

                                        // Quarantine the task manager
                                        getNodeHealthManager()
                                                .markQuarantined(
                                                        resourceId,
                                                        "localhost",
                                                        "test quarantine",
                                                        Duration.ofMinutes(10));

                                        // Verify node is quarantined
                                        assertThat(getNodeHealthManager().isHealthy(resourceId))
                                                .isFalse();

                                        // Manually remove quarantine
                                        getNodeHealthManager().removeQuarantine(resourceId);

                                        // Verify node is healthy again
                                        assertThat(getNodeHealthManager().isHealthy(resourceId))
                                                .isTrue();

                                        // Request a slot
                                        getSlotManager()
                                                .processResourceRequirements(
                                                        createResourceRequirements(jobId, 1));
                                    });

                            // Wait for slot allocation
                            Thread.sleep(100);

                            // Verify that slot was allocated
                            assertThat(slotRequestCount.get()).isEqualTo(1);
                        });
            }
        };
    }
}
