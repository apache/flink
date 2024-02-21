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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.testutils.EachCallbackWrapper;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.entrypoint.WorkingDirectory;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGateway;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGatewayBuilder;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.rpc.TestingRpcServiceExtension;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

/** Recovery tests for {@link TaskExecutor}. */
class TaskExecutorRecoveryTest {
    private final TestingRpcServiceExtension rpcServiceExtension = new TestingRpcServiceExtension();

    @RegisterExtension
    private final EachCallbackWrapper<TestingRpcServiceExtension> eachWrapper =
            new EachCallbackWrapper<>(rpcServiceExtension);

    @Test
    void testRecoveredTaskExecutorWillRestoreAllocationState(@TempDir File tempDir)
            throws Exception {
        final ResourceID resourceId = ResourceID.generate();

        final Configuration configuration = new Configuration();
        configuration.set(TaskManagerOptions.NUM_TASK_SLOTS, 2);
        configuration.set(CheckpointingOptions.LOCAL_RECOVERY, true);

        final TestingResourceManagerGateway testingResourceManagerGateway =
                new TestingResourceManagerGateway();
        final ArrayBlockingQueue<TaskExecutorSlotReport> queue = new ArrayBlockingQueue<>(2);
        testingResourceManagerGateway.setSendSlotReportFunction(
                slotReportInformation -> {
                    queue.offer(
                            TaskExecutorSlotReport.create(
                                    slotReportInformation.f0, slotReportInformation.f2));
                    return CompletableFuture.completedFuture(Acknowledge.get());
                });

        final TestingRpcService rpcService = rpcServiceExtension.getTestingRpcService();
        rpcService.registerGateway(
                testingResourceManagerGateway.getAddress(), testingResourceManagerGateway);

        final JobID jobId = new JobID();

        final TestingHighAvailabilityServices highAvailabilityServices =
                new TestingHighAvailabilityServices();

        highAvailabilityServices.setResourceManagerLeaderRetriever(
                new SettableLeaderRetrievalService(
                        testingResourceManagerGateway.getAddress(),
                        testingResourceManagerGateway.getFencingToken().toUUID()));
        final SettableLeaderRetrievalService jobMasterLeaderRetriever =
                new SettableLeaderRetrievalService();
        highAvailabilityServices.setJobMasterLeaderRetriever(jobId, jobMasterLeaderRetriever);

        final WorkingDirectory workingDirectory = WorkingDirectory.create(tempDir);
        final TaskExecutor taskExecutor =
                TaskExecutorBuilder.newBuilder(
                                rpcService, highAvailabilityServices, workingDirectory)
                        .setConfiguration(configuration)
                        .setResourceId(resourceId)
                        .build();

        taskExecutor.start();

        final TaskExecutorGateway taskExecutorGateway =
                taskExecutor.getSelfGateway(TaskExecutorGateway.class);

        final TaskExecutorSlotReport taskExecutorSlotReport = queue.take();

        final SlotReport slotReport = taskExecutorSlotReport.getSlotReport();

        assertThat(slotReport.getNumSlotStatus(), is(2));

        final SlotStatus slotStatus = slotReport.iterator().next();
        final SlotID allocatedSlotID = slotStatus.getSlotID();

        final AllocationID allocationId = new AllocationID();
        taskExecutorGateway
                .requestSlot(
                        allocatedSlotID,
                        jobId,
                        allocationId,
                        slotStatus.getResourceProfile(),
                        "localhost",
                        testingResourceManagerGateway.getFencingToken(),
                        Time.seconds(10L))
                .join();

        taskExecutor.close();

        final BlockingQueue<Collection<SlotOffer>> offeredSlots = new ArrayBlockingQueue<>(1);

        final TestingJobMasterGateway jobMasterGateway =
                new TestingJobMasterGatewayBuilder()
                        .setOfferSlotsFunction(
                                (resourceID, slotOffers) -> {
                                    offeredSlots.offer(new HashSet<>(slotOffers));
                                    return CompletableFuture.completedFuture(slotOffers);
                                })
                        .build();
        rpcService.registerGateway(jobMasterGateway.getAddress(), jobMasterGateway);
        jobMasterLeaderRetriever.notifyListener(
                jobMasterGateway.getAddress(), jobMasterGateway.getFencingToken().toUUID());

        // recover the TaskExecutor
        final TaskExecutor recoveredTaskExecutor =
                TaskExecutorBuilder.newBuilder(
                                rpcService, highAvailabilityServices, workingDirectory)
                        .setConfiguration(configuration)
                        .setResourceId(resourceId)
                        .build();

        recoveredTaskExecutor.start();

        final TaskExecutorSlotReport recoveredSlotReport = queue.take();

        for (SlotStatus status : recoveredSlotReport.getSlotReport()) {
            if (status.getSlotID().equals(allocatedSlotID)) {
                assertThat(status.getJobID(), is(jobId));
                assertThat(status.getAllocationID(), is(allocationId));
            } else {
                assertThat(status.getJobID(), is(nullValue()));
            }
        }

        final Collection<SlotOffer> take = offeredSlots.take();

        assertThat(take, hasSize(1));
        final SlotOffer offeredSlot = take.iterator().next();

        assertThat(offeredSlot.getAllocationId(), is(allocationId));
    }

    private static final class TaskExecutorSlotReport {
        private final ResourceID taskExecutorResourceId;
        private final SlotReport slotReport;

        private TaskExecutorSlotReport(ResourceID taskExecutorResourceId, SlotReport slotReport) {
            this.taskExecutorResourceId = taskExecutorResourceId;
            this.slotReport = slotReport;
        }

        public ResourceID getTaskExecutorResourceId() {
            return taskExecutorResourceId;
        }

        public SlotReport getSlotReport() {
            return slotReport;
        }

        public static TaskExecutorSlotReport create(
                ResourceID taskExecutorResourceId, SlotReport slotReport) {
            return new TaskExecutorSlotReport(taskExecutorResourceId, slotReport);
        }
    }
}
