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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.LocalUnresolvedTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.taskmanager.UnresolvedTaskManagerLocation;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** JobMaster-related test utils. */
public class JobMasterTestUtils {

    public static void registerTaskExecutorAndOfferSlots(
            TestingRpcService rpcService,
            JobMasterGateway jobMasterGateway,
            JobID jobId,
            int numSlots,
            Time testingTimeout)
            throws ExecutionException, InterruptedException {

        final TaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setCancelTaskFunction(
                                executionAttemptId -> {
                                    jobMasterGateway.updateTaskExecutionState(
                                            new TaskExecutionState(
                                                    executionAttemptId, ExecutionState.CANCELED));
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .createTestingTaskExecutorGateway();

        final UnresolvedTaskManagerLocation unresolvedTaskManagerLocation =
                new LocalUnresolvedTaskManagerLocation();

        rpcService.registerGateway(taskExecutorGateway.getAddress(), taskExecutorGateway);

        jobMasterGateway
                .registerTaskManager(
                        taskExecutorGateway.getAddress(),
                        unresolvedTaskManagerLocation,
                        jobId,
                        testingTimeout)
                .get();

        Collection<SlotOffer> slotOffers =
                IntStream.range(0, numSlots)
                        .mapToObj(
                                index ->
                                        new SlotOffer(
                                                new AllocationID(), index, ResourceProfile.ANY))
                        .collect(Collectors.toList());

        jobMasterGateway
                .offerSlots(
                        unresolvedTaskManagerLocation.getResourceID(), slotOffers, testingTimeout)
                .get();
    }

    private JobMasterTestUtils() {}
}
