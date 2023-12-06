/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils;
import org.apache.flink.runtime.executiongraph.ExecutionVertexDeploymentTest;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlotBuilder;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.testutils.DirectScheduledExecutorService;
import org.apache.flink.types.SerializableOptional;
import org.apache.flink.util.TestLoggerExtension;
import org.apache.flink.util.concurrent.ScheduledExecutorServiceAdapter;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

/** Tests for {@link BatchExecutionDeployExecutor}. */
@ExtendWith(TestLoggerExtension.class)
class BatchExecutionDeployExecutorTest {

    private ExecutionDeployExecutor executionDeployExecutor;
    private Time rpcTimeout;

    @BeforeEach
    void setUp() {
        rpcTimeout = Time.milliseconds(5000);
        executionDeployExecutor = createExecutionDeployer();
    }

    @Test
    public void testDeployTask() throws Exception {
        final ExecutionDeployExecutor executionDeployExecutor = createExecutionDeployer();
        final Execution execution =
                ExecutionGraphTestUtils.getExecutionVertex().getCurrentExecutionAttempt();

        final SimpleAckingTaskManagerGateway taskManagerGateway =
                new SimpleAckingTaskManagerGateway();
        taskManagerGateway.setBatchSubmitFunction(
                tdds ->
                        CompletableFuture.completedFuture(
                                tdds.stream()
                                        .map(
                                                tdd ->
                                                        SerializableOptional.ofNullable(
                                                                (Throwable) null))
                                        .collect(Collectors.toList())));

        final LogicalSlot slot =
                new TestingLogicalSlotBuilder()
                        .setTaskManagerGateway(taskManagerGateway)
                        .createTestingLogicalSlot();

        execution.transitionState(ExecutionState.SCHEDULED);
        execution.tryAssignResource(slot);

        executionDeployExecutor.executeDeploy(execution);
        executionDeployExecutor.flushDeploy();

        assertEquals(execution.getState(), ExecutionState.DEPLOYING);
    }

    @Test
    public void testDeployOneTaskFailed() throws Exception {
        final Execution execution =
                ExecutionGraphTestUtils.getExecutionVertex().getCurrentExecutionAttempt();
        final Execution failedExecution =
                ExecutionGraphTestUtils.getExecutionVertex().getCurrentExecutionAttempt();
        final SimpleAckingTaskManagerGateway taskManagerGateway =
                new SimpleAckingTaskManagerGateway();
        taskManagerGateway.setBatchSubmitFunction(
                tdds ->
                        CompletableFuture.completedFuture(
                                tdds.stream()
                                        .map(
                                                tdd -> {
                                                    if (tdd.getExecutionAttemptId()
                                                            .equals(
                                                                    failedExecution
                                                                            .getAttemptId())) {
                                                        final Throwable submitFailedException =
                                                                new Exception("Submit failed");
                                                        return SerializableOptional.ofNullable(
                                                                submitFailedException);
                                                    }
                                                    return SerializableOptional.ofNullable(
                                                            (Throwable) null);
                                                })
                                        .collect(Collectors.toList())));

        TaskManagerLocation localTaskManagerLocation = new LocalTaskManagerLocation();

        final LogicalSlot slot =
                new TestingLogicalSlotBuilder()
                        .setTaskManagerGateway(taskManagerGateway)
                        .setTaskManagerLocation(localTaskManagerLocation)
                        .createTestingLogicalSlot();

        final LogicalSlot slot2 =
                new TestingLogicalSlotBuilder()
                        .setTaskManagerGateway(taskManagerGateway)
                        .setTaskManagerLocation(localTaskManagerLocation)
                        .createTestingLogicalSlot();

        execution.transitionState(ExecutionState.SCHEDULED);
        failedExecution.transitionState(ExecutionState.SCHEDULED);
        execution.tryAssignResource(slot);
        failedExecution.tryAssignResource(slot2);

        executionDeployExecutor.executeDeploy(execution);
        executionDeployExecutor.executeDeploy(failedExecution);

        executionDeployExecutor.flushDeploy();

        assertEquals(ExecutionState.DEPLOYING, execution.getState());
        assertEquals(ExecutionState.FAILED, failedExecution.getState());
    }

    @Test
    public void testDeployAllTaskFailed() throws Exception {
        final Execution execution =
                ExecutionGraphTestUtils.getExecutionVertex().getCurrentExecutionAttempt();
        final Execution execution2 =
                ExecutionGraphTestUtils.getExecutionVertex().getCurrentExecutionAttempt();
        final SimpleAckingTaskManagerGateway taskManagerGateway =
                new ExecutionVertexDeploymentTest.SubmitFailingSimpleAckingTaskManagerGateway();
        final LogicalSlot slot =
                new TestingLogicalSlotBuilder()
                        .setTaskManagerGateway(taskManagerGateway)
                        .createTestingLogicalSlot();

        final LogicalSlot slot2 =
                new TestingLogicalSlotBuilder()
                        .setTaskManagerGateway(taskManagerGateway)
                        .createTestingLogicalSlot();

        execution.transitionState(ExecutionState.SCHEDULED);
        execution2.transitionState(ExecutionState.SCHEDULED);
        execution.tryAssignResource(slot);
        execution2.tryAssignResource(slot2);

        executionDeployExecutor.executeDeploy(execution);
        executionDeployExecutor.executeDeploy(execution2);

        executionDeployExecutor.flushDeploy();

        assertEquals(execution.getState(), ExecutionState.FAILED);
        assertEquals(execution2.getState(), ExecutionState.FAILED);
    }

    private ExecutionDeployExecutor createExecutionDeployer() {
        return new BatchExecutionDeployExecutor.Factory()
                .createInstance(
                        new DefaultExecutionOperations(),
                        new ScheduledExecutorServiceAdapter(new DirectScheduledExecutorService()),
                        rpcTimeout);
    }
}
