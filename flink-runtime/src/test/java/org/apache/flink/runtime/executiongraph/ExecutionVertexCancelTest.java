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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlotBuilder;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.scheduler.SchedulerBase;
import org.apache.flink.runtime.scheduler.SchedulerTestingUtils;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.util.concurrent.FutureUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createNoOpVertex;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.getExecutionVertex;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.setVertexResource;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.setVertexState;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Tests for cancelling {@link ExecutionVertex ExecutionVertices}. */
class ExecutionVertexCancelTest {

    @RegisterExtension
    static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

    // --------------------------------------------------------------------------------------------
    //  Canceling in different states
    // --------------------------------------------------------------------------------------------

    @Test
    void testCancelFromCreated() {
        try {
            final ExecutionVertex vertex = getExecutionVertex();

            assertThat(vertex.getExecutionState()).isEqualTo(ExecutionState.CREATED);

            vertex.cancel();

            assertThat(vertex.getExecutionState()).isEqualTo(ExecutionState.CANCELED);

            assertThat(vertex.getFailureInfo()).isNotPresent();

            assertThat(vertex.getStateTimestamp(ExecutionState.CREATED)).isGreaterThan(0);
            assertThat(vertex.getStateTimestamp(ExecutionState.CANCELING)).isGreaterThan(0);
            assertThat(vertex.getStateTimestamp(ExecutionState.CANCELED)).isGreaterThan(0);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    void testCancelFromScheduled() {
        try {
            final ExecutionVertex vertex = getExecutionVertex();

            setVertexState(vertex, ExecutionState.SCHEDULED);
            assertThat(vertex.getExecutionState()).isEqualTo(ExecutionState.SCHEDULED);

            vertex.cancel();

            assertThat(vertex.getExecutionState()).isEqualTo(ExecutionState.CANCELED);

            assertThat(vertex.getFailureInfo()).isNotPresent();

            assertThat(vertex.getStateTimestamp(ExecutionState.CREATED)).isGreaterThan(0);
            assertThat(vertex.getStateTimestamp(ExecutionState.CANCELING)).isGreaterThan(0);
            assertThat(vertex.getStateTimestamp(ExecutionState.CANCELED)).isGreaterThan(0);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    void testCancelFromRunning() {
        try {
            final ExecutionVertex vertex = getExecutionVertex();

            LogicalSlot slot =
                    new TestingLogicalSlotBuilder()
                            .setTaskManagerGateway(
                                    new CancelSequenceSimpleAckingTaskManagerGateway(1))
                            .createTestingLogicalSlot();

            setVertexResource(vertex, slot);
            setVertexState(vertex, ExecutionState.RUNNING);

            assertThat(vertex.getExecutionState()).isEqualTo(ExecutionState.RUNNING);

            vertex.cancel();
            vertex.getCurrentExecutionAttempt()
                    .completeCancelling(); // response by task manager once actually canceled

            assertThat(vertex.getExecutionState()).isEqualTo(ExecutionState.CANCELED);

            assertThat(slot.isAlive()).isFalse();

            assertThat(vertex.getFailureInfo()).isNotPresent();

            assertThat(vertex.getStateTimestamp(ExecutionState.CREATED)).isGreaterThan(0);
            assertThat(vertex.getStateTimestamp(ExecutionState.CANCELING)).isGreaterThan(0);
            assertThat(vertex.getStateTimestamp(ExecutionState.CANCELED)).isGreaterThan(0);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    void testRepeatedCancelFromRunning() {
        try {
            final ExecutionVertex vertex = getExecutionVertex();

            LogicalSlot slot =
                    new TestingLogicalSlotBuilder()
                            .setTaskManagerGateway(
                                    new CancelSequenceSimpleAckingTaskManagerGateway(1))
                            .createTestingLogicalSlot();

            setVertexResource(vertex, slot);
            setVertexState(vertex, ExecutionState.RUNNING);

            assertThat(vertex.getExecutionState()).isEqualTo(ExecutionState.RUNNING);

            vertex.cancel();

            assertThat(vertex.getExecutionState()).isEqualTo(ExecutionState.CANCELING);

            vertex.cancel();

            assertThat(vertex.getExecutionState()).isEqualTo(ExecutionState.CANCELING);

            // callback by TaskManager after canceling completes
            vertex.getCurrentExecutionAttempt().completeCancelling();

            assertThat(vertex.getExecutionState()).isEqualTo(ExecutionState.CANCELED);

            assertThat(slot.isAlive()).isFalse();

            assertThat(vertex.getFailureInfo()).isNotPresent();

            assertThat(vertex.getStateTimestamp(ExecutionState.CREATED)).isGreaterThan(0);
            assertThat(vertex.getStateTimestamp(ExecutionState.CANCELING)).isGreaterThan(0);
            assertThat(vertex.getStateTimestamp(ExecutionState.CANCELED)).isGreaterThan(0);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    void testCancelFromRunningDidNotFindTask() {
        // this may happen when the task finished or failed while the call was in progress
        try {
            final ExecutionVertex vertex = getExecutionVertex();

            LogicalSlot slot =
                    new TestingLogicalSlotBuilder()
                            .setTaskManagerGateway(
                                    new CancelSequenceSimpleAckingTaskManagerGateway(1))
                            .createTestingLogicalSlot();

            setVertexResource(vertex, slot);
            setVertexState(vertex, ExecutionState.RUNNING);

            assertThat(vertex.getExecutionState()).isEqualTo(ExecutionState.RUNNING);

            vertex.cancel();

            assertThat(vertex.getExecutionState()).isEqualTo(ExecutionState.CANCELING);

            assertThat(vertex.getFailureInfo()).isNotPresent();

            assertThat(vertex.getStateTimestamp(ExecutionState.CREATED)).isGreaterThan(0);
            assertThat(vertex.getStateTimestamp(ExecutionState.CANCELING)).isGreaterThan(0);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    void testCancelCallFails() {
        try {
            final ExecutionVertex vertex = getExecutionVertex();

            LogicalSlot slot =
                    new TestingLogicalSlotBuilder()
                            .setTaskManagerGateway(
                                    new CancelSequenceSimpleAckingTaskManagerGateway(0))
                            .createTestingLogicalSlot();

            setVertexResource(vertex, slot);
            setVertexState(vertex, ExecutionState.RUNNING);

            assertThat(vertex.getExecutionState()).isEqualTo(ExecutionState.RUNNING);

            vertex.cancel();

            // Callback fails, leading to CANCELED
            assertThat(vertex.getExecutionState()).isEqualTo(ExecutionState.CANCELED);

            assertThat(slot.isAlive()).isFalse();

            assertThat(vertex.getStateTimestamp(ExecutionState.CREATED)).isGreaterThan(0);
            assertThat(vertex.getStateTimestamp(ExecutionState.CANCELING)).isGreaterThan(0);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    void testSendCancelAndReceiveFail() throws Exception {
        final SchedulerBase scheduler =
                SchedulerTestingUtils.createScheduler(
                        JobGraphTestUtils.streamingJobGraph(createNoOpVertex(10)),
                        ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                        EXECUTOR_RESOURCE.getExecutor());
        final ExecutionGraph graph = scheduler.getExecutionGraph();

        scheduler.startScheduling();

        ExecutionGraphTestUtils.switchAllVerticesToRunning(graph);
        assertThat(graph.getState()).isEqualTo(JobStatus.RUNNING);

        final ExecutionVertex[] vertices =
                graph.getVerticesTopologically().iterator().next().getTaskVertices();
        assertThat(graph.getRegisteredExecutions()).hasSize(vertices.length);

        final Execution exec = vertices[3].getCurrentExecutionAttempt();
        exec.cancel();
        assertThat(exec.getState()).isEqualTo(ExecutionState.CANCELING);

        exec.markFailed(new Exception("test"));
        assertThat(
                        exec.getState() == ExecutionState.FAILED
                                || exec.getState() == ExecutionState.CANCELED)
                .isTrue();

        assertThat(exec.getAssignedResource().isAlive()).isFalse();
        assertThat(graph.getRegisteredExecutions()).hasSize(vertices.length - 1);
    }

    private static class CancelSequenceSimpleAckingTaskManagerGateway
            extends SimpleAckingTaskManagerGateway {
        private final int successfulOperations;
        private int index = -1;

        public CancelSequenceSimpleAckingTaskManagerGateway(int successfulOperations) {
            super();
            this.successfulOperations = successfulOperations;
        }

        @Override
        public CompletableFuture<Acknowledge> cancelTask(
                ExecutionAttemptID executionAttemptID, Time timeout) {
            index++;

            if (index >= successfulOperations) {
                return FutureUtils.completedExceptionally(new IOException("Rpc call fails"));
            } else {
                return CompletableFuture.completedFuture(Acknowledge.get());
            }
        }
    }
}
