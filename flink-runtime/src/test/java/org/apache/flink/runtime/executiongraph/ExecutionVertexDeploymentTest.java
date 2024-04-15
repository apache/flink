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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlot;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlotBuilder;
import org.apache.flink.runtime.messages.Acknowledge;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.getExecutionVertex;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

class ExecutionVertexDeploymentTest {

    private static final String ERROR_MESSAGE = "test_failure_error_message";

    @Test
    void testDeployCall() {
        try {
            final ExecutionVertex vertex = getExecutionVertex();

            final LogicalSlot slot = new TestingLogicalSlotBuilder().createTestingLogicalSlot();

            assertThat(vertex.getExecutionState()).isEqualTo(ExecutionState.CREATED);
            vertex.getCurrentExecutionAttempt().transitionState(ExecutionState.SCHEDULED);
            vertex.deployToSlot(slot);
            assertThat(vertex.getExecutionState()).isEqualTo(ExecutionState.DEPLOYING);

            // no repeated scheduling
            try {
                vertex.deployToSlot(slot);
                fail("Scheduled from wrong state");
            } catch (IllegalStateException e) {
                // as expected
            }

            assertThat(vertex.getFailureInfo()).isNotPresent();

            assertThat(vertex.getStateTimestamp(ExecutionState.CREATED)).isGreaterThan(0);
            assertThat(vertex.getStateTimestamp(ExecutionState.DEPLOYING)).isGreaterThan(0);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    void testDeployWithSynchronousAnswer() {
        try {
            final ExecutionVertex vertex = getExecutionVertex();

            final LogicalSlot slot = new TestingLogicalSlotBuilder().createTestingLogicalSlot();

            assertThat(vertex.getExecutionState()).isEqualTo(ExecutionState.CREATED);
            vertex.getCurrentExecutionAttempt().transitionState(ExecutionState.SCHEDULED);

            vertex.deployToSlot(slot);

            assertThat(vertex.getExecutionState()).isEqualTo(ExecutionState.DEPLOYING);

            // no repeated scheduling
            try {
                vertex.deployToSlot(slot);
                fail("Scheduled from wrong state");
            } catch (IllegalStateException e) {
                // as expected
            }

            assertThat(vertex.getFailureInfo()).isNotPresent();

            assertThat(vertex.getStateTimestamp(ExecutionState.CREATED)).isGreaterThan(0);
            assertThat(vertex.getStateTimestamp(ExecutionState.DEPLOYING)).isGreaterThan(0);
            assertThat(vertex.getStateTimestamp(ExecutionState.RUNNING)).isZero();
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    void testDeployWithAsynchronousAnswer() {
        try {
            final ExecutionVertex vertex = getExecutionVertex();

            final LogicalSlot slot = new TestingLogicalSlotBuilder().createTestingLogicalSlot();

            assertThat(vertex.getExecutionState()).isEqualTo(ExecutionState.CREATED);
            vertex.getCurrentExecutionAttempt().transitionState(ExecutionState.SCHEDULED);

            vertex.deployToSlot(slot);

            // no repeated scheduling
            try {
                vertex.deployToSlot(slot);
                fail("Scheduled from wrong state");
            } catch (IllegalStateException e) {
                // as expected
            }

            assertThat(vertex.getExecutionState()).isEqualTo(ExecutionState.DEPLOYING);

            // no repeated scheduling
            try {
                vertex.deployToSlot(slot);
                fail("Scheduled from wrong state");
            } catch (IllegalStateException e) {
                // as expected
            }

            assertThat(vertex.getStateTimestamp(ExecutionState.CREATED)).isGreaterThan(0);
            assertThat(vertex.getStateTimestamp(ExecutionState.DEPLOYING)).isGreaterThan(0);
            assertThat(vertex.getStateTimestamp(ExecutionState.RUNNING)).isZero();
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    void testDeployFailedSynchronous() {
        try {
            final ExecutionVertex vertex = getExecutionVertex();

            final LogicalSlot slot =
                    new TestingLogicalSlotBuilder()
                            .setTaskManagerGateway(
                                    new SubmitFailingSimpleAckingTaskManagerGateway())
                            .createTestingLogicalSlot();

            assertThat(vertex.getExecutionState()).isEqualTo(ExecutionState.CREATED);
            vertex.getCurrentExecutionAttempt().transitionState(ExecutionState.SCHEDULED);

            vertex.deployToSlot(slot);

            assertThat(vertex.getExecutionState()).isEqualTo(ExecutionState.FAILED);
            assertThat(vertex.getFailureInfo()).isPresent();
            assertThat(vertex.getFailureInfo().map(ErrorInfo::getExceptionAsString).get())
                    .contains(ERROR_MESSAGE);

            assertThat(vertex.getStateTimestamp(ExecutionState.CREATED)).isGreaterThan(0);
            assertThat(vertex.getStateTimestamp(ExecutionState.DEPLOYING)).isGreaterThan(0);
            assertThat(vertex.getStateTimestamp(ExecutionState.FAILED)).isGreaterThan(0);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    void testDeployFailedAsynchronously() {
        try {
            final ExecutionVertex vertex = getExecutionVertex();

            final LogicalSlot slot =
                    new TestingLogicalSlotBuilder()
                            .setTaskManagerGateway(
                                    new SubmitFailingSimpleAckingTaskManagerGateway())
                            .createTestingLogicalSlot();

            assertThat(vertex.getExecutionState()).isEqualTo(ExecutionState.CREATED);
            vertex.getCurrentExecutionAttempt().transitionState(ExecutionState.SCHEDULED);

            vertex.deployToSlot(slot);

            // wait until the state transition must be done
            for (int i = 0; i < 100; i++) {
                if (vertex.getExecutionState() == ExecutionState.FAILED
                        && vertex.getFailureInfo().isPresent()) {
                    break;
                } else {
                    Thread.sleep(10);
                }
            }

            assertThat(vertex.getExecutionState()).isEqualTo(ExecutionState.FAILED);
            assertThat(vertex.getFailureInfo()).isPresent();
            assertThat(vertex.getFailureInfo().map(ErrorInfo::getExceptionAsString).get())
                    .contains(ERROR_MESSAGE);

            assertThat(vertex.getStateTimestamp(ExecutionState.CREATED)).isGreaterThan(0);
            assertThat(vertex.getStateTimestamp(ExecutionState.DEPLOYING)).isGreaterThan(0);
            assertThat(vertex.getStateTimestamp(ExecutionState.FAILED)).isGreaterThan(0);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    void testFailExternallyDuringDeploy() {
        try {
            final ExecutionVertex vertex = getExecutionVertex();

            TestingLogicalSlot testingLogicalSlot =
                    new TestingLogicalSlotBuilder()
                            .setTaskManagerGateway(
                                    new SubmitBlockingSimpleAckingTaskManagerGateway())
                            .createTestingLogicalSlot();

            assertThat(vertex.getExecutionState()).isEqualTo(ExecutionState.CREATED);
            vertex.getCurrentExecutionAttempt().transitionState(ExecutionState.SCHEDULED);
            vertex.deployToSlot(testingLogicalSlot);
            assertThat(vertex.getExecutionState()).isEqualTo(ExecutionState.DEPLOYING);

            Exception testError = new Exception("test error");
            vertex.fail(testError);

            assertThat(vertex.getExecutionState()).isEqualTo(ExecutionState.FAILED);
            assertThat(
                            vertex.getFailureInfo()
                                    .map(ErrorInfo::getException)
                                    .get()
                                    .deserializeError(ClassLoader.getSystemClassLoader()))
                    .isEqualTo(testError);

            assertThat(vertex.getStateTimestamp(ExecutionState.CREATED)).isGreaterThan(0);
            assertThat(vertex.getStateTimestamp(ExecutionState.DEPLOYING)).isGreaterThan(0);
            assertThat(vertex.getStateTimestamp(ExecutionState.FAILED)).isGreaterThan(0);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    public static class SubmitFailingSimpleAckingTaskManagerGateway
            extends SimpleAckingTaskManagerGateway {
        @Override
        public CompletableFuture<Acknowledge> submitTask(
                TaskDeploymentDescriptor tdd, Time timeout) {
            CompletableFuture<Acknowledge> future = new CompletableFuture<>();
            future.completeExceptionally(new Exception(ERROR_MESSAGE));
            return future;
        }
    }

    private static class SubmitBlockingSimpleAckingTaskManagerGateway
            extends SimpleAckingTaskManagerGateway {
        @Override
        public CompletableFuture<Acknowledge> submitTask(
                TaskDeploymentDescriptor tdd, Time timeout) {
            return new CompletableFuture<>();
        }
    }
}
