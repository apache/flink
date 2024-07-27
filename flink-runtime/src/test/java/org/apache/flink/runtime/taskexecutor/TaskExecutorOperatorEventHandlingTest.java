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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGatewayBuilder;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.MetricRegistryTestUtils;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.TestOperatorEvent;
import org.apache.flink.runtime.operators.coordination.TestingCoordinationRequestHandler;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.testutils.CancelableInvokable;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.SerializedValue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createExecutionAttemptId;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for the (failure handling of the) delivery of Operator Events. */
class TaskExecutorOperatorEventHandlingTest {

    @RegisterExtension
    private static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_EXTENSION =
            TestingUtils.defaultExecutorExtension();

    private MetricRegistryImpl metricRegistry;

    private TestingRpcService rpcService;

    @BeforeEach
    void setup() {
        rpcService = new TestingRpcService();
        metricRegistry =
                new MetricRegistryImpl(
                        MetricRegistryTestUtils.defaultMetricRegistryConfiguration());
        metricRegistry.startQueryService(rpcService, new ResourceID("mqs"));
    }

    @AfterEach
    void teardown() throws ExecutionException, InterruptedException {
        if (rpcService != null) {
            rpcService.closeAsync().get();
        }

        if (metricRegistry != null) {
            metricRegistry.closeAsync().get();
        }
    }

    @Test
    void eventHandlingInTaskFailureFailsTask() throws Exception {
        final JobID jobId = new JobID();
        final ExecutionAttemptID eid = createExecutionAttemptId(new JobVertexID(), 3, 0);

        try (TaskSubmissionTestEnvironment env =
                createExecutorWithRunningTask(jobId, eid, OperatorEventFailingInvokable.class)) {
            final TaskExecutorGateway tmGateway = env.getTaskExecutorGateway();
            final CompletableFuture<?> resultFuture =
                    tmGateway.sendOperatorEventToTask(
                            eid, new OperatorID(), new SerializedValue<>(new TestOperatorEvent()));

            assertThat(resultFuture)
                    .failsWithin(Duration.ofSeconds(10))
                    .withThrowableOfType(ExecutionException.class)
                    .withCauseInstanceOf(FlinkException.class);
            assertThat(env.getTaskSlotTable().getTask(eid).getExecutionState())
                    .isEqualTo(ExecutionState.FAILED);
        }
    }

    @Test
    void eventToCoordinatorDeliveryFailureFailsTask() throws Exception {
        final JobID jobId = new JobID();
        final ExecutionAttemptID eid = createExecutionAttemptId(new JobVertexID(), 3, 0);

        try (TaskSubmissionTestEnvironment env =
                createExecutorWithRunningTask(jobId, eid, OperatorEventSendingInvokable.class)) {
            final Task task = env.getTaskSlotTable().getTask(eid);

            task.getExecutingThread().join(10_000);
            assertThat(task.getExecutionState()).isEqualTo(ExecutionState.FAILED);
        }
    }

    @Test
    void requestToCoordinatorDeliveryFailureFailsTask() throws Exception {
        final JobID jobId = new JobID();
        final ExecutionAttemptID eid = createExecutionAttemptId(new JobVertexID(), 3, 0);

        try (TaskSubmissionTestEnvironment env =
                createExecutorWithRunningTask(
                        jobId, eid, CoordinationRequestSendingInvokable.class)) {
            final Task task = env.getTaskSlotTable().getTask(eid);

            task.getExecutingThread().join(10_000);
            assertThat(task.getExecutionState()).isEqualTo(ExecutionState.FAILED);
        }
    }

    // ------------------------------------------------------------------------
    //  test setup helpers
    // ------------------------------------------------------------------------

    private TaskSubmissionTestEnvironment createExecutorWithRunningTask(
            JobID jobId,
            ExecutionAttemptID executionAttemptId,
            Class<? extends AbstractInvokable> invokableClass)
            throws Exception {

        final TaskDeploymentDescriptor tdd =
                createTaskDeploymentDescriptor(jobId, executionAttemptId, invokableClass);

        final CompletableFuture<Void> taskRunningFuture = new CompletableFuture<>();

        final JobMasterId token = JobMasterId.generate();
        final TaskSubmissionTestEnvironment env =
                new TaskSubmissionTestEnvironment.Builder(jobId)
                        .setJobMasterId(token)
                        .setSlotSize(1)
                        .addTaskManagerActionListener(
                                executionAttemptId, ExecutionState.RUNNING, taskRunningFuture)
                        .setMetricQueryServiceAddress(
                                metricRegistry.getMetricQueryServiceGatewayRpcAddress())
                        .setJobMasterGateway(
                                new TestingJobMasterGatewayBuilder()
                                        .setFencingTokenSupplier(() -> token)
                                        .setOperatorEventSender(
                                                (eio, oid, value) -> {
                                                    throw new RuntimeException();
                                                })
                                        .setDeliverCoordinationRequestFunction(
                                                (oid, value) -> {
                                                    throw new RuntimeException();
                                                })
                                        .build())
                        .build(EXECUTOR_EXTENSION.getExecutor());

        env.getTaskSlotTable()
                .allocateSlot(0, jobId, tdd.getAllocationId(), Duration.ofSeconds(60));

        final TaskExecutorGateway tmGateway = env.getTaskExecutorGateway();
        tmGateway.submitTask(tdd, env.getJobMasterId(), Time.seconds(10)).get();
        taskRunningFuture.get();

        return env;
    }

    private static TaskDeploymentDescriptor createTaskDeploymentDescriptor(
            JobID jobId,
            ExecutionAttemptID executionAttemptId,
            Class<? extends AbstractInvokable> invokableClass)
            throws IOException {

        return TaskExecutorSubmissionTest.createTaskDeploymentDescriptor(
                jobId,
                "test job",
                executionAttemptId,
                new SerializedValue<>(new ExecutionConfig()),
                "test task",
                64,
                17,
                new Configuration(),
                new Configuration(),
                invokableClass.getName(),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList());
    }

    // ------------------------------------------------------------------------
    //  test mocks
    // ------------------------------------------------------------------------

    /** Test invokable that fails when receiving an operator event. */
    public static final class OperatorEventFailingInvokable extends CancelableInvokable {

        public OperatorEventFailingInvokable(Environment environment) {
            super(environment);
        }

        @Override
        public void doInvoke() throws InterruptedException {
            waitUntilCancelled();
        }

        @Override
        public void dispatchOperatorEvent(OperatorID operator, SerializedValue<OperatorEvent> event)
                throws FlinkException {
            throw new FlinkException("test exception");
        }
    }

    /** Test invokable that fails when receiving an operator event. */
    public static final class OperatorEventSendingInvokable extends CancelableInvokable {

        public OperatorEventSendingInvokable(Environment environment) {
            super(environment);
        }

        @Override
        public void doInvoke() throws Exception {
            getEnvironment()
                    .getOperatorCoordinatorEventGateway()
                    .sendOperatorEventToCoordinator(
                            new OperatorID(), new SerializedValue<>(new TestOperatorEvent()));

            waitUntilCancelled();
        }
    }

    /** Test invokable that fails when receiving a coordination request. */
    public static final class CoordinationRequestSendingInvokable extends CancelableInvokable {

        public CoordinationRequestSendingInvokable(Environment environment) {
            super(environment);
        }

        @Override
        protected void doInvoke() throws Exception {
            getEnvironment()
                    .getOperatorCoordinatorEventGateway()
                    .sendRequestToCoordinator(
                            new OperatorID(),
                            new SerializedValue<>(
                                    new TestingCoordinationRequestHandler.Request<>(0L)));

            waitUntilCancelled();
        }
    }
}
