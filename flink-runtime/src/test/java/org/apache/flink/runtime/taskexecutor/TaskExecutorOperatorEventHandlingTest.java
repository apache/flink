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
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGatewayBuilder;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.TestOperatorEvent;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.testutils.CancelableInvokable;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.core.testutils.FlinkMatchers.futureWillCompleteExceptionally;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Test for the (failure handling of the) delivery of Operator Events.
 */
public class TaskExecutorOperatorEventHandlingTest extends TestLogger {

	private MetricRegistryImpl metricRegistry;

	private TestingRpcService rpcService;

	@Before
	public void setup() {
		rpcService = new TestingRpcService();
		metricRegistry = new MetricRegistryImpl(MetricRegistryConfiguration.defaultMetricRegistryConfiguration());
		metricRegistry.startQueryService(rpcService, new ResourceID("mqs"));
	}

	@After
	public void teardown() throws ExecutionException, InterruptedException {
		if (rpcService != null) {
			rpcService.stopService().get();
		}

		if (metricRegistry != null) {
			metricRegistry.shutdown().get();
		}
	}

	@Test
	public void eventHandlingInTaskFailureFailsTask() throws Exception {
		final JobID jobId = new JobID();
		final ExecutionAttemptID eid = new ExecutionAttemptID();

		try (TaskSubmissionTestEnvironment env = createExecutorWithRunningTask(jobId, eid, OperatorEventFailingInvokable.class)) {
			final TaskExecutorGateway tmGateway = env.getTaskExecutorGateway();
			final CompletableFuture<?> resultFuture = tmGateway.sendOperatorEventToTask(eid, new OperatorID(), new SerializedValue<>(null));

			assertThat(resultFuture, futureWillCompleteExceptionally(FlinkException.class, Duration.ofSeconds(10)));
			assertEquals(ExecutionState.FAILED, env.getTaskSlotTable().getTask(eid).getExecutionState());
		}
	}

	@Test
	public void eventToCoordinatorDeliveryFailureFailsTask() throws Exception {
		final JobID jobId = new JobID();
		final ExecutionAttemptID eid = new ExecutionAttemptID();

		try (TaskSubmissionTestEnvironment env = createExecutorWithRunningTask(jobId, eid, OperatorEventSendingInvokable.class)) {
			final Task task = env.getTaskSlotTable().getTask(eid);

			task.getExecutingThread().join(10_000);
			assertEquals(ExecutionState.FAILED, task.getExecutionState());
		}
	}

	// ------------------------------------------------------------------------
	//  test setup helpers
	// ------------------------------------------------------------------------

	private TaskSubmissionTestEnvironment createExecutorWithRunningTask(
			JobID jobId,
			ExecutionAttemptID executionAttemptId,
			Class<? extends AbstractInvokable> invokableClass) throws Exception {

		final TaskDeploymentDescriptor tdd = createTaskDeploymentDescriptor(
				jobId, executionAttemptId, invokableClass);

		final CompletableFuture<Void> taskRunningFuture = new CompletableFuture<>();

		final JobMasterId token = JobMasterId.generate();
		final TaskSubmissionTestEnvironment env = new TaskSubmissionTestEnvironment.Builder(jobId)
				.setJobMasterId(token)
				.setSlotSize(1)
				.addTaskManagerActionListener(executionAttemptId, ExecutionState.RUNNING, taskRunningFuture)
				.setMetricQueryServiceAddress(metricRegistry.getMetricQueryServiceGatewayRpcAddress())
				.setJobMasterGateway(new TestingJobMasterGatewayBuilder()
					.setFencingTokenSupplier(() -> token)
					.setOperatorEventSender((eio, oid, value) -> {
						throw new RuntimeException();
					})
					.build())
				.build();

		env.getTaskSlotTable().allocateSlot(0, jobId, tdd.getAllocationId(), Time.seconds(60));

		final TaskExecutorGateway tmGateway = env.getTaskExecutorGateway();
		tmGateway.submitTask(tdd, env.getJobMasterId(), Time.seconds(10)).get();
		taskRunningFuture.get();

		return env;
	}

	private static TaskDeploymentDescriptor createTaskDeploymentDescriptor(
			JobID jobId,
			ExecutionAttemptID executionAttemptId,
			Class<? extends AbstractInvokable> invokableClass) throws IOException {

		return TaskExecutorSubmissionTest.createTaskDeploymentDescriptor(
				jobId,
				"test job",
				executionAttemptId,
				new SerializedValue<>(new ExecutionConfig()),
				"test task",
				64,
				3,
				17,
				0,
				new Configuration(),
				new Configuration(),
				invokableClass.getName(),
				Collections.emptyList(),
				Collections.emptyList(),
				Collections.emptyList(),
				Collections.emptyList(),
				0);
	}

	// ------------------------------------------------------------------------
	//  test mocks
	// ------------------------------------------------------------------------

	/**
	 * Test invokable that fails when receiving an operator event.
	 */
	public static final class OperatorEventFailingInvokable extends CancelableInvokable {

		public OperatorEventFailingInvokable(Environment environment) {
			super(environment);
		}

		@Override
		public void invoke() throws InterruptedException {
			waitUntilCancelled();
		}

		@Override
		public void dispatchOperatorEvent(OperatorID operator, SerializedValue<OperatorEvent> event) throws FlinkException {
			throw new FlinkException("test exception");
		}
	}

	/**
	 * Test invokable that fails when receiving an operator event.
	 */
	public static final class OperatorEventSendingInvokable extends CancelableInvokable {

		public OperatorEventSendingInvokable(Environment environment) {
			super(environment);
		}

		@Override
		public void invoke() throws Exception {
			getEnvironment().getOperatorCoordinatorEventGateway()
				.sendOperatorEventToCoordinator(new OperatorID(), new SerializedValue<>(new TestOperatorEvent()));

			waitUntilCancelled();
		}
	}
}
