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
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraphException;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.PartitionInfo;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.TestingAbstractInvokables;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGateway;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGatewayBuilder;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.TaskBackPressureResponse;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor;
import org.apache.flink.runtime.shuffle.PartitionDescriptor;
import org.apache.flink.runtime.shuffle.PartitionDescriptorBuilder;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleEnvironment;
import org.apache.flink.runtime.taskexecutor.slot.TaskSlotTable;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.testtasks.BlockingNoOpInvokable;
import org.apache.flink.runtime.testtasks.OutputBlockedInvokable;
import org.apache.flink.runtime.util.NettyShuffleDescriptorBuilder;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.NetUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.runtime.util.NettyShuffleDescriptorBuilder.createRemoteWithIdAndLocation;
import static org.apache.flink.runtime.util.NettyShuffleDescriptorBuilder.newBuilder;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

/**
 * Tests for submission logic of the {@link TaskExecutor}.
 */
public class TaskExecutorSubmissionTest extends TestLogger {

	private static final long TEST_TIMEOUT = 20000L;

	@Rule
	public final TestName testName = new TestName();

	private static final Time timeout = Time.milliseconds(10000L);

	private JobID jobId = new JobID();

	/**
	 * Tests that we can submit a task to the TaskManager given that we've allocated a slot there.
	 */
	@Test(timeout = TEST_TIMEOUT)
	public void testTaskSubmission() throws Exception {
		final ExecutionAttemptID eid = new ExecutionAttemptID();

		final TaskDeploymentDescriptor tdd = createTestTaskDeploymentDescriptor("test task", eid, FutureCompletingInvokable.class);

		final CompletableFuture<Void> taskRunningFuture = new CompletableFuture<>();

		try (TaskSubmissionTestEnvironment env =
			new TaskSubmissionTestEnvironment.Builder(jobId)
				.setSlotSize(1)
				.addTaskManagerActionListener(eid, ExecutionState.RUNNING, taskRunningFuture)
				.build()) {
			TaskExecutorGateway tmGateway = env.getTaskExecutorGateway();
			TaskSlotTable taskSlotTable = env.getTaskSlotTable();

			taskSlotTable.allocateSlot(0, jobId, tdd.getAllocationId(), Time.seconds(60));
			tmGateway.submitTask(tdd, env.getJobMasterId(), timeout).get();

			taskRunningFuture.get();
		}
	}

	/**
	 * Tests that the TaskManager sends a proper exception back to the sender if the submit task
	 * message fails.
	 */
	@Test(timeout = TEST_TIMEOUT)
	public void testSubmitTaskFailure() throws Exception {
		final ExecutionAttemptID eid = new ExecutionAttemptID();

		final TaskDeploymentDescriptor tdd = createTestTaskDeploymentDescriptor(
			"test task",
			eid,
			BlockingNoOpInvokable.class,
			0); // this will make the submission fail because the number of key groups must be >= 1

		try (TaskSubmissionTestEnvironment env =
			new TaskSubmissionTestEnvironment.Builder(jobId)
				.build()) {
			TaskExecutorGateway tmGateway = env.getTaskExecutorGateway();
			TaskSlotTable taskSlotTable = env.getTaskSlotTable();

			taskSlotTable.allocateSlot(0, jobId, tdd.getAllocationId(), Time.seconds(60));
			tmGateway.submitTask(tdd, env.getJobMasterId(), timeout).get();
		} catch (Exception e) {
			assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
		}
	}

	/**
	 * Tests that we can cancel the task of the TaskManager given that we've submitted it.
	 */
	@Test(timeout = TEST_TIMEOUT)
	public void testTaskSubmissionAndCancelling() throws Exception {
		final ExecutionAttemptID eid1 = new ExecutionAttemptID();
		final ExecutionAttemptID eid2 = new ExecutionAttemptID();

		final TaskDeploymentDescriptor tdd1 = createTestTaskDeploymentDescriptor("test task", eid1, BlockingNoOpInvokable.class);
		final TaskDeploymentDescriptor tdd2 = createTestTaskDeploymentDescriptor("test task", eid2, BlockingNoOpInvokable.class);

		final CompletableFuture<Void> task1RunningFuture = new CompletableFuture<>();
		final CompletableFuture<Void> task2RunningFuture = new CompletableFuture<>();
		final CompletableFuture<Void> task1CanceledFuture = new CompletableFuture<>();

		try (TaskSubmissionTestEnvironment env =
			new TaskSubmissionTestEnvironment.Builder(jobId)
				.setSlotSize(2)
				.addTaskManagerActionListener(eid1, ExecutionState.RUNNING, task1RunningFuture)
				.addTaskManagerActionListener(eid2, ExecutionState.RUNNING, task2RunningFuture)
				.addTaskManagerActionListener(eid1, ExecutionState.CANCELED, task1CanceledFuture)
				.build()) {
			TaskExecutorGateway tmGateway = env.getTaskExecutorGateway();
			TaskSlotTable<Task> taskSlotTable = env.getTaskSlotTable();

			taskSlotTable.allocateSlot(0, jobId, tdd1.getAllocationId(), Time.seconds(60));
			tmGateway.submitTask(tdd1, env.getJobMasterId(), timeout).get();
			task1RunningFuture.get();

			taskSlotTable.allocateSlot(1, jobId, tdd2.getAllocationId(), Time.seconds(60));
			tmGateway.submitTask(tdd2, env.getJobMasterId(), timeout).get();
			task2RunningFuture.get();

			assertSame(taskSlotTable.getTask(eid1).getExecutionState(), ExecutionState.RUNNING);
			assertSame(taskSlotTable.getTask(eid2).getExecutionState(), ExecutionState.RUNNING);

			tmGateway.cancelTask(eid1, timeout);
			task1CanceledFuture.get();

			assertSame(taskSlotTable.getTask(eid1).getExecutionState(), ExecutionState.CANCELED);
			assertSame(taskSlotTable.getTask(eid2).getExecutionState(), ExecutionState.RUNNING);
		}
	}

	/**
	 * Tests that submitted tasks will fail when attempting to send/receive data if no
	 * ResultPartitions/InputGates are set up.
	 */
	@Test(timeout = TEST_TIMEOUT)
	public void testGateChannelEdgeMismatch() throws Exception {
		final ExecutionAttemptID eid1 = new ExecutionAttemptID();
		final ExecutionAttemptID eid2 = new ExecutionAttemptID();

		final TaskDeploymentDescriptor tdd1 =
			createTestTaskDeploymentDescriptor("Sender", eid1, TestingAbstractInvokables.Sender.class);
		final TaskDeploymentDescriptor tdd2 =
			createTestTaskDeploymentDescriptor("Receiver", eid2, TestingAbstractInvokables.Receiver.class);

		final CompletableFuture<Void> task1RunningFuture = new CompletableFuture<>();
		final CompletableFuture<Void> task2RunningFuture = new CompletableFuture<>();
		final CompletableFuture<Void> task1FailedFuture = new CompletableFuture<>();
		final CompletableFuture<Void> task2FailedFuture = new CompletableFuture<>();

		try (TaskSubmissionTestEnvironment env =
			new TaskSubmissionTestEnvironment.Builder(jobId)
				.addTaskManagerActionListener(eid1, ExecutionState.RUNNING, task1RunningFuture)
				.addTaskManagerActionListener(eid2, ExecutionState.RUNNING, task2RunningFuture)
				.addTaskManagerActionListener(eid1, ExecutionState.FAILED, task1FailedFuture)
				.addTaskManagerActionListener(eid2, ExecutionState.FAILED, task2FailedFuture)
				.setSlotSize(2)
				.build()) {
			TaskExecutorGateway tmGateway = env.getTaskExecutorGateway();
			TaskSlotTable<Task> taskSlotTable = env.getTaskSlotTable();

			taskSlotTable.allocateSlot(0, jobId, tdd1.getAllocationId(), Time.seconds(60));
			tmGateway.submitTask(tdd1, env.getJobMasterId(), timeout).get();
			task1RunningFuture.get();

			taskSlotTable.allocateSlot(1, jobId, tdd2.getAllocationId(), Time.seconds(60));
			tmGateway.submitTask(tdd2, env.getJobMasterId(), timeout).get();
			task2RunningFuture.get();

			task1FailedFuture.get();
			task2FailedFuture.get();

			assertSame(taskSlotTable.getTask(eid1).getExecutionState(), ExecutionState.FAILED);
			assertSame(taskSlotTable.getTask(eid2).getExecutionState(), ExecutionState.FAILED);
		}
	}

	@Test(timeout = TEST_TIMEOUT)
	public void testRunJobWithForwardChannel() throws Exception {
		ResourceID producerLocation = ResourceID.generate();
		NettyShuffleDescriptor sdd =
			createRemoteWithIdAndLocation(new IntermediateResultPartitionID(), producerLocation);

		TaskDeploymentDescriptor tdd1 = createSender(sdd);
		TaskDeploymentDescriptor tdd2 = createReceiver(sdd);
		ExecutionAttemptID eid1 = tdd1.getExecutionAttemptId();
		ExecutionAttemptID eid2 = tdd2.getExecutionAttemptId();

		final CompletableFuture<Void> task1RunningFuture = new CompletableFuture<>();
		final CompletableFuture<Void> task2RunningFuture = new CompletableFuture<>();
		final CompletableFuture<Void> task1FinishedFuture = new CompletableFuture<>();
		final CompletableFuture<Void> task2FinishedFuture = new CompletableFuture<>();

		final JobMasterId jobMasterId = JobMasterId.generate();
		TestingJobMasterGateway testingJobMasterGateway =
			new TestingJobMasterGatewayBuilder()
			.setFencingTokenSupplier(() -> jobMasterId)
			.setScheduleOrUpdateConsumersFunction(
				resultPartitionID -> CompletableFuture.completedFuture(Acknowledge.get()))
			.build();

		try (TaskSubmissionTestEnvironment env =
			new TaskSubmissionTestEnvironment.Builder(jobId)
				.setResourceID(producerLocation)
				.setSlotSize(2)
				.addTaskManagerActionListener(eid1, ExecutionState.RUNNING, task1RunningFuture)
				.addTaskManagerActionListener(eid2, ExecutionState.RUNNING, task2RunningFuture)
				.addTaskManagerActionListener(eid1, ExecutionState.FINISHED, task1FinishedFuture)
				.addTaskManagerActionListener(eid2, ExecutionState.FINISHED, task2FinishedFuture)
				.setJobMasterId(jobMasterId)
				.setJobMasterGateway(testingJobMasterGateway)
				.useRealNonMockShuffleEnvironment()
				.build()) {
			TaskExecutorGateway tmGateway = env.getTaskExecutorGateway();
			TaskSlotTable<Task> taskSlotTable = env.getTaskSlotTable();

			taskSlotTable.allocateSlot(0, jobId, tdd1.getAllocationId(), Time.seconds(60));
			tmGateway.submitTask(tdd1, jobMasterId, timeout).get();
			task1RunningFuture.get();

			taskSlotTable.allocateSlot(1, jobId, tdd2.getAllocationId(), Time.seconds(60));
			tmGateway.submitTask(tdd2, jobMasterId, timeout).get();
			task2RunningFuture.get();

			task1FinishedFuture.get();
			task2FinishedFuture.get();

			assertSame(taskSlotTable.getTask(eid1).getExecutionState(), ExecutionState.FINISHED);
			assertSame(taskSlotTable.getTask(eid2).getExecutionState(), ExecutionState.FINISHED);
		}
	}

	/**
	 * This tests creates two tasks. The sender sends data but fails to send the
	 * state update back to the job manager.
	 * the second one blocks to be canceled
	 */
	@Test(timeout = TEST_TIMEOUT)
	public void testCancellingDependentAndStateUpdateFails() throws Exception {
		ResourceID producerLocation = ResourceID.generate();
		NettyShuffleDescriptor sdd =
			createRemoteWithIdAndLocation(new IntermediateResultPartitionID(), producerLocation);

		TaskDeploymentDescriptor tdd1 = createSender(sdd);
		TaskDeploymentDescriptor tdd2 = createReceiver(sdd);
		ExecutionAttemptID eid1 = tdd1.getExecutionAttemptId();
		ExecutionAttemptID eid2 = tdd2.getExecutionAttemptId();

		final CompletableFuture<Void> task1RunningFuture = new CompletableFuture<>();
		final CompletableFuture<Void> task2RunningFuture = new CompletableFuture<>();
		final CompletableFuture<Void> task1FailedFuture = new CompletableFuture<>();
		final CompletableFuture<Void> task2CanceledFuture = new CompletableFuture<>();

		final JobMasterId jobMasterId = JobMasterId.generate();
		TestingJobMasterGateway testingJobMasterGateway =
			new TestingJobMasterGatewayBuilder()
			.setFencingTokenSupplier(() -> jobMasterId)
			.setUpdateTaskExecutionStateFunction(taskExecutionState -> {
				if (taskExecutionState != null && taskExecutionState.getID().equals(eid1)) {
					return FutureUtils.completedExceptionally(
						new ExecutionGraphException("The execution attempt " + eid2 + " was not found."));
				} else {
					return CompletableFuture.completedFuture(Acknowledge.get());
				}
			})
			.build();

		try (TaskSubmissionTestEnvironment env =
			new TaskSubmissionTestEnvironment.Builder(jobId)
				.setResourceID(producerLocation)
				.setSlotSize(2)
				.addTaskManagerActionListener(eid1, ExecutionState.RUNNING, task1RunningFuture)
				.addTaskManagerActionListener(eid2, ExecutionState.RUNNING, task2RunningFuture)
				.addTaskManagerActionListener(eid1, ExecutionState.FAILED, task1FailedFuture)
				.addTaskManagerActionListener(eid2, ExecutionState.CANCELED, task2CanceledFuture)
				.setJobMasterId(jobMasterId)
				.setJobMasterGateway(testingJobMasterGateway)
				.useRealNonMockShuffleEnvironment()
				.build()) {
			TaskExecutorGateway tmGateway = env.getTaskExecutorGateway();
			TaskSlotTable<Task> taskSlotTable = env.getTaskSlotTable();

			taskSlotTable.allocateSlot(0, jobId, tdd1.getAllocationId(), Time.seconds(60));
			tmGateway.submitTask(tdd1, jobMasterId, timeout).get();
			task1RunningFuture.get();

			taskSlotTable.allocateSlot(1, jobId, tdd2.getAllocationId(), Time.seconds(60));
			tmGateway.submitTask(tdd2, jobMasterId, timeout).get();
			task2RunningFuture.get();

			task1FailedFuture.get();
			assertSame(taskSlotTable.getTask(eid1).getExecutionState(), ExecutionState.FAILED);

			tmGateway.cancelTask(eid2, timeout);

			task2CanceledFuture.get();
			assertSame(taskSlotTable.getTask(eid2).getExecutionState(), ExecutionState.CANCELED);
		}
	}

	/**
	 * Tests that repeated remote {@link PartitionNotFoundException}s ultimately fail the receiver.
	 */
	@Test(timeout = TEST_TIMEOUT)
	public void testRemotePartitionNotFound() throws Exception {
		final int dataPort = NetUtils.getAvailablePort();
		Configuration config = new Configuration();
		config.setInteger(NettyShuffleEnvironmentOptions.DATA_PORT, dataPort);
		config.setInteger(NettyShuffleEnvironmentOptions.NETWORK_REQUEST_BACKOFF_INITIAL, 100);
		config.setInteger(NettyShuffleEnvironmentOptions.NETWORK_REQUEST_BACKOFF_MAX, 200);

		// Remote location (on the same TM though) for the partition
		NettyShuffleDescriptor sdd =
			NettyShuffleDescriptorBuilder.newBuilder().setDataPort(dataPort).buildRemote();
		TaskDeploymentDescriptor tdd = createReceiver(sdd);
		ExecutionAttemptID eid = tdd.getExecutionAttemptId();

		final CompletableFuture<Void> taskRunningFuture = new CompletableFuture<>();
		final CompletableFuture<Void> taskFailedFuture = new CompletableFuture<>();

		try (TaskSubmissionTestEnvironment env =
			new TaskSubmissionTestEnvironment.Builder(jobId)
				.setSlotSize(2)
				.addTaskManagerActionListener(eid, ExecutionState.RUNNING, taskRunningFuture)
				.addTaskManagerActionListener(eid, ExecutionState.FAILED, taskFailedFuture)
				.setConfiguration(config)
				.setLocalCommunication(false)
				.useRealNonMockShuffleEnvironment()
				.build()) {
			TaskExecutorGateway tmGateway = env.getTaskExecutorGateway();
			TaskSlotTable<Task> taskSlotTable = env.getTaskSlotTable();

			taskSlotTable.allocateSlot(0, jobId, tdd.getAllocationId(), Time.seconds(60));
			tmGateway.submitTask(tdd, env.getJobMasterId(), timeout).get();
			taskRunningFuture.get();

			taskFailedFuture.get();
			assertThat(taskSlotTable.getTask(eid).getFailureCause(), instanceOf(PartitionNotFoundException.class));
		}
	}

	/**
	 * Tests that the TaskManager fails the task if the partition update fails.
	 */
	@Test
	public void testUpdateTaskInputPartitionsFailure() throws Exception {
		final ExecutionAttemptID eid = new ExecutionAttemptID();

		final TaskDeploymentDescriptor tdd = createTestTaskDeploymentDescriptor("test task", eid, BlockingNoOpInvokable.class);

		final CompletableFuture<Void> taskRunningFuture = new CompletableFuture<>();
		final CompletableFuture<Void> taskFailedFuture = new CompletableFuture<>();
		final ShuffleEnvironment<?, ?> shuffleEnvironment = mock(ShuffleEnvironment.class, Mockito.RETURNS_MOCKS);

		try (TaskSubmissionTestEnvironment env =
			new TaskSubmissionTestEnvironment.Builder(jobId)
				.setShuffleEnvironment(shuffleEnvironment)
				.setSlotSize(1)
				.addTaskManagerActionListener(eid, ExecutionState.RUNNING, taskRunningFuture)
				.addTaskManagerActionListener(eid, ExecutionState.FAILED, taskFailedFuture)
				.build()) {
			TaskExecutorGateway tmGateway = env.getTaskExecutorGateway();
			TaskSlotTable<Task> taskSlotTable = env.getTaskSlotTable();

			taskSlotTable.allocateSlot(0, jobId, tdd.getAllocationId(), Time.seconds(60));
			tmGateway.submitTask(tdd, env.getJobMasterId(), timeout).get();
			taskRunningFuture.get();

			final ResourceID producerLocation = env.getTaskExecutor().getResourceID();
			NettyShuffleDescriptor shuffleDescriptor =
				createRemoteWithIdAndLocation(new IntermediateResultPartitionID(), producerLocation);
			final PartitionInfo partitionUpdate = new PartitionInfo(new IntermediateDataSetID(), shuffleDescriptor);
			doThrow(new IOException()).when(shuffleEnvironment).updatePartitionInfo(eid, partitionUpdate);

			final CompletableFuture<Acknowledge> updateFuture = tmGateway.updatePartitions(
				eid,
				Collections.singletonList(partitionUpdate),
				timeout);

			updateFuture.get();
			taskFailedFuture.get();
			Task task = taskSlotTable.getTask(tdd.getExecutionAttemptId());
			assertThat(task.getExecutionState(), is(ExecutionState.FAILED));
			assertThat(task.getFailureCause(), instanceOf(IOException.class));
		}
	}

	/**
	 *  Tests that repeated local {@link PartitionNotFoundException}s ultimately fail the receiver.
	 */
	@Test(timeout = TEST_TIMEOUT)
	public void testLocalPartitionNotFound() throws Exception {
		ResourceID producerLocation = ResourceID.generate();
		NettyShuffleDescriptor shuffleDescriptor =
			createRemoteWithIdAndLocation(new IntermediateResultPartitionID(), producerLocation);
		TaskDeploymentDescriptor tdd = createReceiver(shuffleDescriptor);
		ExecutionAttemptID eid = tdd.getExecutionAttemptId();

		Configuration config = new Configuration();
		config.setInteger(NettyShuffleEnvironmentOptions.NETWORK_REQUEST_BACKOFF_INITIAL, 100);
		config.setInteger(NettyShuffleEnvironmentOptions.NETWORK_REQUEST_BACKOFF_MAX, 200);

		final CompletableFuture<Void> taskRunningFuture = new CompletableFuture<>();
		final CompletableFuture<Void> taskFailedFuture = new CompletableFuture<>();

		try (TaskSubmissionTestEnvironment env =
			new TaskSubmissionTestEnvironment.Builder(jobId)
				.setResourceID(producerLocation)
				.setSlotSize(1)
				.addTaskManagerActionListener(eid, ExecutionState.RUNNING, taskRunningFuture)
				.addTaskManagerActionListener(eid, ExecutionState.FAILED, taskFailedFuture)
				.setConfiguration(config)
				.useRealNonMockShuffleEnvironment()
				.build()) {
			TaskExecutorGateway tmGateway = env.getTaskExecutorGateway();
			TaskSlotTable<Task> taskSlotTable = env.getTaskSlotTable();

			taskSlotTable.allocateSlot(0, jobId, tdd.getAllocationId(), Time.seconds(60));
			tmGateway.submitTask(tdd, env.getJobMasterId(), timeout).get();
			taskRunningFuture.get();

			taskFailedFuture.get();

			assertSame(taskSlotTable.getTask(eid).getExecutionState(), ExecutionState.FAILED);
			assertThat(taskSlotTable.getTask(eid).getFailureCause(), instanceOf(PartitionNotFoundException.class));
		}
	}

	/**
	 * Test that a failing schedule or update consumers call leads to the failing of the respective
	 * task.
	 *
	 * <p>IMPORTANT: We have to make sure that the invokable's cancel method is called, because only
	 * then the future is completed. We do this by not eagerly deploying consumer tasks and requiring
	 * the invokable to fill one memory segment. The completed memory segment will trigger the
	 * scheduling of the downstream operator since it is in pipeline mode. After we've filled the
	 * memory segment, we'll block the invokable and wait for the task failure due to the failed
	 * schedule or update consumers call.
	 */
	@Test(timeout = TEST_TIMEOUT)
	public void testFailingScheduleOrUpdateConsumers() throws Exception {
		final Configuration configuration = new Configuration();

		// set the memory segment to the smallest size possible, because we have to fill one
		// memory buffer to trigger the schedule or update consumers message to the downstream
		// operators
		configuration.set(TaskManagerOptions.MEMORY_SEGMENT_SIZE, MemorySize.parse("4096"));

		NettyShuffleDescriptor sdd =
			createRemoteWithIdAndLocation(new IntermediateResultPartitionID(), ResourceID.generate());
		TaskDeploymentDescriptor tdd = createSender(sdd, TestingAbstractInvokables.TestInvokableRecordCancel.class);
		ExecutionAttemptID eid = tdd.getExecutionAttemptId();

		final CompletableFuture<Void> taskRunningFuture = new CompletableFuture<>();

		final Exception exception = new Exception("Failed schedule or update consumers");

		final JobMasterId jobMasterId = JobMasterId.generate();
		TestingJobMasterGateway testingJobMasterGateway =
			new TestingJobMasterGatewayBuilder()
				.setFencingTokenSupplier(() -> jobMasterId)
				.setUpdateTaskExecutionStateFunction(resultPartitionID -> FutureUtils.completedExceptionally(exception))
				.build();

		try (TaskSubmissionTestEnvironment env =
			new TaskSubmissionTestEnvironment.Builder(jobId)
				.setSlotSize(1)
				.setConfiguration(configuration)
				.addTaskManagerActionListener(eid, ExecutionState.RUNNING, taskRunningFuture)
				.setJobMasterId(jobMasterId)
				.setJobMasterGateway(testingJobMasterGateway)
				.useRealNonMockShuffleEnvironment()
				.build()) {
			TaskExecutorGateway tmGateway = env.getTaskExecutorGateway();
			TaskSlotTable<Task> taskSlotTable = env.getTaskSlotTable();

			TestingAbstractInvokables.TestInvokableRecordCancel.resetGotCanceledFuture();

			taskSlotTable.allocateSlot(0, jobId, tdd.getAllocationId(), Time.seconds(60));
			tmGateway.submitTask(tdd, jobMasterId, timeout).get();
			taskRunningFuture.get();

			CompletableFuture<Boolean> cancelFuture = TestingAbstractInvokables.TestInvokableRecordCancel.gotCanceled();

			assertTrue(cancelFuture.get());
			assertTrue(ExceptionUtils.findThrowableWithMessage(taskSlotTable.getTask(eid).getFailureCause(), exception.getMessage()).isPresent());
		}
	}

	// ------------------------------------------------------------------------
	// Back pressure request
	// ------------------------------------------------------------------------

	/**
	 * Tests request of task back pressure.
	 */
	@Test(timeout = TEST_TIMEOUT)
	public void testRequestTaskBackPressure() throws Exception {
		final NettyShuffleDescriptor shuffleDescriptor = newBuilder().buildLocal();
		final TaskDeploymentDescriptor tdd = createSender(shuffleDescriptor, OutputBlockedInvokable.class);
		final ExecutionAttemptID executionAttemptID = tdd.getExecutionAttemptId();

		final CompletableFuture<Void> taskRunningFuture = new CompletableFuture<>();
		final CompletableFuture<Void> taskCanceledFuture = new CompletableFuture<>();

		final Configuration configuration = new Configuration();
		configuration.set(WebOptions.BACKPRESSURE_NUM_SAMPLES, 20);
		configuration.set(WebOptions.BACKPRESSURE_DELAY, 5);
		configuration.set(TaskManagerOptions.MEMORY_SEGMENT_SIZE, MemorySize.parse("4096"));

		try (final TaskSubmissionTestEnvironment env = new TaskSubmissionTestEnvironment.Builder(jobId)
					.setSlotSize(1)
					.setConfiguration(configuration)
					.useRealNonMockShuffleEnvironment()
					.addTaskManagerActionListener(executionAttemptID, ExecutionState.RUNNING, taskRunningFuture)
					.addTaskManagerActionListener(executionAttemptID, ExecutionState.CANCELED, taskCanceledFuture)
					.build()) {
			final TaskExecutorGateway tmGateway = env.getTaskExecutorGateway();
			final TaskSlotTable taskSlotTable = env.getTaskSlotTable();

			taskSlotTable.allocateSlot(0, jobId, tdd.getAllocationId(), Time.seconds(60));
			tmGateway.submitTask(tdd, env.getJobMasterId(), timeout).get();
			taskRunningFuture.get();

			// 1) trigger request for non-existing task.
			final int requestId = 1234;
			final ExecutionAttemptID nonExistTaskEid = new ExecutionAttemptID();

			final CompletableFuture<TaskBackPressureResponse> failedRequestFuture =
				tmGateway.requestTaskBackPressure(nonExistTaskEid, requestId, timeout);
			try {
				failedRequestFuture.get();
			} catch (Exception e) {
				assertThat(e.getCause(), instanceOf(IllegalStateException.class));
				assertThat(e.getCause().getMessage(), startsWith("Cannot request back pressure"));
			}

			// 2) trigger request for the blocking task.
			double backPressureRatio = 0;

			for (int i = 0; i < 5; ++i) {
				CompletableFuture<TaskBackPressureResponse> successfulRequestFuture =
					tmGateway.requestTaskBackPressure(executionAttemptID, i, timeout);

				TaskBackPressureResponse response = successfulRequestFuture.get();

				assertEquals(response.getRequestId(), i);
				assertEquals(response.getExecutionAttemptID(), executionAttemptID);

				if ((backPressureRatio = response.getBackPressureRatio()) >= 1.0) {
					break;
				}
			}

			assertEquals("Task was not back pressured in given time.", 1.0, backPressureRatio, 0.0);

			// 3) trigger request for the blocking task, but cancel it before request finishes.
			CompletableFuture<TaskBackPressureResponse> canceledRequestFuture =
				tmGateway.requestTaskBackPressure(executionAttemptID, requestId, timeout);

			tmGateway.cancelTask(executionAttemptID, timeout);
			taskCanceledFuture.get();

			TaskBackPressureResponse responseAfterCancel = canceledRequestFuture.get();

			assertEquals(executionAttemptID, responseAfterCancel.getExecutionAttemptID());
			assertEquals(requestId, responseAfterCancel.getRequestId());
			assertTrue(responseAfterCancel.getBackPressureRatio() > 0);
		}
	}

	private TaskDeploymentDescriptor createSender(NettyShuffleDescriptor shuffleDescriptor) throws IOException {
		return createSender(shuffleDescriptor, TestingAbstractInvokables.Sender.class);
	}

	private TaskDeploymentDescriptor createSender(
			NettyShuffleDescriptor shuffleDescriptor,
			Class<? extends AbstractInvokable> abstractInvokable) throws IOException {
		PartitionDescriptor partitionDescriptor = PartitionDescriptorBuilder
			.newBuilder()
			.setPartitionId(shuffleDescriptor.getResultPartitionID().getPartitionId())
			.build();
		ResultPartitionDeploymentDescriptor resultPartitionDeploymentDescriptor = new ResultPartitionDeploymentDescriptor(
			partitionDescriptor,
			shuffleDescriptor,
			1,
			true);
		return createTestTaskDeploymentDescriptor(
			"Sender",
			shuffleDescriptor.getResultPartitionID().getProducerId(),
			abstractInvokable,
			1,
			Collections.singletonList(resultPartitionDeploymentDescriptor),
			Collections.emptyList());
	}

	private TaskDeploymentDescriptor createReceiver(NettyShuffleDescriptor shuffleDescriptor) throws IOException {
		InputGateDeploymentDescriptor inputGateDeploymentDescriptor = new InputGateDeploymentDescriptor(
			new IntermediateDataSetID(),
			ResultPartitionType.PIPELINED,
			0,
			new ShuffleDescriptor[] {shuffleDescriptor});
		return createTestTaskDeploymentDescriptor(
			"Receiver",
			new ExecutionAttemptID(),
			TestingAbstractInvokables.Receiver.class,
			1,
			Collections.emptyList(),
			Collections.singletonList(inputGateDeploymentDescriptor));
	}

	private TaskDeploymentDescriptor createTestTaskDeploymentDescriptor(
		String taskName,
		ExecutionAttemptID eid,
		Class<? extends AbstractInvokable> abstractInvokable
	) throws IOException {
		return createTestTaskDeploymentDescriptor(taskName, eid, abstractInvokable, 1);
	}

	private TaskDeploymentDescriptor createTestTaskDeploymentDescriptor(
		String taskName,
		ExecutionAttemptID eid,
		Class<? extends AbstractInvokable> abstractInvokable,
		int maxNumberOfSubtasks
	) throws IOException {
		return createTestTaskDeploymentDescriptor(taskName,
			eid,
			abstractInvokable,
			maxNumberOfSubtasks,
			Collections.emptyList(),
			Collections.emptyList());
	}

	private TaskDeploymentDescriptor createTestTaskDeploymentDescriptor(
		String taskName,
		ExecutionAttemptID eid,
		Class<? extends AbstractInvokable> abstractInvokable,
		int maxNumberOfSubtasks,
		List<ResultPartitionDeploymentDescriptor> producedPartitions,
		List<InputGateDeploymentDescriptor> inputGates
	) throws IOException {
		Preconditions.checkNotNull(producedPartitions);
		Preconditions.checkNotNull(inputGates);
		return createTaskDeploymentDescriptor(
			jobId, testName.getMethodName(), eid,
			new SerializedValue<>(new ExecutionConfig()), taskName, maxNumberOfSubtasks, 0, 1, 0,
			new Configuration(), new Configuration(), abstractInvokable.getName(),
			producedPartitions,
			inputGates,
			Collections.emptyList(),
			Collections.emptyList(),
			0);
	}

	static TaskDeploymentDescriptor createTaskDeploymentDescriptor(
			JobID jobId,
			String jobName,
			ExecutionAttemptID executionAttemptId,
			SerializedValue<ExecutionConfig> serializedExecutionConfig,
			String taskName,
			int maxNumberOfSubtasks,
			int subtaskIndex,
			int numberOfSubtasks,
			int attemptNumber,
			Configuration jobConfiguration,
			Configuration taskConfiguration,
			String invokableClassName,
			List<ResultPartitionDeploymentDescriptor> producedPartitions,
			List<InputGateDeploymentDescriptor> inputGates,
			Collection<PermanentBlobKey> requiredJarFiles,
			Collection<URL> requiredClasspaths,
			int targetSlotNumber) throws IOException {

		JobInformation jobInformation = new JobInformation(
			jobId,
			jobName,
			serializedExecutionConfig,
			jobConfiguration,
			requiredJarFiles,
			requiredClasspaths);

		TaskInformation taskInformation = new TaskInformation(
			new JobVertexID(),
			taskName,
			numberOfSubtasks,
			maxNumberOfSubtasks,
			invokableClassName,
			taskConfiguration);

		SerializedValue<JobInformation> serializedJobInformation = new SerializedValue<>(jobInformation);
		SerializedValue<TaskInformation> serializedJobVertexInformation = new SerializedValue<>(taskInformation);

		return new TaskDeploymentDescriptor(
			jobId,
			new TaskDeploymentDescriptor.NonOffloaded<>(serializedJobInformation),
			new TaskDeploymentDescriptor.NonOffloaded<>(serializedJobVertexInformation),
			executionAttemptId,
			new AllocationID(),
			subtaskIndex,
			attemptNumber,
			targetSlotNumber,
			null,
			producedPartitions,
			inputGates);
	}

	/**
	 * Test invokable which completes the given future when executed.
	 */
	public static class FutureCompletingInvokable extends AbstractInvokable {

		static final CompletableFuture<Boolean> COMPLETABLE_FUTURE = new CompletableFuture<>();

		public FutureCompletingInvokable(Environment environment) {
			super(environment);
		}

		@Override
		public void invoke() throws Exception {
			COMPLETABLE_FUTURE.complete(true);
		}
	}
}
