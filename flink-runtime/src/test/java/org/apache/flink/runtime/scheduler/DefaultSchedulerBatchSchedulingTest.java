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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.RpcTaskManagerGateway;
import org.apache.flink.runtime.jobmaster.slotpool.LocationPreferenceSlotSelectionStrategy;
import org.apache.flink.runtime.jobmaster.slotpool.SchedulerImpl;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPool;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPoolBuilder;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPoolImpl;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPoolUtils;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.CheckedSupplier;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;

/**
 * Tests base for the scheduling of batch jobs.
 */
public class DefaultSchedulerBatchSchedulingTest extends TestLogger {

	protected final Logger log = LoggerFactory.getLogger(getClass());

	private static final JobID jobId = new JobID();

	private static ScheduledExecutorService singleThreadScheduledExecutorService;
	private static ComponentMainThreadExecutor mainThreadExecutor;

	@BeforeClass
	public static void setupClass() {
		singleThreadScheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
		mainThreadExecutor = ComponentMainThreadExecutorServiceAdapter.forSingleThreadExecutor(singleThreadScheduledExecutorService);
	}

	@AfterClass
	public static void teardownClass() {
		if (singleThreadScheduledExecutorService != null) {
			singleThreadScheduledExecutorService.shutdownNow();
		}
	}

	/**
	 * Tests that a batch job can be executed with fewer slots than its parallelism.
	 * See FLINK-13187 for more information.
	 */
	@Test
	public void testSchedulingOfJobWithFewerSlotsThanParallelism() throws Exception {
		final int parallelism = 5;
		final Time batchSlotTimeout = Time.milliseconds(5L);
		final JobGraph jobGraph = createJobGraph(parallelism);
		jobGraph.setScheduleMode(ScheduleMode.LAZY_FROM_SOURCES_WITH_BATCH_SLOT_REQUEST);

		try (final SlotPoolImpl slotPool = createSlotPool(mainThreadExecutor, batchSlotTimeout)) {
			final ArrayBlockingQueue<ExecutionAttemptID> submittedTasksQueue = new ArrayBlockingQueue<>(parallelism);
			TestingTaskExecutorGateway testingTaskExecutorGateway = new TestingTaskExecutorGatewayBuilder()
				.setSubmitTaskConsumer(
					(tdd, ignored) -> {
						submittedTasksQueue.offer(tdd.getExecutionAttemptId());
						return CompletableFuture.completedFuture(Acknowledge.get());
					})
				.createTestingTaskExecutorGateway();

			// register a single slot at the slot pool
			SlotPoolUtils.offerSlots(
				slotPool,
				mainThreadExecutor,
				Collections.singletonList(ResourceProfile.ANY),
				new RpcTaskManagerGateway(testingTaskExecutorGateway, JobMasterId.generate()));

			final SlotProvider slotProvider = createSlotProvider(slotPool, mainThreadExecutor);
			final SchedulerNG scheduler = createScheduler(jobGraph, slotProvider, batchSlotTimeout);

			final GloballyTerminalJobStatusListener jobStatusListener = new GloballyTerminalJobStatusListener();
			scheduler.registerJobStatusListener(jobStatusListener);
			startScheduling(scheduler, mainThreadExecutor);

			// wait until the batch slot timeout has been reached
			Thread.sleep(batchSlotTimeout.toMilliseconds());

			final CompletableFuture<JobStatus> terminationFuture = jobStatusListener.getTerminationFuture();

			for (int i = 0; i < parallelism; i++) {
				final CompletableFuture<ExecutionAttemptID> submittedTaskFuture = CompletableFuture.supplyAsync(CheckedSupplier.unchecked(submittedTasksQueue::take));

				// wait until one of them is completed
				CompletableFuture.anyOf(submittedTaskFuture, terminationFuture).join();

				if (submittedTaskFuture.isDone()) {
					finishExecution(submittedTaskFuture.get(), scheduler, mainThreadExecutor);
				} else {
					fail(String.format("Job reached a globally terminal state %s before all executions were finished.", terminationFuture.get()));
				}
			}

			assertThat(terminationFuture.get(), is(JobStatus.FINISHED));
		}
	}

	private void finishExecution(
			ExecutionAttemptID executionAttemptId,
			SchedulerNG scheduler,
			ComponentMainThreadExecutor mainThreadExecutor) {
		CompletableFuture.runAsync(
			() -> {
				scheduler.updateTaskExecutionState(new TaskExecutionState(jobId, executionAttemptId, ExecutionState.RUNNING));
				scheduler.updateTaskExecutionState(new TaskExecutionState(jobId, executionAttemptId, ExecutionState.FINISHED));
			},
			mainThreadExecutor
		).join();
	}

	@Nonnull
	private SlotProvider createSlotProvider(SlotPool slotPool, ComponentMainThreadExecutor mainThreadExecutor) {
		final SchedulerImpl scheduler = new SchedulerImpl(LocationPreferenceSlotSelectionStrategy.createDefault(), slotPool);
		scheduler.start(mainThreadExecutor);

		return scheduler;
	}

	private void startScheduling(SchedulerNG scheduler, ComponentMainThreadExecutor mainThreadExecutor) {
		scheduler.initialize(mainThreadExecutor);
		CompletableFuture.runAsync(
			scheduler::startScheduling,
			mainThreadExecutor)
			.join();
	}

	private SlotPoolImpl createSlotPool(ComponentMainThreadExecutor mainThreadExecutor, Time batchSlotTimeout) throws Exception {
		return new SlotPoolBuilder(mainThreadExecutor)
			.setBatchSlotTimeout(batchSlotTimeout)
			.build();
	}

	private JobGraph createJobGraph(int parallelism) {
		final JobVertex jobVertex = new JobVertex("testing task");
		jobVertex.setParallelism(parallelism);
		jobVertex.setInvokableClass(NoOpInvokable.class);
		return new JobGraph(jobId, "test job", jobVertex);
	}

	private static class GloballyTerminalJobStatusListener implements JobStatusListener {

		private final CompletableFuture<JobStatus> globallyTerminalJobStatusFuture = new CompletableFuture<>();

		@Override
		public void jobStatusChanges(JobID jobId, JobStatus newJobStatus, long timestamp, Throwable error) {
			if (newJobStatus.isGloballyTerminalState()) {
				globallyTerminalJobStatusFuture.complete(newJobStatus);
			}
		}

		public CompletableFuture<JobStatus> getTerminationFuture() {
			return globallyTerminalJobStatusFuture;
		}
	}

	private SchedulerNG createScheduler(JobGraph jobGraph, SlotProvider slotProvider, Time slotRequestTimeout) throws Exception {
		return SchedulerTestingUtils.createScheduler(jobGraph, slotProvider, slotRequestTimeout);
	}
}
