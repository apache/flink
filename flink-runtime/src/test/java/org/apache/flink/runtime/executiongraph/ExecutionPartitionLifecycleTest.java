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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.blob.VoidBlobWriter;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointRecoveryFactory;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.restart.NoRestartStrategy;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.io.network.partition.NoOpJobMasterPartitionTracker;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.TestingJobMasterPartitionTracker;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.LocationPreferenceConstraint;
import org.apache.flink.runtime.jobmanager.scheduler.ScheduledUnit;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotOwner;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlotBuilder;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.shuffle.NettyShuffleMaster;
import org.apache.flink.runtime.shuffle.PartitionDescriptor;
import org.apache.flink.runtime.shuffle.ProducerDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link Execution}.
 */
public class ExecutionPartitionLifecycleTest extends TestLogger {

	@ClassRule
	public static final TestingComponentMainThreadExecutor.Resource EXECUTOR_RESOURCE =
		new TestingComponentMainThreadExecutor.Resource();

	private Execution execution;
	private ResultPartitionDeploymentDescriptor descriptor;
	private ResourceID taskExecutorResourceId;
	private JobID jobId;

	@Test
	public void testPartitionReleaseOnFinishWhileCanceling() throws Exception {
		testPartitionReleaseOnStateTransitionsAfterRunning(Execution::cancel, Execution::markFinished);
	}

	@Test
	public void testPartitionReleaseOnCancelWhileFinished() throws Exception {
		testPartitionReleaseOnStateTransitionsAfterRunning(Execution::markFinished, Execution::cancel);
	}

	@Test
	public void testPartitionReleaseOnSuspendWhileFinished() throws Exception {
		testPartitionReleaseOnStateTransitionsAfterRunning(Execution::markFinished, Execution::suspend);
	}

	private void testPartitionReleaseOnStateTransitionsAfterRunning(Consumer<Execution> stateTransition1, Consumer<Execution> stateTransition2) throws Exception {
		final SimpleAckingTaskManagerGateway taskManagerGateway = new SimpleAckingTaskManagerGateway();
		final CompletableFuture<Tuple2<JobID, Collection<ResultPartitionID>>> releasePartitionsCallFuture = new CompletableFuture<>();
		taskManagerGateway.setReleasePartitionsConsumer(((jobID, partitionIds) -> releasePartitionsCallFuture.complete(Tuple2.of(jobID, partitionIds))));

		final TestingShuffleMaster testingShuffleMaster = new TestingShuffleMaster();

		setupExecutionGraphAndStartRunningJob(ResultPartitionType.PIPELINED, NoOpJobMasterPartitionTracker.INSTANCE, taskManagerGateway, testingShuffleMaster);

		stateTransition1.accept(execution);
		assertFalse(releasePartitionsCallFuture.isDone());

		stateTransition2.accept(execution);
		assertTrue(releasePartitionsCallFuture.isDone());

		final Tuple2<JobID, Collection<ResultPartitionID>> releasePartitionsCall = releasePartitionsCallFuture.get();
		assertEquals(jobId, releasePartitionsCall.f0);
		assertThat(releasePartitionsCall.f1, contains(descriptor.getShuffleDescriptor().getResultPartitionID()));

		assertEquals(1, testingShuffleMaster.externallyReleasedPartitions.size());
		assertEquals(descriptor.getShuffleDescriptor(), testingShuffleMaster.externallyReleasedPartitions.poll());
	}

	private enum PartitionReleaseResult {
		NONE,
		STOP_TRACKING,
		STOP_TRACKING_AND_RELEASE
	}

	@Test
	public void testPartitionTrackedAndNotReleasedWhenFinished() throws Exception {
		testPartitionTrackingForStateTransition(Execution::markFinished, PartitionReleaseResult.NONE);
	}

	@Test
	public void testPartitionNotTrackedAndNotReleasedWhenCanceledByTM() throws Exception {
		testPartitionTrackingForStateTransition(
			execution -> {
				execution.cancel();
				execution.completeCancelling(Collections.emptyMap(), new IOMetrics(0, 0, 0, 0), false);
			},
			PartitionReleaseResult.STOP_TRACKING);
	}

	@Test
	public void testPartitionNotTrackedAndReleasedWhenCanceledByJM() throws Exception {
		testPartitionTrackingForStateTransition(
			execution -> {
				execution.cancel();
				execution.completeCancelling();
			},
			PartitionReleaseResult.STOP_TRACKING_AND_RELEASE);
	}

	@Test
	public void testPartitionNotTrackedAndNotReleasedWhenFailedByTM() throws Exception {
		testPartitionTrackingForStateTransition(
			execution -> execution.markFailed(
				new Exception("Test exception"),
				Collections.emptyMap(),
				new IOMetrics(0, 0, 0, 0)),
			PartitionReleaseResult.STOP_TRACKING);
	}

	@Test
	public void testPartitionNotTrackedAndReleasedWhenFailedByJM() throws Exception {
		testPartitionTrackingForStateTransition(
			execution -> execution.markFailed(new Exception("Test exception")),
			PartitionReleaseResult.STOP_TRACKING_AND_RELEASE);
	}

	private void testPartitionTrackingForStateTransition(final Consumer<Execution> stateTransition, final PartitionReleaseResult partitionReleaseResult) throws Exception {
		CompletableFuture<Tuple2<ResourceID, ResultPartitionDeploymentDescriptor>> partitionStartTrackingFuture = new CompletableFuture<>();
		CompletableFuture<Collection<ResultPartitionID>> partitionStopTrackingFuture = new CompletableFuture<>();
		CompletableFuture<Collection<ResultPartitionID>> partitionStopTrackingAndReleaseFuture = new CompletableFuture<>();
		final TestingJobMasterPartitionTracker partitionTracker = new TestingJobMasterPartitionTracker();
		partitionTracker.setStartTrackingPartitionsConsumer(
			(resourceID, resultPartitionDeploymentDescriptor) ->
				partitionStartTrackingFuture.complete(Tuple2.of(resourceID, resultPartitionDeploymentDescriptor))
		);
		partitionTracker.setStopTrackingPartitionsConsumer(partitionStopTrackingFuture::complete);
		partitionTracker.setStopTrackingAndReleasePartitionsConsumer(partitionStopTrackingAndReleaseFuture::complete);

		setupExecutionGraphAndStartRunningJob(ResultPartitionType.BLOCKING, partitionTracker, new SimpleAckingTaskManagerGateway(), NettyShuffleMaster.INSTANCE);

		Tuple2<ResourceID, ResultPartitionDeploymentDescriptor> startTrackingCall = partitionStartTrackingFuture.get();
		assertThat(startTrackingCall.f0, equalTo(taskExecutorResourceId));
		assertThat(startTrackingCall.f1, equalTo(descriptor));

		stateTransition.accept(execution);

		switch (partitionReleaseResult) {
			case NONE:
				assertFalse(partitionStopTrackingFuture.isDone());
				assertFalse(partitionStopTrackingAndReleaseFuture.isDone());
				break;
			case STOP_TRACKING:
				assertTrue(partitionStopTrackingFuture.isDone());
				assertFalse(partitionStopTrackingAndReleaseFuture.isDone());
				final Collection<ResultPartitionID> stopTrackingCall = partitionStopTrackingFuture.get();
				assertEquals(Collections.singletonList(descriptor.getShuffleDescriptor().getResultPartitionID()), stopTrackingCall);
				break;
			case STOP_TRACKING_AND_RELEASE:
				assertFalse(partitionStopTrackingFuture.isDone());
				assertTrue(partitionStopTrackingAndReleaseFuture.isDone());
				final Collection<ResultPartitionID> stopTrackingAndReleaseCall = partitionStopTrackingAndReleaseFuture.get();
				assertEquals(Collections.singletonList(descriptor.getShuffleDescriptor().getResultPartitionID()), stopTrackingAndReleaseCall);
				break;
		}
	}

	private void setupExecutionGraphAndStartRunningJob(ResultPartitionType resultPartitionType, JobMasterPartitionTracker partitionTracker, TaskManagerGateway taskManagerGateway, ShuffleMaster<?> shuffleMaster) throws JobException, JobExecutionException {
		final JobVertex producerVertex = createNoOpJobVertex();
		final JobVertex consumerVertex = createNoOpJobVertex();
		consumerVertex.connectNewDataSetAsInput(producerVertex, DistributionPattern.ALL_TO_ALL, resultPartitionType);

		final TaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();

		final SlotProvider slotProvider = new SlotProvider() {
			@Override
			public CompletableFuture<LogicalSlot> allocateSlot(SlotRequestId slotRequestId, ScheduledUnit scheduledUnit, SlotProfile slotProfile, Time allocationTimeout) {
				return CompletableFuture.completedFuture(
					new TestingLogicalSlotBuilder()
						.setTaskManagerLocation(taskManagerLocation)
						.setTaskManagerGateway(taskManagerGateway)
						.setSlotOwner(new SingleSlotTestingSlotOwner())
						.createTestingLogicalSlot());
			}

			@Override
			public void cancelSlotRequest(SlotRequestId slotRequestId, @Nullable SlotSharingGroupId slotSharingGroupId, Throwable cause) {
			}
		};

		final ExecutionGraph executionGraph = ExecutionGraphBuilder.buildGraph(
			null,
			new JobGraph(new JobID(), "test job", producerVertex, consumerVertex),
			new Configuration(),
			TestingUtils.defaultExecutor(),
			TestingUtils.defaultExecutor(),
			slotProvider,
			ExecutionPartitionLifecycleTest.class.getClassLoader(),
			new StandaloneCheckpointRecoveryFactory(),
			Time.seconds(10),
			new NoRestartStrategy(),
			new UnregisteredMetricsGroup(),
			VoidBlobWriter.getInstance(),
			Time.seconds(10),
			log,
			shuffleMaster,
			partitionTracker);

		executionGraph.start(ComponentMainThreadExecutorServiceAdapter.forMainThread());

		final ExecutionJobVertex executionJobVertex = executionGraph.getJobVertex(producerVertex.getID());
		final ExecutionVertex executionVertex = executionJobVertex.getTaskVertices()[0];
		execution = executionVertex.getCurrentExecutionAttempt();

		execution.allocateResourcesForExecution(
			executionGraph.getSlotProviderStrategy(),
			LocationPreferenceConstraint.ALL,
			Collections.emptySet());

		execution.deploy();
		execution.switchToRunning();

		final IntermediateResultPartitionID expectedIntermediateResultPartitionId = executionJobVertex
			.getProducedDataSets()[0]
			.getPartitions()[0]
			.getPartitionId();

		descriptor = execution
			.getResultPartitionDeploymentDescriptor(expectedIntermediateResultPartitionId).get();
		taskExecutorResourceId = taskManagerLocation.getResourceID();
		jobId = executionGraph.getJobID();
	}

	@Nonnull
	private JobVertex createNoOpJobVertex() {
		final JobVertex jobVertex = new JobVertex("Test vertex", new JobVertexID());
		jobVertex.setInvokableClass(NoOpInvokable.class);

		return jobVertex;
	}

	/**
	 * Slot owner which records the first returned slot.
	 */
	private static final class SingleSlotTestingSlotOwner implements SlotOwner {

		final CompletableFuture<LogicalSlot> returnedSlot = new CompletableFuture<>();

		@Override
		public void returnLogicalSlot(LogicalSlot logicalSlot) {
			returnedSlot.complete(logicalSlot);
		}
	}

	private static class TestingShuffleMaster implements ShuffleMaster<ShuffleDescriptor> {

		final Queue<ShuffleDescriptor> externallyReleasedPartitions = new ArrayBlockingQueue<>(4);

		@Override
		public CompletableFuture<ShuffleDescriptor> registerPartitionWithProducer(PartitionDescriptor partitionDescriptor, ProducerDescriptor producerDescriptor) {
			return CompletableFuture.completedFuture(new ShuffleDescriptor() {
				@Override
				public ResultPartitionID getResultPartitionID() {
					return new ResultPartitionID(
						partitionDescriptor.getPartitionId(),
						producerDescriptor.getProducerExecutionId());
				}

				@Override
				public Optional<ResourceID> storesLocalResourcesOn() {
					return Optional.of(producerDescriptor.getProducerLocation());
				}
			});
		}

		@Override
		public void releasePartitionExternally(ShuffleDescriptor shuffleDescriptor) {
			externallyReleasedPartitions.add(shuffleDescriptor);
		}
	}
}
