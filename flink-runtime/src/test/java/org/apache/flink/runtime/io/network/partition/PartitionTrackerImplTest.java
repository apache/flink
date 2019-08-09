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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.shuffle.PartitionDescriptor;
import org.apache.flink.runtime.shuffle.ProducerDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;

/**
 * Tests for the {@link PartitionTrackerImpl}.
 */
public class PartitionTrackerImplTest extends TestLogger {

	@Test
	public void testPipelinedPartitionIsNotTracked() {
		testReleaseOnConsumptionHandling(ResultPartitionType.PIPELINED);
	}

	@Test
	public void testBlockingPartitionIsTracked() {
		testReleaseOnConsumptionHandling(ResultPartitionType.BLOCKING);
	}

	private void testReleaseOnConsumptionHandling(ResultPartitionType resultPartitionType) {
		final PartitionTracker partitionTracker = new PartitionTrackerImpl(
			new JobID(),
			new TestingShuffleMaster(),
			ignored -> Optional.empty()
		);

		final ResourceID resourceId = ResourceID.generate();
		final ResultPartitionID resultPartitionId = new ResultPartitionID();
		partitionTracker.startTrackingPartition(
			resourceId,
			createResultPartitionDeploymentDescriptor(
				resultPartitionId,
				resultPartitionType,
				false));

		assertThat(partitionTracker.isTrackingPartitionsFor(resourceId), is(resultPartitionType.isBlocking()));
	}

	@Test
	public void testStartStopTracking() {
		final Queue<Tuple3<ResourceID, JobID, Collection<ResultPartitionID>>> taskExecutorReleaseCalls = new ArrayBlockingQueue<>(4);
		final PartitionTracker partitionTracker = new PartitionTrackerImpl(
			new JobID(),
			new TestingShuffleMaster(),
			resourceId -> Optional.of(createTaskExecutorGateway(resourceId, taskExecutorReleaseCalls))
		);

		final ResourceID executorWithTrackedPartition = new ResourceID("tracked");
		final ResourceID executorWithoutTrackedPartition = new ResourceID("untracked");

		assertThat(partitionTracker.isTrackingPartitionsFor(executorWithTrackedPartition), is(false));
		assertThat(partitionTracker.isTrackingPartitionsFor(executorWithoutTrackedPartition), is(false));

		partitionTracker.startTrackingPartition(
			executorWithTrackedPartition,
			createResultPartitionDeploymentDescriptor(new ResultPartitionID(), true));

		assertThat(partitionTracker.isTrackingPartitionsFor(executorWithTrackedPartition), is(true));
		assertThat(partitionTracker.isTrackingPartitionsFor(executorWithoutTrackedPartition), is(false));

		partitionTracker.stopTrackingPartitionsFor(executorWithTrackedPartition);

		assertThat(partitionTracker.isTrackingPartitionsFor(executorWithTrackedPartition), is(false));
		assertThat(partitionTracker.isTrackingPartitionsFor(executorWithoutTrackedPartition), is(false));
	}

	@Test
	public void testReleaseCallsWithLocalResources() {
		final TestingShuffleMaster shuffleMaster = new TestingShuffleMaster();
		final JobID jobId = new JobID();

		final Queue<Tuple3<ResourceID, JobID, Collection<ResultPartitionID>>> taskExecutorReleaseCalls = new ArrayBlockingQueue<>(4);
		final PartitionTracker partitionTracker = new PartitionTrackerImpl(
			jobId,
			shuffleMaster,
			resourceId -> Optional.of(createTaskExecutorGateway(resourceId, taskExecutorReleaseCalls))
		);

		final ResourceID taskExecutorId1 = ResourceID.generate();
		final ResourceID taskExecutorId2 = ResourceID.generate();
		final ResultPartitionID resultPartitionId1 = new ResultPartitionID();
		final ResultPartitionID resultPartitionId2 = new ResultPartitionID();

		partitionTracker.startTrackingPartition(
			taskExecutorId1,
			createResultPartitionDeploymentDescriptor(resultPartitionId1, true));
		partitionTracker.startTrackingPartition(
			taskExecutorId2,
			createResultPartitionDeploymentDescriptor(resultPartitionId2, true));

		{
			partitionTracker.stopTrackingAndReleasePartitionsFor(taskExecutorId1);

			assertEquals(1, taskExecutorReleaseCalls.size());

			Tuple3<ResourceID, JobID, Collection<ResultPartitionID>> taskExecutorReleaseCall = taskExecutorReleaseCalls.remove();
			assertEquals(taskExecutorId1, taskExecutorReleaseCall.f0);
			assertEquals(jobId, taskExecutorReleaseCall.f1);
			assertThat(taskExecutorReleaseCall.f2, contains(resultPartitionId1));

			assertEquals(1, shuffleMaster.externallyReleasedPartitions.size());
			assertEquals(resultPartitionId1, shuffleMaster.externallyReleasedPartitions.remove());

			assertThat(partitionTracker.isTrackingPartitionsFor(taskExecutorId1), is(false));
		}

		{
			partitionTracker.stopTrackingAndReleasePartitions(Collections.singletonList(resultPartitionId2));

			assertEquals(1, taskExecutorReleaseCalls.size());

			Tuple3<ResourceID, JobID, Collection<ResultPartitionID>> releaseCall = taskExecutorReleaseCalls.remove();
			assertEquals(taskExecutorId2, releaseCall.f0);
			assertEquals(jobId, releaseCall.f1);
			assertThat(releaseCall.f2, contains(resultPartitionId2));

			assertEquals(1, shuffleMaster.externallyReleasedPartitions.size());
			assertEquals(resultPartitionId2, shuffleMaster.externallyReleasedPartitions.remove());

			assertThat(partitionTracker.isTrackingPartitionsFor(taskExecutorId2), is(false));
		}
	}

	@Test
	public void testReleaseCallsWithoutLocalResources() {
		final TestingShuffleMaster shuffleMaster = new TestingShuffleMaster();

		final Queue<Tuple3<ResourceID, JobID, Collection<ResultPartitionID>>> taskExecutorReleaseCalls = new ArrayBlockingQueue<>(4);
		final PartitionTracker partitionTracker = new PartitionTrackerImpl(
			new JobID(),
			shuffleMaster,
			resourceId -> Optional.of(createTaskExecutorGateway(resourceId, taskExecutorReleaseCalls))
		);

		final ResourceID taskExecutorId1 = ResourceID.generate();
		final ResourceID taskExecutorId2 = ResourceID.generate();
		final ResultPartitionID resultPartitionId1 = new ResultPartitionID();
		final ResultPartitionID resultPartitionId2 = new ResultPartitionID();

		partitionTracker.startTrackingPartition(
			taskExecutorId1,
			createResultPartitionDeploymentDescriptor(resultPartitionId1, false));
		partitionTracker.startTrackingPartition(
			taskExecutorId2,
			createResultPartitionDeploymentDescriptor(resultPartitionId2, false));

		{
			partitionTracker.stopTrackingAndReleasePartitionsFor(taskExecutorId1);

			assertEquals(0, taskExecutorReleaseCalls.size());

			assertEquals(1, shuffleMaster.externallyReleasedPartitions.size());
			assertEquals(resultPartitionId1, shuffleMaster.externallyReleasedPartitions.remove());

			assertThat(partitionTracker.isTrackingPartitionsFor(taskExecutorId1), is(false));
		}

		{
			partitionTracker.stopTrackingAndReleasePartitions(Collections.singletonList(resultPartitionId2));

			assertEquals(0, taskExecutorReleaseCalls.size());

			assertEquals(1, shuffleMaster.externallyReleasedPartitions.size());
			assertEquals(resultPartitionId2, shuffleMaster.externallyReleasedPartitions.remove());

			assertThat(partitionTracker.isTrackingPartitionsFor(taskExecutorId2), is(false));
		}
	}

	@Test
	public void testStopTrackingIssuesNoReleaseCalls() {
		final TestingShuffleMaster shuffleMaster = new TestingShuffleMaster();

		final Queue<Tuple3<ResourceID, JobID, Collection<ResultPartitionID>>> taskExecutorReleaseCalls = new ArrayBlockingQueue<>(4);
		final PartitionTracker partitionTracker = new PartitionTrackerImpl(
			new JobID(),
			new TestingShuffleMaster(),
			resourceId -> Optional.of(createTaskExecutorGateway(resourceId, taskExecutorReleaseCalls))
		);

		final ResourceID taskExecutorId1 = ResourceID.generate();
		final ResultPartitionID resultPartitionId1 = new ResultPartitionID();

		partitionTracker.startTrackingPartition(
			taskExecutorId1,
			createResultPartitionDeploymentDescriptor(resultPartitionId1, true));

		partitionTracker.stopTrackingPartitionsFor(taskExecutorId1);

		assertEquals(0, taskExecutorReleaseCalls.size());
		assertEquals(0, shuffleMaster.externallyReleasedPartitions.size());
	}

	private static ResultPartitionDeploymentDescriptor createResultPartitionDeploymentDescriptor(
			ResultPartitionID resultPartitionId,
			boolean hasLocalResources) {
		return createResultPartitionDeploymentDescriptor(resultPartitionId, ResultPartitionType.BLOCKING, hasLocalResources);
	}

	private static ResultPartitionDeploymentDescriptor createResultPartitionDeploymentDescriptor(
		ResultPartitionID resultPartitionId,
		ResultPartitionType type,
		boolean hasLocalResources) {

		return new ResultPartitionDeploymentDescriptor(
			new PartitionDescriptor(
				new IntermediateDataSetID(),
				resultPartitionId.getPartitionId(),
				type,
				1,
				0),
			new ShuffleDescriptor() {
				@Override
				public ResultPartitionID getResultPartitionID() {
					return resultPartitionId;
				}

				@Override
				public Optional<ResourceID> storesLocalResourcesOn() {
					return hasLocalResources
						? Optional.of(ResourceID.generate())
						: Optional.empty();
				}
			},
			1,
			true);
	}

	private static TaskExecutorGateway createTaskExecutorGateway(ResourceID taskExecutorId, Collection<Tuple3<ResourceID, JobID, Collection<ResultPartitionID>>> releaseCalls) {
		return new TestingTaskExecutorGatewayBuilder()
			.setReleasePartitionsConsumer((jobId, partitionIds) -> releaseCalls.add(Tuple3.of(taskExecutorId, jobId, partitionIds)))
			.createTestingTaskExecutorGateway();
	}

	private static class TestingShuffleMaster implements ShuffleMaster<ShuffleDescriptor> {

		final Queue<ResultPartitionID> externallyReleasedPartitions = new ArrayBlockingQueue<>(4);

		@Override
		public CompletableFuture<ShuffleDescriptor> registerPartitionWithProducer(PartitionDescriptor partitionDescriptor, ProducerDescriptor producerDescriptor) {
			return null;
		}

		@Override
		public void releasePartitionExternally(ShuffleDescriptor shuffleDescriptor) {
			externallyReleasedPartitions.add(shuffleDescriptor.getResultPartitionID());
		}
	}

}
