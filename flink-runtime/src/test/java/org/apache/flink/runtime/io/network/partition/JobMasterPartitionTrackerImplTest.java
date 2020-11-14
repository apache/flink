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
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.shuffle.PartitionDescriptor;
import org.apache.flink.runtime.shuffle.ProducerDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;

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
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;

/**
 * Tests for the {@link JobMasterPartitionTrackerImpl}.
 */
public class JobMasterPartitionTrackerImplTest extends TestLogger {

	@Test
	public void testPipelinedPartitionIsNotTracked() {
		testReleaseOnConsumptionHandling(ResultPartitionType.PIPELINED);
	}

	@Test
	public void testBlockingPartitionIsTracked() {
		testReleaseOnConsumptionHandling(ResultPartitionType.BLOCKING);
	}

	@Test
	public void testPipelinedApproximatePartitionIsTracked() {
		testReleaseOnConsumptionHandling(ResultPartitionType.PIPELINED_APPROXIMATE);
	}

	private static void testReleaseOnConsumptionHandling(ResultPartitionType resultPartitionType) {
		final JobMasterPartitionTracker partitionTracker = new JobMasterPartitionTrackerImpl(
			new JobID(),
			new TestingShuffleMaster(),
			ignored -> Optional.empty()
		);

		final ResourceID resourceId = ResourceID.generate();
		final ResultPartitionID resultPartitionId = new ResultPartitionID();
		partitionTracker.startTrackingPartition(
			resourceId,
			AbstractPartitionTrackerTest.createResultPartitionDeploymentDescriptor(
				resultPartitionId,
				resultPartitionType,
				false));

		assertThat(partitionTracker.isTrackingPartitionsFor(resourceId), is(resultPartitionType.isReconnectable()));
	}

	@Test
	public void testReleaseCallsWithLocalResources() {
		final TestingShuffleMaster shuffleMaster = new TestingShuffleMaster();
		final JobID jobId = new JobID();

		final Queue<ReleaseCall> taskExecutorReleaseCalls = new ArrayBlockingQueue<>(4);
		final JobMasterPartitionTracker partitionTracker = new JobMasterPartitionTrackerImpl(
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
			AbstractPartitionTrackerTest.createResultPartitionDeploymentDescriptor(resultPartitionId1, true));
		partitionTracker.startTrackingPartition(
			taskExecutorId2,
			AbstractPartitionTrackerTest.createResultPartitionDeploymentDescriptor(resultPartitionId2, true));

		{
			partitionTracker.stopTrackingAndReleasePartitionsFor(taskExecutorId1);

			assertEquals(1, taskExecutorReleaseCalls.size());

			ReleaseCall taskExecutorReleaseCall = taskExecutorReleaseCalls.remove();
			assertEquals(taskExecutorId1, taskExecutorReleaseCall.getTaskExecutorId());
			assertEquals(jobId, taskExecutorReleaseCall.getJobId());
			assertThat(taskExecutorReleaseCall.getReleasedPartitions(), contains(resultPartitionId1));
			assertThat(taskExecutorReleaseCall.getPromotedPartitions(), is(empty()));

			assertEquals(1, shuffleMaster.externallyReleasedPartitions.size());
			assertEquals(resultPartitionId1, shuffleMaster.externallyReleasedPartitions.remove());

			assertThat(partitionTracker.isTrackingPartitionsFor(taskExecutorId1), is(false));
		}

		{
			partitionTracker.stopTrackingAndReleasePartitions(Collections.singletonList(resultPartitionId2));

			assertEquals(1, taskExecutorReleaseCalls.size());

			ReleaseCall releaseCall = taskExecutorReleaseCalls.remove();
			assertEquals(taskExecutorId2, releaseCall.getTaskExecutorId());
			assertEquals(jobId, releaseCall.getJobId());
			assertThat(releaseCall.getReleasedPartitions(), contains(resultPartitionId2));
			assertThat(releaseCall.getPromotedPartitions(), is(empty()));

			assertEquals(1, shuffleMaster.externallyReleasedPartitions.size());
			assertEquals(resultPartitionId2, shuffleMaster.externallyReleasedPartitions.remove());

			assertThat(partitionTracker.isTrackingPartitionsFor(taskExecutorId2), is(false));
		}
	}

	@Test
	public void testReleaseCallsWithoutLocalResources() {
		final TestingShuffleMaster shuffleMaster = new TestingShuffleMaster();

		final Queue<ReleaseCall> taskExecutorReleaseCalls = new ArrayBlockingQueue<>(4);
		final JobMasterPartitionTracker partitionTracker = new JobMasterPartitionTrackerImpl(
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
			AbstractPartitionTrackerTest.createResultPartitionDeploymentDescriptor(resultPartitionId1, false));
		partitionTracker.startTrackingPartition(
			taskExecutorId2,
			AbstractPartitionTrackerTest.createResultPartitionDeploymentDescriptor(resultPartitionId2, false));

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

		final Queue<ReleaseCall> taskExecutorReleaseCalls = new ArrayBlockingQueue<>(4);
		final JobMasterPartitionTrackerImpl partitionTracker = new JobMasterPartitionTrackerImpl(
			new JobID(),
			shuffleMaster,
			resourceId -> Optional.of(createTaskExecutorGateway(resourceId, taskExecutorReleaseCalls))
		);

		final ResourceID taskExecutorId1 = ResourceID.generate();
		final ResultPartitionID resultPartitionId1 = new ResultPartitionID();

		partitionTracker.startTrackingPartition(
			taskExecutorId1,
			AbstractPartitionTrackerTest.createResultPartitionDeploymentDescriptor(resultPartitionId1, true));

		partitionTracker.stopTrackingPartitionsFor(taskExecutorId1);

		assertEquals(0, taskExecutorReleaseCalls.size());
		assertEquals(0, shuffleMaster.externallyReleasedPartitions.size());
	}

	@Test
	public void testReleaseOrPromote() {
		final TestingShuffleMaster shuffleMaster = new TestingShuffleMaster();

		final Queue<ReleaseCall> taskExecutorReleaseCalls = new ArrayBlockingQueue<>(4);
		final JobMasterPartitionTracker partitionTracker = new JobMasterPartitionTrackerImpl(
			new JobID(),
			shuffleMaster,
			resourceId -> Optional.of(createTaskExecutorGateway(resourceId, taskExecutorReleaseCalls))
		);

		final ResourceID taskExecutorId1 = ResourceID.generate();
		final ResultPartitionID jobPartitionId = new ResultPartitionID();
		final ResultPartitionID clusterPartitionId = new ResultPartitionID();

		// any partition type that is not BLOCKING_PERSISTENT denotes a job partition
		final ResultPartitionDeploymentDescriptor jobPartition = AbstractPartitionTrackerTest.createResultPartitionDeploymentDescriptor(jobPartitionId, ResultPartitionType.BLOCKING, true);
		partitionTracker.startTrackingPartition(
			taskExecutorId1,
			jobPartition);

		// BLOCKING_PERSISTENT denotes a cluster partition
		final ResultPartitionDeploymentDescriptor clusterPartition = AbstractPartitionTrackerTest.createResultPartitionDeploymentDescriptor(clusterPartitionId, ResultPartitionType.BLOCKING_PERSISTENT, true);
		partitionTracker.startTrackingPartition(
			taskExecutorId1,
			clusterPartition);

		partitionTracker.stopTrackingAndReleaseOrPromotePartitionsFor(taskExecutorId1);

		// exactly one call should have been made to the hosting task executor
		assertEquals(1, taskExecutorReleaseCalls.size());

		// the job partition should have been released on the shuffle master
		assertEquals(1, shuffleMaster.externallyReleasedPartitions.size());
		assertEquals(jobPartitionId, shuffleMaster.externallyReleasedPartitions.remove());

		final ReleaseCall taskExecutorReleaseOrPromoteCall = taskExecutorReleaseCalls.remove();

		// the job partition should be passed as a partition to release
		assertEquals(jobPartitionId, Iterables.getOnlyElement(taskExecutorReleaseOrPromoteCall.getReleasedPartitions()));

		// the cluster partition should be passed as a partition to promote
		assertEquals(clusterPartitionId, Iterables.getOnlyElement(taskExecutorReleaseOrPromoteCall.getPromotedPartitions()));
	}

	private static TaskExecutorGateway createTaskExecutorGateway(ResourceID taskExecutorId, Collection<ReleaseCall> releaseCalls) {
		return new TestingTaskExecutorGatewayBuilder()
			.setReleaseOrPromotePartitionsConsumer((jobId, partitionToRelease, partitionsToPromote) -> releaseCalls.add(new ReleaseCall(taskExecutorId, jobId, partitionToRelease, partitionsToPromote)))
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

	private static class ReleaseCall {
		private final ResourceID taskExecutorId;
		private final JobID jobId;
		private final Collection<ResultPartitionID> releasedPartitions;
		private final Collection<ResultPartitionID> promotedPartitions;

		private ReleaseCall(ResourceID taskExecutorId, JobID jobId, Collection<ResultPartitionID> releasedPartitions, Collection<ResultPartitionID> promotedPartitions) {
			this.taskExecutorId = taskExecutorId;
			this.jobId = jobId;
			this.releasedPartitions = releasedPartitions;
			this.promotedPartitions = promotedPartitions;
		}

		public ResourceID getTaskExecutorId() {
			return taskExecutorId;
		}

		public JobID getJobId() {
			return jobId;
		}

		public Collection<ResultPartitionID> getReleasedPartitions() {
			return releasedPartitions;
		}

		public Collection<ResultPartitionID> getPromotedPartitions() {
			return promotedPartitions;
		}
	}

}
