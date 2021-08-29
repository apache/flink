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

import org.apache.flink.shaded.guava30.com.google.common.collect.Iterables;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;

/** Tests for the {@link JobMasterPartitionTrackerImpl}. */
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
        final JobMasterPartitionTracker partitionTracker =
                new JobMasterPartitionTrackerImpl(
                        new JobID(), new TestingShuffleMaster(), ignored -> Optional.empty());

        final ResourceID resourceId = ResourceID.generate();
        final ResultPartitionID resultPartitionId = new ResultPartitionID();
        partitionTracker.startTrackingPartition(
                resourceId,
                AbstractPartitionTrackerTest.createResultPartitionDeploymentDescriptor(
                        resultPartitionId, resultPartitionType, true));

        assertThat(
                partitionTracker.isTrackingPartitionsFor(resourceId),
                is(resultPartitionType.isReconnectable()));
    }

    @Test
    public void testReleaseCallsWithLocalResources() {
        final TestingShuffleMaster shuffleMaster = new TestingShuffleMaster();
        final JobID jobId = new JobID();

        final Queue<ReleaseCall> releaseCalls = new ArrayBlockingQueue<>(4);
        final JobMasterPartitionTracker partitionTracker =
                new JobMasterPartitionTrackerImpl(
                        jobId,
                        shuffleMaster,
                        tmId -> Optional.of(createTaskExecutorGateway(tmId, releaseCalls)));

        final ResourceID tmId = ResourceID.generate();
        final ResultPartitionID resultPartitionId = new ResultPartitionID();

        partitionTracker.startTrackingPartition(
                tmId,
                AbstractPartitionTrackerTest.createResultPartitionDeploymentDescriptor(
                        resultPartitionId, true));

        assertThat(partitionTracker.isTrackingPartitionsFor(tmId), is(true));

        partitionTracker.stopTrackingAndReleasePartitions(Arrays.asList(resultPartitionId));

        assertEquals(1, releaseCalls.size());
        ReleaseCall releaseCall = releaseCalls.remove();
        assertEquals(tmId, releaseCall.getTaskExecutorId());
        assertEquals(jobId, releaseCall.getJobId());
        assertThat(releaseCall.getReleasedPartitions(), contains(resultPartitionId));
        assertThat(releaseCall.getPromotedPartitions(), is(empty()));
        assertEquals(1, shuffleMaster.externallyReleasedPartitions.size());
        assertEquals(resultPartitionId, shuffleMaster.externallyReleasedPartitions.remove());
        assertThat(partitionTracker.isTrackingPartitionsFor(tmId), is(false));
    }

    @Test
    public void testReleaseCallsWithoutLocalResources() {
        final TestingShuffleMaster shuffleMaster = new TestingShuffleMaster();

        final Queue<ReleaseCall> releaseCalls = new ArrayBlockingQueue<>(4);
        final JobMasterPartitionTracker partitionTracker =
                new JobMasterPartitionTrackerImpl(
                        new JobID(),
                        shuffleMaster,
                        tmId -> Optional.of(createTaskExecutorGateway(tmId, releaseCalls)));

        final ResourceID tmId = ResourceID.generate();
        final ResultPartitionID resultPartitionId = new ResultPartitionID();

        partitionTracker.startTrackingPartition(
                tmId,
                AbstractPartitionTrackerTest.createResultPartitionDeploymentDescriptor(
                        resultPartitionId, false));
        assertThat(partitionTracker.isTrackingPartitionsFor(tmId), is(false));

        partitionTracker.stopTrackingAndReleasePartitions(Arrays.asList(resultPartitionId));

        assertEquals(0, releaseCalls.size());
        assertEquals(1, shuffleMaster.externallyReleasedPartitions.size());
        assertThat(shuffleMaster.externallyReleasedPartitions, contains(resultPartitionId));
    }

    @Test
    public void testStopTrackingIssuesNoReleaseCalls() {
        final TestingShuffleMaster shuffleMaster = new TestingShuffleMaster();

        final Queue<ReleaseCall> taskExecutorReleaseCalls = new ArrayBlockingQueue<>(4);
        final JobMasterPartitionTrackerImpl partitionTracker =
                new JobMasterPartitionTrackerImpl(
                        new JobID(),
                        shuffleMaster,
                        resourceId ->
                                Optional.of(
                                        createTaskExecutorGateway(
                                                resourceId, taskExecutorReleaseCalls)));

        final ResourceID taskExecutorId1 = ResourceID.generate();
        final ResultPartitionID resultPartitionId1 = new ResultPartitionID();

        partitionTracker.startTrackingPartition(
                taskExecutorId1,
                AbstractPartitionTrackerTest.createResultPartitionDeploymentDescriptor(
                        resultPartitionId1, true));

        partitionTracker.stopTrackingPartitionsFor(taskExecutorId1);

        assertEquals(0, taskExecutorReleaseCalls.size());
        assertEquals(0, shuffleMaster.externallyReleasedPartitions.size());
    }

    @Test
    public void testTrackingInternalAndExternalPartitionsByTmId() {
        final TestingShuffleMaster shuffleMaster = new TestingShuffleMaster();

        final Queue<ReleaseCall> taskExecutorReleaseCalls = new ArrayBlockingQueue<>(4);
        final JobMasterPartitionTrackerImpl partitionTracker =
                new JobMasterPartitionTrackerImpl(
                        new JobID(),
                        shuffleMaster,
                        resourceId ->
                                Optional.of(
                                        createTaskExecutorGateway(
                                                resourceId, taskExecutorReleaseCalls)));

        final ResourceID taskExecutorId = ResourceID.generate();
        final ResultPartitionID resultPartitionId1 = new ResultPartitionID();
        final ResultPartitionID resultPartitionId2 = new ResultPartitionID();

        partitionTracker.startTrackingPartition(
                taskExecutorId,
                AbstractPartitionTrackerTest.createResultPartitionDeploymentDescriptor(
                        resultPartitionId2, false));
        // No local resource is occupied
        assertThat(partitionTracker.isTrackingPartitionsFor(taskExecutorId), is(false));

        partitionTracker.startTrackingPartition(
                taskExecutorId,
                AbstractPartitionTrackerTest.createResultPartitionDeploymentDescriptor(
                        resultPartitionId1, true));
        // Local resource is occupied
        assertThat(partitionTracker.isTrackingPartitionsFor(taskExecutorId), is(true));

        assertThat(
                partitionTracker.getAllTrackedPartitions().stream()
                        .map(desc -> desc.getShuffleDescriptor().getResultPartitionID())
                        .collect(Collectors.toList()),
                containsInAnyOrder(resultPartitionId1, resultPartitionId2));

        partitionTracker.stopTrackingPartitionsFor(taskExecutorId);

        assertThat(partitionTracker.isTrackingPartitionsFor(taskExecutorId), is(false));
        assertThat(partitionTracker.isPartitionTracked(resultPartitionId1), is(false));
        assertThat(partitionTracker.isPartitionTracked(resultPartitionId2), is(true));
        assertThat(
                Iterables.getOnlyElement(partitionTracker.getAllTrackedPartitions())
                        .getShuffleDescriptor()
                        .getResultPartitionID(),
                is(resultPartitionId2));
    }

    @Test
    public void testReleaseOrPromote() {
        final TestingShuffleMaster shuffleMaster = new TestingShuffleMaster();

        final Queue<ReleaseCall> taskExecutorReleaseOrPromoteCalls = new ArrayBlockingQueue<>(4);
        final JobMasterPartitionTracker partitionTracker =
                new JobMasterPartitionTrackerImpl(
                        new JobID(),
                        shuffleMaster,
                        resourceId ->
                                Optional.of(
                                        createTaskExecutorGateway(
                                                resourceId, taskExecutorReleaseOrPromoteCalls)));

        final ResourceID taskExecutorId1 = ResourceID.generate();
        final ResultPartitionID jobPartitionId0 = new ResultPartitionID();
        final ResultPartitionID jobPartitionId1 = new ResultPartitionID();
        final ResultPartitionID clusterPartitionId0 = new ResultPartitionID();
        final ResultPartitionID clusterPartitionId1 = new ResultPartitionID();

        // Any partition type that is not BLOCKING_PERSISTENT denotes a job partition;
        // A local job partition (occupies tm local resource)
        final ResultPartitionDeploymentDescriptor jobPartition0 =
                AbstractPartitionTrackerTest.createResultPartitionDeploymentDescriptor(
                        jobPartitionId0, ResultPartitionType.BLOCKING, true);
        partitionTracker.startTrackingPartition(taskExecutorId1, jobPartition0);

        // An external job partition (accommodated by external shuffle service)
        final ResultPartitionDeploymentDescriptor jobPartition1 =
                AbstractPartitionTrackerTest.createResultPartitionDeploymentDescriptor(
                        jobPartitionId1, ResultPartitionType.BLOCKING, false);
        partitionTracker.startTrackingPartition(taskExecutorId1, jobPartition1);

        // BLOCKING_PERSISTENT denotes a cluster partition
        // An local cluster partition
        final ResultPartitionDeploymentDescriptor clusterPartition0 =
                AbstractPartitionTrackerTest.createResultPartitionDeploymentDescriptor(
                        clusterPartitionId0, ResultPartitionType.BLOCKING_PERSISTENT, true);
        partitionTracker.startTrackingPartition(taskExecutorId1, clusterPartition0);

        // An external cluster partition
        final ResultPartitionDeploymentDescriptor clusterPartition1 =
                AbstractPartitionTrackerTest.createResultPartitionDeploymentDescriptor(
                        clusterPartitionId1, ResultPartitionType.BLOCKING_PERSISTENT, false);
        partitionTracker.startTrackingPartition(taskExecutorId1, clusterPartition1);

        partitionTracker.stopTrackingAndReleaseOrPromotePartitions(
                Arrays.asList(
                        jobPartitionId0,
                        jobPartitionId1,
                        clusterPartitionId0,
                        clusterPartitionId1));

        // Exactly one call should have been made to the hosting task executor
        assertEquals(1, taskExecutorReleaseOrPromoteCalls.size());

        final ReleaseCall taskExecutorReleaseOrPromoteCall =
                taskExecutorReleaseOrPromoteCalls.remove();

        // One local partition released and one local partition promoted.
        assertEquals(
                jobPartitionId0,
                Iterables.getOnlyElement(taskExecutorReleaseOrPromoteCall.getReleasedPartitions()));
        assertEquals(
                clusterPartitionId0,
                Iterables.getOnlyElement(taskExecutorReleaseOrPromoteCall.getPromotedPartitions()));

        // Both internal and external partitions will be fed into shuffle-master for releasing.
        Collection<ResultPartitionID> externallyReleasedPartitions =
                new ArrayList<>(shuffleMaster.externallyReleasedPartitions);
        assertThat(
                externallyReleasedPartitions, containsInAnyOrder(jobPartitionId0, jobPartitionId1));
    }

    private static TaskExecutorGateway createTaskExecutorGateway(
            ResourceID taskExecutorId, Collection<ReleaseCall> releaseOrPromoteCalls) {
        return new TestingTaskExecutorGatewayBuilder()
                .setReleaseOrPromotePartitionsConsumer(
                        (jobId, partitionsToRelease, partitionsToPromote) ->
                                releaseOrPromoteCalls.add(
                                        new ReleaseCall(
                                                taskExecutorId,
                                                jobId,
                                                partitionsToRelease,
                                                partitionsToPromote)))
                .createTestingTaskExecutorGateway();
    }

    private static class TestingShuffleMaster implements ShuffleMaster<ShuffleDescriptor> {

        final Queue<ResultPartitionID> externallyReleasedPartitions = new ArrayBlockingQueue<>(4);

        @Override
        public CompletableFuture<ShuffleDescriptor> registerPartitionWithProducer(
                JobID jobID,
                PartitionDescriptor partitionDescriptor,
                ProducerDescriptor producerDescriptor) {
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

        private ReleaseCall(
                ResourceID taskExecutorId,
                JobID jobId,
                Collection<ResultPartitionID> releasedPartitions,
                Collection<ResultPartitionID> promotedPartitions) {
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
