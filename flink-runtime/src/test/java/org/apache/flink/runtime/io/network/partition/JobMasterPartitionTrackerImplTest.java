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
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.shuffle.PartitionDescriptor;
import org.apache.flink.runtime.shuffle.ProducerDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava31.com.google.common.collect.Iterables;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
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
                is(resultPartitionType.isReleaseByScheduler()));
    }

    @Test
    public void testReleaseCallsWithLocalResources() {
        final TestingShuffleMaster shuffleMaster = new TestingShuffleMaster();
        final JobID jobId = new JobID();

        final Queue<ReleaseCall> releaseCall = new ArrayBlockingQueue<>(4);
        final JobMasterPartitionTracker partitionTracker =
                new JobMasterPartitionTrackerImpl(
                        jobId,
                        shuffleMaster,
                        tmId ->
                                Optional.of(
                                        createTaskExecutorGateway(
                                                tmId, releaseCall, new ArrayBlockingQueue<>(4))));

        final ResourceID tmId = ResourceID.generate();
        final ResultPartitionID resultPartitionId = new ResultPartitionID();

        partitionTracker.startTrackingPartition(
                tmId,
                AbstractPartitionTrackerTest.createResultPartitionDeploymentDescriptor(
                        resultPartitionId, true));

        assertThat(partitionTracker.isTrackingPartitionsFor(tmId), is(true));

        partitionTracker.stopTrackingAndReleasePartitions(Arrays.asList(resultPartitionId));

        assertEquals(1, releaseCall.size());
        ReleaseCall releaseOrPromoteCall = releaseCall.remove();
        assertEquals(tmId, releaseOrPromoteCall.getTaskExecutorId());
        assertEquals(jobId, releaseOrPromoteCall.getJobId());
        assertThat(releaseOrPromoteCall.getReleasedPartitions(), contains(resultPartitionId));
        assertEquals(1, shuffleMaster.externallyReleasedPartitions.size());
        assertEquals(resultPartitionId, shuffleMaster.externallyReleasedPartitions.remove());
        assertThat(partitionTracker.isTrackingPartitionsFor(tmId), is(false));
    }

    @Test
    public void testReleaseCallsWithoutLocalResources() {
        final TestingShuffleMaster shuffleMaster = new TestingShuffleMaster();

        final Queue<ReleaseCall> releaseCalls = new ArrayBlockingQueue<>(4);
        final Queue<PromoteCall> promoteCalls = new ArrayBlockingQueue<>(4);
        final JobMasterPartitionTracker partitionTracker =
                new JobMasterPartitionTrackerImpl(
                        new JobID(),
                        shuffleMaster,
                        tmId ->
                                Optional.of(
                                        createTaskExecutorGateway(
                                                tmId, releaseCalls, promoteCalls)));

        final ResourceID tmId = ResourceID.generate();
        final ResultPartitionID resultPartitionId = new ResultPartitionID();

        partitionTracker.startTrackingPartition(
                tmId,
                AbstractPartitionTrackerTest.createResultPartitionDeploymentDescriptor(
                        resultPartitionId, false));
        assertThat(partitionTracker.isTrackingPartitionsFor(tmId), is(false));

        partitionTracker.stopTrackingAndReleasePartitions(Arrays.asList(resultPartitionId));

        assertEquals(0, releaseCalls.size());
        assertEquals(0, promoteCalls.size());
        assertEquals(1, shuffleMaster.externallyReleasedPartitions.size());
        assertThat(shuffleMaster.externallyReleasedPartitions, contains(resultPartitionId));
    }

    @Test
    public void testStopTrackingIssuesNoReleaseCalls() {
        final TestingShuffleMaster shuffleMaster = new TestingShuffleMaster();

        final Queue<ReleaseCall> releaseCalls = new ArrayBlockingQueue<>(4);
        final Queue<PromoteCall> promoteCalls = new ArrayBlockingQueue<>(4);
        final JobMasterPartitionTrackerImpl partitionTracker =
                new JobMasterPartitionTrackerImpl(
                        new JobID(),
                        shuffleMaster,
                        resourceId ->
                                Optional.of(
                                        createTaskExecutorGateway(
                                                resourceId, releaseCalls, promoteCalls)));

        final ResourceID taskExecutorId1 = ResourceID.generate();
        final ResultPartitionID resultPartitionId1 = new ResultPartitionID();

        partitionTracker.startTrackingPartition(
                taskExecutorId1,
                AbstractPartitionTrackerTest.createResultPartitionDeploymentDescriptor(
                        resultPartitionId1, true));

        partitionTracker.stopTrackingPartitionsFor(taskExecutorId1);

        assertEquals(0, releaseCalls.size());
        assertEquals(0, promoteCalls.size());
        assertEquals(0, shuffleMaster.externallyReleasedPartitions.size());
    }

    @Test
    public void testTrackingInternalAndExternalPartitionsByTmId() {
        final TestingShuffleMaster shuffleMaster = new TestingShuffleMaster();

        final Queue<ReleaseCall> releaseCalls = new ArrayBlockingQueue<>(4);
        final Queue<PromoteCall> promoteCalls = new ArrayBlockingQueue<>(4);
        final JobMasterPartitionTrackerImpl partitionTracker =
                new JobMasterPartitionTrackerImpl(
                        new JobID(),
                        shuffleMaster,
                        resourceId ->
                                Optional.of(
                                        createTaskExecutorGateway(
                                                resourceId, releaseCalls, promoteCalls)));

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
    public void testGetJobPartitionClusterPartition() {
        final TestingShuffleMaster shuffleMaster = new TestingShuffleMaster();

        final Queue<ReleaseCall> releaseCalls = new ArrayBlockingQueue<>(4);
        final Queue<PromoteCall> promoteCalls = new ArrayBlockingQueue<>(4);
        final JobMasterPartitionTrackerImpl partitionTracker =
                new JobMasterPartitionTrackerImpl(
                        new JobID(),
                        shuffleMaster,
                        resourceId ->
                                Optional.of(
                                        createTaskExecutorGateway(
                                                resourceId, releaseCalls, promoteCalls)));

        final ResourceID taskExecutorId = ResourceID.generate();
        final ResultPartitionID resultPartitionId1 = new ResultPartitionID();
        final ResultPartitionID resultPartitionId2 = new ResultPartitionID();

        final ResultPartitionDeploymentDescriptor clusterPartition =
                AbstractPartitionTrackerTest.createResultPartitionDeploymentDescriptor(
                        resultPartitionId1, ResultPartitionType.BLOCKING_PERSISTENT, false);
        final ResultPartitionDeploymentDescriptor jobPartition =
                AbstractPartitionTrackerTest.createResultPartitionDeploymentDescriptor(
                        resultPartitionId2, false);
        partitionTracker.startTrackingPartition(taskExecutorId, clusterPartition);
        partitionTracker.startTrackingPartition(taskExecutorId, jobPartition);

        Assertions.assertThat(partitionTracker.getAllTrackedNonClusterPartitions())
                .containsExactly(jobPartition);
        Assertions.assertThat(partitionTracker.getAllTrackedClusterPartitions())
                .containsExactly(clusterPartition);
    }

    @Test
    public void testGetShuffleDescriptors() {
        final TestingShuffleMaster shuffleMaster = new TestingShuffleMaster();
        IntermediateDataSetID intermediateDataSetId = new IntermediateDataSetID();

        final Queue<ReleaseCall> releaseCalls = new ArrayBlockingQueue<>(4);
        final Queue<PromoteCall> promoteCalls = new ArrayBlockingQueue<>(4);
        final JobMasterPartitionTrackerImpl partitionTracker =
                new JobMasterPartitionTrackerImpl(
                        new JobID(),
                        shuffleMaster,
                        resourceId ->
                                Optional.of(
                                        createTaskExecutorGateway(
                                                resourceId, releaseCalls, promoteCalls)));

        TestingResourceManagerGateway resourceManagerGateway = new TestingResourceManagerGateway();
        partitionTracker.connectToResourceManager(resourceManagerGateway);
        partitionTracker.getClusterPartitionShuffleDescriptors(intermediateDataSetId);

        assertThat(
                resourceManagerGateway.requestedIntermediateDataSetIds,
                contains(intermediateDataSetId));
    }

    @Test(expected = NullPointerException.class)
    public void testGetShuffleDescriptorsBeforeConnectToResourceManager() {
        final TestingShuffleMaster shuffleMaster = new TestingShuffleMaster();
        IntermediateDataSetID intermediateDataSetId = new IntermediateDataSetID();

        final Queue<ReleaseCall> releaseCalls = new ArrayBlockingQueue<>(4);
        final Queue<PromoteCall> promoteCalls = new ArrayBlockingQueue<>(4);
        final JobMasterPartitionTrackerImpl partitionTracker =
                new JobMasterPartitionTrackerImpl(
                        new JobID(),
                        shuffleMaster,
                        resourceId ->
                                Optional.of(
                                        createTaskExecutorGateway(
                                                resourceId, releaseCalls, promoteCalls)));
        partitionTracker.getClusterPartitionShuffleDescriptors(intermediateDataSetId);
    }

    @Test
    public void testReleaseJobPartitionPromoteClusterPartition() {
        final TestingShuffleMaster shuffleMaster = new TestingShuffleMaster();

        final Queue<ReleaseCall> taskExecutorReleaseCalls = new ArrayBlockingQueue<>(4);
        final Queue<PromoteCall> taskExecutorPromoteCalls = new ArrayBlockingQueue<>(4);
        final JobMasterPartitionTracker partitionTracker =
                new JobMasterPartitionTrackerImpl(
                        new JobID(),
                        shuffleMaster,
                        resourceId ->
                                Optional.of(
                                        createTaskExecutorGateway(
                                                resourceId,
                                                taskExecutorReleaseCalls,
                                                taskExecutorPromoteCalls)));

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

        partitionTracker.stopTrackingAndReleasePartitions(
                Arrays.asList(jobPartitionId0, jobPartitionId1));
        partitionTracker.stopTrackingAndPromotePartitions(
                Arrays.asList(clusterPartitionId0, clusterPartitionId1));

        // Exactly one call should have been made to the hosting task executor
        assertEquals(1, taskExecutorReleaseCalls.size());
        assertEquals(1, taskExecutorPromoteCalls.size());

        final ReleaseCall releaseCall = taskExecutorReleaseCalls.remove();

        final PromoteCall promoteCall = taskExecutorPromoteCalls.remove();

        // One local partition released and one local partition promoted.
        assertEquals(
                jobPartitionId0, Iterables.getOnlyElement(releaseCall.getReleasedPartitions()));
        assertEquals(
                clusterPartitionId0, Iterables.getOnlyElement(promoteCall.getPromotedPartitions()));

        // Both internal and external partitions will be fed into shuffle-master for releasing.
        Collection<ResultPartitionID> externallyReleasedPartitions =
                new ArrayList<>(shuffleMaster.externallyReleasedPartitions);
        assertThat(
                externallyReleasedPartitions, containsInAnyOrder(jobPartitionId0, jobPartitionId1));
    }

    private static TaskExecutorGateway createTaskExecutorGateway(
            ResourceID taskExecutorId,
            Collection<ReleaseCall> releaseCalls,
            Collection<PromoteCall> promoteCalls) {
        return new TestingTaskExecutorGatewayBuilder()
                .setReleasePartitionsConsumer(
                        (jobId, partitions) ->
                                releaseCalls.add(
                                        new ReleaseCall(taskExecutorId, jobId, partitions)))
                .setPromotePartitionsConsumer(
                        (jobId, partitions) ->
                                promoteCalls.add(
                                        new PromoteCall(taskExecutorId, jobId, partitions)))
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

    private static class TestingResourceManagerGateway
            extends org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway {

        private final List<IntermediateDataSetID> requestedIntermediateDataSetIds =
                new ArrayList<>();

        @Override
        public CompletableFuture<List<ShuffleDescriptor>> getClusterPartitionsShuffleDescriptors(
                IntermediateDataSetID intermediateDataSetID) {
            requestedIntermediateDataSetIds.add(intermediateDataSetID);
            return CompletableFuture.completedFuture(Collections.emptyList());
        }
    }

    private static class ReleaseCall {
        private final ResourceID taskExecutorId;
        private final JobID jobId;
        private final Collection<ResultPartitionID> releasedPartitions;

        private ReleaseCall(
                ResourceID taskExecutorId,
                JobID jobId,
                Collection<ResultPartitionID> releasedPartitions) {
            this.taskExecutorId = taskExecutorId;
            this.jobId = jobId;
            this.releasedPartitions = releasedPartitions;
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
    }

    private static class PromoteCall {
        private final ResourceID taskExecutorId;
        private final JobID jobId;
        private final Collection<ResultPartitionID> promotedPartitions;

        private PromoteCall(
                ResourceID taskExecutorId,
                JobID jobId,
                Collection<ResultPartitionID> promotedPartitions) {
            this.taskExecutorId = taskExecutorId;
            this.jobId = jobId;
            this.promotedPartitions = promotedPartitions;
        }

        public ResourceID getTaskExecutorId() {
            return taskExecutorId;
        }

        public JobID getJobId() {
            return jobId;
        }

        public Collection<ResultPartitionID> getPromotedPartitions() {
            return promotedPartitions;
        }
    }
}
