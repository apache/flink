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

package org.apache.flink.runtime.minicluster;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobmanager.Tasks.AgnosticBinaryReceiver;
import org.apache.flink.runtime.jobmanager.Tasks.AgnosticReceiver;
import org.apache.flink.runtime.jobmanager.Tasks.AgnosticTertiaryReceiver;
import org.apache.flink.runtime.jobmanager.Tasks.ExceptionReceiver;
import org.apache.flink.runtime.jobmanager.Tasks.ExceptionSender;
import org.apache.flink.runtime.jobmanager.Tasks.Forwarder;
import org.apache.flink.runtime.jobmanager.Tasks.InstantiationErrorSender;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.jobmaster.TestingAbstractInvokables.Receiver;
import org.apache.flink.runtime.jobmaster.TestingAbstractInvokables.Sender;
import org.apache.flink.runtime.testtasks.BlockingNoOpInvokable;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.testtasks.WaitingNoOpInvokable;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.util.ExceptionUtils.findThrowable;
import static org.apache.flink.util.ExceptionUtils.findThrowableWithMessage;
import static org.hamcrest.core.StringStartsWith.startsWith;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Integration test cases for the {@link MiniCluster}. */
public class MiniClusterITCase extends TestLogger {

    @Test
    public void runJobWithSingleRpcService() throws Exception {
        final int numOfTMs = 3;
        final int slotsPerTM = 7;

        final MiniClusterConfiguration cfg =
                new MiniClusterConfiguration.Builder()
                        .setNumTaskManagers(numOfTMs)
                        .setNumSlotsPerTaskManager(slotsPerTM)
                        .setRpcServiceSharing(RpcServiceSharing.SHARED)
                        .setConfiguration(getDefaultConfiguration())
                        .build();

        try (final MiniCluster miniCluster = new MiniCluster(cfg)) {
            miniCluster.start();

            miniCluster.executeJobBlocking(getSimpleJob(numOfTMs * slotsPerTM));
        }
    }

    @Test
    public void runJobWithMultipleRpcServices() throws Exception {
        final int numOfTMs = 3;
        final int slotsPerTM = 7;

        final MiniClusterConfiguration cfg =
                new MiniClusterConfiguration.Builder()
                        .setNumTaskManagers(numOfTMs)
                        .setNumSlotsPerTaskManager(slotsPerTM)
                        .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
                        .setConfiguration(getDefaultConfiguration())
                        .build();

        try (final MiniCluster miniCluster = new MiniCluster(cfg)) {
            miniCluster.start();

            miniCluster.executeJobBlocking(getSimpleJob(numOfTMs * slotsPerTM));
        }
    }

    @Test
    public void testHandleStreamingJobsWhenNotEnoughSlot() throws Exception {
        try {
            setupAndRunHandleJobsWhenNotEnoughSlots(ScheduleMode.EAGER);
            fail("Job should fail.");
        } catch (JobExecutionException e) {
            assertTrue(findThrowableWithMessage(e, "Job execution failed.").isPresent());
            assertTrue(findThrowable(e, NoResourceAvailableException.class).isPresent());
        }
    }

    @Test
    public void testHandleBatchJobsWhenNotEnoughSlot() throws Exception {
        try {
            setupAndRunHandleJobsWhenNotEnoughSlots(ScheduleMode.LAZY_FROM_SOURCES);
            fail("Job should fail.");
        } catch (JobExecutionException e) {
            assertTrue(findThrowableWithMessage(e, "Job execution failed.").isPresent());
            assertTrue(findThrowable(e, NoResourceAvailableException.class).isPresent());
        }
    }

    private void setupAndRunHandleJobsWhenNotEnoughSlots(ScheduleMode scheduleMode)
            throws Exception {
        final JobVertex vertex = new JobVertex("Test Vertex");
        vertex.setParallelism(2);
        vertex.setMaxParallelism(2);
        vertex.setInvokableClass(BlockingNoOpInvokable.class);

        final JobGraph jobGraph = new JobGraph("Test Job", vertex);
        jobGraph.setScheduleMode(scheduleMode);

        runHandleJobsWhenNotEnoughSlots(jobGraph);
    }

    private void runHandleJobsWhenNotEnoughSlots(final JobGraph jobGraph) throws Exception {
        final Configuration configuration = getDefaultConfiguration();
        configuration.setLong(JobManagerOptions.SLOT_REQUEST_TIMEOUT, 100L);

        final MiniClusterConfiguration cfg =
                new MiniClusterConfiguration.Builder()
                        .setNumTaskManagers(1)
                        .setNumSlotsPerTaskManager(1)
                        .setConfiguration(configuration)
                        .build();

        try (final MiniCluster miniCluster = new MiniCluster(cfg)) {
            miniCluster.start();

            miniCluster.executeJobBlocking(jobGraph);
        }
    }

    @Test
    public void testForwardJob() throws Exception {
        final int parallelism = 31;

        final MiniClusterConfiguration cfg =
                new MiniClusterConfiguration.Builder()
                        .setNumTaskManagers(1)
                        .setNumSlotsPerTaskManager(parallelism)
                        .setConfiguration(getDefaultConfiguration())
                        .build();

        try (final MiniCluster miniCluster = new MiniCluster(cfg)) {
            miniCluster.start();

            final JobVertex sender = new JobVertex("Sender");
            sender.setInvokableClass(Sender.class);
            sender.setParallelism(parallelism);

            final JobVertex receiver = new JobVertex("Receiver");
            receiver.setInvokableClass(Receiver.class);
            receiver.setParallelism(parallelism);

            receiver.connectNewDataSetAsInput(
                    sender, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

            final JobGraph jobGraph = new JobGraph("Pointwise Job", sender, receiver);

            miniCluster.executeJobBlocking(jobGraph);
        }
    }

    @Test
    public void testBipartiteJob() throws Exception {
        final int parallelism = 31;

        final MiniClusterConfiguration cfg =
                new MiniClusterConfiguration.Builder()
                        .setNumTaskManagers(1)
                        .setNumSlotsPerTaskManager(parallelism)
                        .setConfiguration(getDefaultConfiguration())
                        .build();

        try (final MiniCluster miniCluster = new MiniCluster(cfg)) {
            miniCluster.start();

            final JobVertex sender = new JobVertex("Sender");
            sender.setInvokableClass(Sender.class);
            sender.setParallelism(parallelism);

            final JobVertex receiver = new JobVertex("Receiver");
            receiver.setInvokableClass(AgnosticReceiver.class);
            receiver.setParallelism(parallelism);

            receiver.connectNewDataSetAsInput(
                    sender, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

            final JobGraph jobGraph = new JobGraph("Bipartite Job", sender, receiver);

            miniCluster.executeJobBlocking(jobGraph);
        }
    }

    @Test
    public void testTwoInputJobFailingEdgeMismatch() throws Exception {
        final int parallelism = 1;

        final MiniClusterConfiguration cfg =
                new MiniClusterConfiguration.Builder()
                        .setNumTaskManagers(1)
                        .setNumSlotsPerTaskManager(6 * parallelism)
                        .setConfiguration(getDefaultConfiguration())
                        .build();

        try (final MiniCluster miniCluster = new MiniCluster(cfg)) {
            miniCluster.start();

            final JobVertex sender1 = new JobVertex("Sender1");
            sender1.setInvokableClass(Sender.class);
            sender1.setParallelism(parallelism);

            final JobVertex sender2 = new JobVertex("Sender2");
            sender2.setInvokableClass(Sender.class);
            sender2.setParallelism(2 * parallelism);

            final JobVertex receiver = new JobVertex("Receiver");
            receiver.setInvokableClass(AgnosticTertiaryReceiver.class);
            receiver.setParallelism(3 * parallelism);

            receiver.connectNewDataSetAsInput(
                    sender1, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
            receiver.connectNewDataSetAsInput(
                    sender2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

            final JobGraph jobGraph = new JobGraph("Bipartite Job", sender1, receiver, sender2);

            try {
                miniCluster.executeJobBlocking(jobGraph);

                fail("Job should fail.");
            } catch (JobExecutionException e) {
                assertTrue(findThrowable(e, ArrayIndexOutOfBoundsException.class).isPresent());
                assertTrue(findThrowableWithMessage(e, "2").isPresent());
            }
        }
    }

    @Test
    public void testTwoInputJob() throws Exception {
        final int parallelism = 11;

        final MiniClusterConfiguration cfg =
                new MiniClusterConfiguration.Builder()
                        .setNumTaskManagers(1)
                        .setNumSlotsPerTaskManager(6 * parallelism)
                        .setConfiguration(getDefaultConfiguration())
                        .build();

        try (final MiniCluster miniCluster = new MiniCluster(cfg)) {
            miniCluster.start();

            final JobVertex sender1 = new JobVertex("Sender1");
            sender1.setInvokableClass(Sender.class);
            sender1.setParallelism(parallelism);

            final JobVertex sender2 = new JobVertex("Sender2");
            sender2.setInvokableClass(Sender.class);
            sender2.setParallelism(2 * parallelism);

            final JobVertex receiver = new JobVertex("Receiver");
            receiver.setInvokableClass(AgnosticBinaryReceiver.class);
            receiver.setParallelism(3 * parallelism);

            receiver.connectNewDataSetAsInput(
                    sender1, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
            receiver.connectNewDataSetAsInput(
                    sender2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

            final JobGraph jobGraph = new JobGraph("Bipartite Job", sender1, receiver, sender2);

            miniCluster.executeJobBlocking(jobGraph);
        }
    }

    @Test
    public void testSchedulingAllAtOnce() throws Exception {
        final int parallelism = 11;

        final MiniClusterConfiguration cfg =
                new MiniClusterConfiguration.Builder()
                        .setNumTaskManagers(1)
                        .setNumSlotsPerTaskManager(parallelism)
                        .setConfiguration(getDefaultConfiguration())
                        .build();

        try (final MiniCluster miniCluster = new MiniCluster(cfg)) {
            miniCluster.start();

            final JobVertex sender = new JobVertex("Sender");
            sender.setInvokableClass(Sender.class);
            sender.setParallelism(parallelism);

            final JobVertex forwarder = new JobVertex("Forwarder");
            forwarder.setInvokableClass(Forwarder.class);
            forwarder.setParallelism(parallelism);

            final JobVertex receiver = new JobVertex("Receiver");
            receiver.setInvokableClass(AgnosticReceiver.class);
            receiver.setParallelism(parallelism);

            final SlotSharingGroup sharingGroup = new SlotSharingGroup();
            sender.setSlotSharingGroup(sharingGroup);
            forwarder.setSlotSharingGroup(sharingGroup);
            receiver.setSlotSharingGroup(sharingGroup);

            forwarder.connectNewDataSetAsInput(
                    sender, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
            receiver.connectNewDataSetAsInput(
                    forwarder, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

            final JobGraph jobGraph = new JobGraph("Forwarding Job", sender, forwarder, receiver);

            jobGraph.setScheduleMode(ScheduleMode.EAGER);

            miniCluster.executeJobBlocking(jobGraph);
        }
    }

    @Test
    public void testJobWithAFailingSenderVertex() throws Exception {
        final int parallelism = 11;

        final MiniClusterConfiguration cfg =
                new MiniClusterConfiguration.Builder()
                        .setNumTaskManagers(1)
                        .setNumSlotsPerTaskManager(parallelism)
                        .setConfiguration(getDefaultConfiguration())
                        .build();

        try (final MiniCluster miniCluster = new MiniCluster(cfg)) {
            miniCluster.start();

            final JobVertex sender = new JobVertex("Sender");
            sender.setInvokableClass(ExceptionSender.class);
            sender.setParallelism(parallelism);

            final JobVertex receiver = new JobVertex("Receiver");
            receiver.setInvokableClass(Receiver.class);
            receiver.setParallelism(parallelism);

            receiver.connectNewDataSetAsInput(
                    sender, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

            final JobGraph jobGraph = new JobGraph("Pointwise Job", sender, receiver);

            try {
                miniCluster.executeJobBlocking(jobGraph);

                fail("Job should fail.");
            } catch (JobExecutionException e) {
                assertTrue(findThrowable(e, Exception.class).isPresent());
                assertTrue(findThrowableWithMessage(e, "Test exception").isPresent());
            }
        }
    }

    @Test
    public void testJobWithAnOccasionallyFailingSenderVertex() throws Exception {
        final int parallelism = 11;

        final MiniClusterConfiguration cfg =
                new MiniClusterConfiguration.Builder()
                        .setNumTaskManagers(1)
                        .setNumSlotsPerTaskManager(parallelism)
                        .setConfiguration(getDefaultConfiguration())
                        .build();

        try (final MiniCluster miniCluster = new MiniCluster(cfg)) {
            miniCluster.start();

            // putting sender and receiver vertex in the same slot sharing group is required
            // to ensure all senders can be deployed. Otherwise this case can fail if the
            // expected failing sender is not deployed.
            final SlotSharingGroup group = new SlotSharingGroup();

            final JobVertex sender = new JobVertex("Sender");
            sender.setInvokableClass(SometimesExceptionSender.class);
            sender.setParallelism(parallelism);
            sender.setSlotSharingGroup(group);

            // set failing senders
            SometimesExceptionSender.configFailingSenders(parallelism);

            final JobVertex receiver = new JobVertex("Receiver");
            receiver.setInvokableClass(Receiver.class);
            receiver.setParallelism(parallelism);
            receiver.setSlotSharingGroup(group);

            receiver.connectNewDataSetAsInput(
                    sender, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

            final JobGraph jobGraph = new JobGraph("Pointwise Job", sender, receiver);

            try {
                miniCluster.executeJobBlocking(jobGraph);

                fail("Job should fail.");
            } catch (JobExecutionException e) {
                assertTrue(findThrowable(e, Exception.class).isPresent());
                assertTrue(findThrowableWithMessage(e, "Test exception").isPresent());
            }
        }
    }

    @Test
    public void testJobWithAFailingReceiverVertex() throws Exception {
        final int parallelism = 11;

        final MiniClusterConfiguration cfg =
                new MiniClusterConfiguration.Builder()
                        .setNumTaskManagers(1)
                        .setNumSlotsPerTaskManager(parallelism)
                        .setConfiguration(getDefaultConfiguration())
                        .build();

        try (final MiniCluster miniCluster = new MiniCluster(cfg)) {
            miniCluster.start();

            final JobVertex sender = new JobVertex("Sender");
            sender.setInvokableClass(Sender.class);
            sender.setParallelism(parallelism);

            final JobVertex receiver = new JobVertex("Receiver");
            receiver.setInvokableClass(ExceptionReceiver.class);
            receiver.setParallelism(parallelism);

            receiver.connectNewDataSetAsInput(
                    sender, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

            final JobGraph jobGraph = new JobGraph("Pointwise Job", sender, receiver);

            try {
                miniCluster.executeJobBlocking(jobGraph);

                fail("Job should fail.");
            } catch (JobExecutionException e) {
                assertTrue(findThrowable(e, Exception.class).isPresent());
                assertTrue(findThrowableWithMessage(e, "Test exception").isPresent());
            }
        }
    }

    @Test
    public void testJobWithAllVerticesFailingDuringInstantiation() throws Exception {
        final int parallelism = 11;

        final MiniClusterConfiguration cfg =
                new MiniClusterConfiguration.Builder()
                        .setNumTaskManagers(1)
                        .setNumSlotsPerTaskManager(parallelism)
                        .setConfiguration(getDefaultConfiguration())
                        .build();

        try (final MiniCluster miniCluster = new MiniCluster(cfg)) {
            miniCluster.start();

            final JobVertex sender = new JobVertex("Sender");
            sender.setInvokableClass(InstantiationErrorSender.class);
            sender.setParallelism(parallelism);

            final JobVertex receiver = new JobVertex("Receiver");
            receiver.setInvokableClass(Receiver.class);
            receiver.setParallelism(parallelism);

            receiver.connectNewDataSetAsInput(
                    sender, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

            final JobGraph jobGraph = new JobGraph("Pointwise Job", sender, receiver);

            try {
                miniCluster.executeJobBlocking(jobGraph);

                fail("Job should fail.");
            } catch (JobExecutionException e) {
                assertTrue(findThrowable(e, Exception.class).isPresent());
                assertTrue(
                        findThrowableWithMessage(e, "Test exception in constructor").isPresent());
            }
        }
    }

    @Test
    public void testJobWithSomeVerticesFailingDuringInstantiation() throws Exception {
        final int parallelism = 11;

        final MiniClusterConfiguration cfg =
                new MiniClusterConfiguration.Builder()
                        .setNumTaskManagers(1)
                        .setNumSlotsPerTaskManager(parallelism)
                        .setConfiguration(getDefaultConfiguration())
                        .build();

        try (final MiniCluster miniCluster = new MiniCluster(cfg)) {
            miniCluster.start();

            // putting sender and receiver vertex in the same slot sharing group is required
            // to ensure all senders can be deployed. Otherwise this case can fail if the
            // expected failing sender is not deployed.
            final SlotSharingGroup group = new SlotSharingGroup();

            final JobVertex sender = new JobVertex("Sender");
            sender.setInvokableClass(SometimesInstantiationErrorSender.class);
            sender.setParallelism(parallelism);
            sender.setSlotSharingGroup(group);

            // set failing senders
            SometimesInstantiationErrorSender.configFailingSenders(parallelism);

            final JobVertex receiver = new JobVertex("Receiver");
            receiver.setInvokableClass(Receiver.class);
            receiver.setParallelism(parallelism);
            receiver.setSlotSharingGroup(group);

            receiver.connectNewDataSetAsInput(
                    sender, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

            final JobGraph jobGraph = new JobGraph("Pointwise Job", sender, receiver);

            try {
                miniCluster.executeJobBlocking(jobGraph);

                fail("Job should fail.");
            } catch (JobExecutionException e) {
                assertTrue(findThrowable(e, Exception.class).isPresent());
                assertTrue(
                        findThrowableWithMessage(e, "Test exception in constructor").isPresent());
            }
        }
    }

    @Test
    public void testCallFinalizeOnMasterBeforeJobCompletes() throws Exception {
        final int parallelism = 11;

        final MiniClusterConfiguration cfg =
                new MiniClusterConfiguration.Builder()
                        .setNumTaskManagers(1)
                        .setNumSlotsPerTaskManager(parallelism)
                        .setConfiguration(getDefaultConfiguration())
                        .build();

        try (final MiniCluster miniCluster = new MiniCluster(cfg)) {
            miniCluster.start();

            final JobVertex source = new JobVertex("Source");
            source.setInvokableClass(WaitingNoOpInvokable.class);
            source.setParallelism(parallelism);

            final WaitOnFinalizeJobVertex sink = new WaitOnFinalizeJobVertex("Sink", 20L);
            sink.setInvokableClass(NoOpInvokable.class);
            sink.setParallelism(parallelism);

            sink.connectNewDataSetAsInput(
                    source, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

            final JobGraph jobGraph =
                    new JobGraph("SubtaskInFinalStateRaceCondition", source, sink);

            final CompletableFuture<JobSubmissionResult> submissionFuture =
                    miniCluster.submitJob(jobGraph);

            final CompletableFuture<JobResult> jobResultFuture =
                    submissionFuture.thenCompose(
                            (JobSubmissionResult ignored) ->
                                    miniCluster.requestJobResult(jobGraph.getJobID()));

            jobResultFuture.get().toJobExecutionResult(getClass().getClassLoader());

            assertTrue(sink.finalizedOnMaster.get());
        }
    }

    @Test
    public void testOutOfMemoryErrorMessageEnrichmentInJobVertexFinalization() throws Exception {
        final int parallelism = 1;

        final MiniClusterConfiguration cfg =
                new MiniClusterConfiguration.Builder()
                        .setNumTaskManagers(1)
                        .setNumSlotsPerTaskManager(parallelism)
                        .setConfiguration(getDefaultConfiguration())
                        .build();

        try (final MiniCluster miniCluster = new MiniCluster(cfg)) {
            miniCluster.start();

            final JobVertex failingJobVertex =
                    new JobVertex("FailingInFinalization") {

                        @Override
                        public void finalizeOnMaster(ClassLoader loader) {
                            throw new OutOfMemoryError("Java heap space");
                        }
                    };
            failingJobVertex.setInvokableClass(NoOpInvokable.class);
            failingJobVertex.setParallelism(parallelism);

            final JobGraph jobGraph =
                    new JobGraph("JobGraphWithFailingJobVertex", failingJobVertex);

            final CompletableFuture<JobSubmissionResult> submissionFuture =
                    miniCluster.submitJob(jobGraph);

            final CompletableFuture<JobResult> jobResultFuture =
                    submissionFuture.thenCompose(
                            (JobSubmissionResult ignored) ->
                                    miniCluster.requestJobResult(jobGraph.getJobID()));

            try {
                jobResultFuture.get().toJobExecutionResult(getClass().getClassLoader());
            } catch (JobExecutionException e) {
                assertTrue(findThrowable(e, OutOfMemoryError.class).isPresent());
                assertThat(
                        findThrowable(e, OutOfMemoryError.class)
                                .map(OutOfMemoryError::getMessage)
                                .get(),
                        startsWith(
                                "Java heap space. A heap space-related out-of-memory error has occurred."));
            }
        }
    }

    @Test
    public void testOutOfMemoryErrorMessageEnrichmentInJobVertexInitialization() throws Exception {
        final int parallelism = 1;

        final MiniClusterConfiguration cfg =
                new MiniClusterConfiguration.Builder()
                        .setNumTaskManagers(1)
                        .setNumSlotsPerTaskManager(parallelism)
                        .setConfiguration(getDefaultConfiguration())
                        .build();

        try (final MiniCluster miniCluster = new MiniCluster(cfg)) {
            miniCluster.start();

            final JobVertex failingJobVertex =
                    new JobVertex("FailingInFinalization") {

                        @Override
                        public void initializeOnMaster(ClassLoader loader) {
                            throw new OutOfMemoryError("Java heap space");
                        }
                    };
            failingJobVertex.setInvokableClass(NoOpInvokable.class);
            failingJobVertex.setParallelism(parallelism);

            final JobGraph jobGraph =
                    new JobGraph("JobGraphWithFailingJobVertex", failingJobVertex);

            final CompletableFuture<JobSubmissionResult> submissionFuture =
                    miniCluster.submitJob(jobGraph);

            final CompletableFuture<JobResult> jobResultFuture =
                    submissionFuture.thenCompose(
                            (JobSubmissionResult ignored) ->
                                    miniCluster.requestJobResult(jobGraph.getJobID()));

            try {
                jobResultFuture.get();
            } catch (ExecutionException e) {
                assertTrue(findThrowable(e, OutOfMemoryError.class).isPresent());
                assertThat(
                        findThrowable(e, OutOfMemoryError.class)
                                .map(OutOfMemoryError::getMessage)
                                .get(),
                        startsWith(
                                "Java heap space. A heap space-related out-of-memory error has occurred."));
            }
        }
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    private Configuration getDefaultConfiguration() {
        final Configuration configuration = new Configuration();
        configuration.setString(RestOptions.BIND_PORT, "0");

        return configuration;
    }

    private static JobGraph getSimpleJob(int parallelism) throws IOException {
        final JobVertex task = new JobVertex("Test task");
        task.setParallelism(parallelism);
        task.setMaxParallelism(parallelism);
        task.setInvokableClass(NoOpInvokable.class);

        final JobGraph jg = new JobGraph(new JobID(), "Test Job", task);
        jg.setScheduleMode(ScheduleMode.EAGER);

        final ExecutionConfig executionConfig = new ExecutionConfig();
        executionConfig.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 1000));
        jg.setExecutionConfig(executionConfig);

        return jg;
    }

    private static class WaitOnFinalizeJobVertex extends JobVertex {

        private static final long serialVersionUID = -1179547322468530299L;

        private final AtomicBoolean finalizedOnMaster = new AtomicBoolean(false);

        private final long waitingTime;

        WaitOnFinalizeJobVertex(String name, long waitingTime) {
            super(name);

            this.waitingTime = waitingTime;
        }

        @Override
        public void finalizeOnMaster(ClassLoader loader) throws Exception {
            Thread.sleep(waitingTime);
            finalizedOnMaster.set(true);
        }
    }
}
