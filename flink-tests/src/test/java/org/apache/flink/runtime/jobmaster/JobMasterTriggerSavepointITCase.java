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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.MiniClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphBuilder;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.junit5.InjectClusterClient;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.ExceptionUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.configuration.JobManagerOptions.SCHEDULER;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.isOneOf;

/**
 * Tests for {@link org.apache.flink.runtime.jobmaster.JobMaster#triggerSavepoint(String, boolean,
 * Duration)}.
 *
 * @see org.apache.flink.runtime.jobmaster.JobMaster
 */
public class JobMasterTriggerSavepointITCase {

    private static CountDownLatch invokeLatch;

    private static volatile CountDownLatch triggerCheckpointLatch;

    @TempDir protected File temporaryFolder;

    @RegisterExtension
    public static MiniClusterExtension miniClusterResource =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(getConfiguration())
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(4)
                            .build());

    private static Configuration getConfiguration() {
        Configuration configuration = new Configuration();
        configuration.set(SCHEDULER, JobManagerOptions.SchedulerType.Adaptive);
        return configuration;
    }

    private Path savepointDirectory;
    private JobGraph jobGraph;

    private void setUpWithCheckpointInterval(
            long checkpointInterval, ClusterClient<?> clusterClient) throws Exception {
        invokeLatch = new CountDownLatch(1);
        triggerCheckpointLatch = new CountDownLatch(1);
        savepointDirectory = temporaryFolder.toPath();

        Assumptions.assumeTrue(
                clusterClient instanceof MiniClusterClient,
                "ClusterClient is not an instance of MiniClusterClient");

        final JobVertex vertex = new JobVertex("testVertex");
        vertex.setInvokableClass(NoOpBlockingInvokable.class);
        vertex.setParallelism(1);

        final JobCheckpointingSettings jobCheckpointingSettings =
                new JobCheckpointingSettings(
                        new CheckpointCoordinatorConfiguration(
                                checkpointInterval,
                                60_000,
                                10,
                                1,
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
                                true,
                                false,
                                0,
                                0),
                        null);

        jobGraph =
                JobGraphBuilder.newStreamingJobGraphBuilder()
                        .addJobVertex(vertex)
                        .setJobCheckpointingSettings(jobCheckpointingSettings)
                        .build();

        clusterClient.submitJob(jobGraph).get();
        Assertions.assertTrue(invokeLatch.await(60, TimeUnit.SECONDS));
        waitForJob(clusterClient);
    }

    @Test
    public void testStopJobAfterSavepoint(@InjectClusterClient ClusterClient<?> clusterClient)
            throws Exception {
        setUpWithCheckpointInterval(10L, clusterClient);

        final String savepointLocation = cancelWithSavepoint(clusterClient);
        final JobStatus jobStatus = clusterClient.getJobStatus(jobGraph.getJobID()).get();

        assertThat(jobStatus, isOneOf(JobStatus.CANCELED, JobStatus.CANCELLING));

        final List<Path> savepoints;
        try (Stream<Path> savepointFiles = Files.list(savepointDirectory)) {
            savepoints = savepointFiles.map(Path::getFileName).collect(Collectors.toList());
        }
        assertThat(savepoints, hasItem(Paths.get(savepointLocation).getFileName()));
    }

    @Test
    public void testStopJobAfterSavepointWithDeactivatedPeriodicCheckpointing(
            @InjectClusterClient ClusterClient<?> clusterClient) throws Exception {
        // set checkpointInterval to Long.MAX_VALUE, which means deactivated checkpointing
        setUpWithCheckpointInterval(Long.MAX_VALUE, clusterClient);

        final String savepointLocation = cancelWithSavepoint(clusterClient);
        final JobStatus jobStatus =
                clusterClient.getJobStatus(jobGraph.getJobID()).get(60, TimeUnit.SECONDS);

        assertThat(jobStatus, isOneOf(JobStatus.CANCELED, JobStatus.CANCELLING));

        final List<Path> savepoints;
        try (Stream<Path> savepointFiles = Files.list(savepointDirectory)) {
            savepoints = savepointFiles.map(Path::getFileName).collect(Collectors.toList());
        }
        assertThat(savepoints, hasItem(Paths.get(savepointLocation).getFileName()));
    }

    @Test
    @Tag("org.apache.flink.testutils.junit.FailsInGHAContainerWithRootUser")
    public void testDoNotCancelJobIfSavepointFails(
            @InjectClusterClient ClusterClient<?> clusterClient) throws Exception {
        setUpWithCheckpointInterval(10L, clusterClient);

        try {
            Files.setPosixFilePermissions(savepointDirectory, Collections.emptySet());
        } catch (IOException e) {
            Assumptions.assumeTrue(e == null);
        }

        try {
            cancelWithSavepoint(clusterClient);
        } catch (Exception e) {
            assertThat(
                    ExceptionUtils.findThrowable(e, CheckpointException.class).isPresent(),
                    equalTo(true));
        }

        final JobStatus jobStatus =
                clusterClient.getJobStatus(jobGraph.getJobID()).get(60, TimeUnit.SECONDS);
        assertThat(jobStatus, equalTo(JobStatus.RUNNING));

        // assert that checkpoints are continued to be triggered
        triggerCheckpointLatch = new CountDownLatch(1);
        assertThat(triggerCheckpointLatch.await(60L, TimeUnit.SECONDS), equalTo(true));
    }

    /**
     * Tests that cancel with savepoint without a properly configured savepoint directory, will fail
     * with a meaningful exception message.
     */
    @Test
    public void testCancelWithSavepointWithoutConfiguredSavepointDirectory(
            @InjectClusterClient ClusterClient<?> clusterClient) throws Exception {
        setUpWithCheckpointInterval(10L, clusterClient);

        try {
            clusterClient
                    .cancelWithSavepoint(jobGraph.getJobID(), null, SavepointFormatType.CANONICAL)
                    .get();
        } catch (Exception e) {
            if (!ExceptionUtils.findThrowableWithMessage(e, "savepoint directory").isPresent()) {
                throw e;
            }
        }
    }

    private void waitForJob(ClusterClient<?> clusterClient) throws Exception {
        for (int i = 0; i < 60; i++) {
            try {
                final JobStatus jobStatus =
                        clusterClient.getJobStatus(jobGraph.getJobID()).get(60, TimeUnit.SECONDS);
                assertThat(jobStatus.isGloballyTerminalState(), equalTo(false));
                if (jobStatus == JobStatus.RUNNING) {
                    return;
                }
            } catch (ExecutionException ignored) {
                // JobManagerRunner is not yet registered in Dispatcher
            }
            Thread.sleep(1000);
        }
        throw new AssertionError("Job did not become running within timeout.");
    }

    /**
     * Invokable which calls {@link CountDownLatch#countDown()} on {@link
     * JobMasterTriggerSavepointITCase#invokeLatch}, and then blocks afterwards.
     */
    public static class NoOpBlockingInvokable extends AbstractInvokable {

        public NoOpBlockingInvokable(final Environment environment) {
            super(environment);
        }

        @Override
        public void invoke() {
            invokeLatch.countDown();
            try {
                Thread.sleep(Long.MAX_VALUE);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public CompletableFuture<Boolean> triggerCheckpointAsync(
                final CheckpointMetaData checkpointMetaData,
                final CheckpointOptions checkpointOptions) {
            final TaskStateSnapshot checkpointStateHandles = new TaskStateSnapshot();
            checkpointStateHandles.putSubtaskStateByOperatorID(
                    OperatorID.fromJobVertexID(getEnvironment().getJobVertexId()),
                    OperatorSubtaskState.builder().build());

            getEnvironment()
                    .acknowledgeCheckpoint(
                            checkpointMetaData.getCheckpointId(),
                            new CheckpointMetrics(),
                            checkpointStateHandles);

            triggerCheckpointLatch.countDown();

            return CompletableFuture.completedFuture(true);
        }

        @Override
        public Future<Void> notifyCheckpointCompleteAsync(final long checkpointId) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public Future<Void> notifyCheckpointAbortAsync(
                long checkpointId, long latestCompletedCheckpointId) {
            return CompletableFuture.completedFuture(null);
        }
    }

    private String cancelWithSavepoint(ClusterClient<?> clusterClient) throws Exception {
        return clusterClient
                .cancelWithSavepoint(
                        jobGraph.getJobID(),
                        savepointDirectory.toAbsolutePath().toString(),
                        SavepointFormatType.CANONICAL)
                .get();
    }
}
