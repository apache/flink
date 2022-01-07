/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.dispatcher.runner.DefaultDispatcherRunnerFactory;
import org.apache.flink.runtime.dispatcher.runner.JobDispatcherLeaderProcessFactoryFactory;
import org.apache.flink.runtime.entrypoint.component.DefaultDispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.entrypoint.component.FileJobGraphRetriever;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.highavailability.nonha.embedded.EmbeddedHaServicesWithLeadershipControl;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphBuilder;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.TestingMiniCluster;
import org.apache.flink.runtime.minicluster.TestingMiniClusterConfiguration;
import org.apache.flink.runtime.resourcemanager.StandaloneResourceManagerFactory;
import org.apache.flink.runtime.rest.JobRestEndpointFactory;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.TestingUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.FutureUtils;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import static java.nio.file.StandardOpenOption.CREATE;
import static org.apache.flink.runtime.entrypoint.component.FileJobGraphRetriever.JOB_GRAPH_FILE_PATH;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/** An integration test which recovers from checkpoint after regaining the leadership. */
public class JobDispatcherITCase extends TestLogger {

    private static final Duration TIMEOUT = Duration.ofMinutes(10);

    @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    private Supplier<DispatcherResourceManagerComponentFactory>
            createJobModeDispatcherResourceManagerComponentFactorySupplier(
                    Configuration configuration) {
        return () -> {
            try {
                return new DefaultDispatcherResourceManagerComponentFactory(
                        new DefaultDispatcherRunnerFactory(
                                JobDispatcherLeaderProcessFactoryFactory.create(
                                        FileJobGraphRetriever.createFrom(configuration, null))),
                        StandaloneResourceManagerFactory.getInstance(),
                        JobRestEndpointFactory.INSTANCE);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }

    @Test
    public void testRecoverFromCheckpointAfterLosingAndRegainingLeadership() throws Exception {
        final Deadline deadline = Deadline.fromNow(TIMEOUT);
        final Configuration configuration = new Configuration();
        configuration.set(HighAvailabilityOptions.HA_MODE, HighAvailabilityMode.ZOOKEEPER.name());
        final TestingMiniClusterConfiguration clusterConfiguration =
                TestingMiniClusterConfiguration.newBuilder()
                        .setConfiguration(configuration)
                        .build();
        final EmbeddedHaServicesWithLeadershipControl haServices =
                new EmbeddedHaServicesWithLeadershipControl(TestingUtils.defaultExecutor());

        final Configuration newConfiguration =
                new Configuration(clusterConfiguration.getConfiguration());
        final long checkpointInterval = 100;
        final JobID jobID =
                generateAndPersistJobGraph(
                        newConfiguration,
                        checkpointInterval,
                        TEMPORARY_FOLDER.newFolder().toPath());

        final TestingMiniCluster.Builder clusterBuilder =
                TestingMiniCluster.newBuilder(clusterConfiguration)
                        .setHighAvailabilityServicesSupplier(() -> haServices)
                        .setDispatcherResourceManagerComponentFactorySupplier(
                                createJobModeDispatcherResourceManagerComponentFactorySupplier(
                                        newConfiguration));
        AtLeastOneCheckpointInvokable.reset();

        try (final MiniCluster cluster = clusterBuilder.build()) {
            // start mini cluster and submit the job
            cluster.start();

            AtLeastOneCheckpointInvokable.atLeastOneCheckpointCompleted.await();

            final CompletableFuture<JobResult> firstJobResult = cluster.requestJobResult(jobID);
            haServices.revokeDispatcherLeadership();
            // make sure the leadership is revoked to avoid race conditions
            assertEquals(ApplicationStatus.UNKNOWN, firstJobResult.get().getApplicationStatus());

            haServices.grantDispatcherLeadership();

            // job is suspended, wait until it's running
            awaitJobStatus(cluster, jobID, JobStatus.RUNNING, deadline);

            assertNotNull(
                    cluster.getArchivedExecutionGraph(jobID)
                            .get()
                            .getCheckpointStatsSnapshot()
                            .getLatestRestoredCheckpoint());
        }
    }

    private JobID generateAndPersistJobGraph(
            Configuration configuration, long checkpointInterval, Path tmpPath) throws Exception {
        final JobVertex jobVertex = new JobVertex("jobVertex");
        jobVertex.setInvokableClass(AtLeastOneCheckpointInvokable.class);
        jobVertex.setParallelism(1);

        final CheckpointCoordinatorConfiguration checkpointCoordinatorConfiguration =
                CheckpointCoordinatorConfiguration.builder()
                        .setCheckpointInterval(checkpointInterval)
                        .build();
        final JobCheckpointingSettings checkpointingSettings =
                new JobCheckpointingSettings(checkpointCoordinatorConfiguration, null);
        final JobGraph jobGraph =
                JobGraphBuilder.newStreamingJobGraphBuilder()
                        .addJobVertex(jobVertex)
                        .setJobCheckpointingSettings(checkpointingSettings)
                        .build();

        final Path jobGraphPath = tmpPath.resolve(JOB_GRAPH_FILE_PATH.defaultValue());
        try (ObjectOutputStream objectOutputStream =
                new ObjectOutputStream(Files.newOutputStream(jobGraphPath, CREATE))) {
            objectOutputStream.writeObject(jobGraph);
        }
        configuration.setString(JOB_GRAPH_FILE_PATH.key(), jobGraphPath.toString());
        return jobGraph.getJobID();
    }

    private static void awaitJobStatus(
            MiniCluster cluster, JobID jobId, JobStatus status, Deadline deadline)
            throws Exception {
        CommonTestUtils.waitUntilCondition(
                () -> {
                    try {
                        return cluster.getJobStatus(jobId).get() == status;
                    } catch (ExecutionException e) {
                        if (ExceptionUtils.findThrowable(e, FlinkJobNotFoundException.class)
                                .isPresent()) {
                            // job may not be yet submitted
                            return false;
                        }
                        throw e;
                    }
                },
                deadline);
    }

    /**
     * An invokable that supports checkpointing and counts down when there is at least one
     * checkpoint.
     */
    public static class AtLeastOneCheckpointInvokable extends AbstractInvokable {

        private static final long CANCEL_SIGNAL = -2L;
        private final BlockingQueue<Long> checkpointsToConfirm = new ArrayBlockingQueue<>(1);

        private static volatile CountDownLatch atLeastOneCheckpointCompleted;

        private static void reset() {
            atLeastOneCheckpointCompleted = new CountDownLatch(1);
        }

        public AtLeastOneCheckpointInvokable(Environment environment) {
            super(environment);
        }

        @Override
        public void invoke() throws Exception {
            long signal = checkpointsToConfirm.take();
            while (signal != CANCEL_SIGNAL) {
                getEnvironment().acknowledgeCheckpoint(signal, new CheckpointMetrics());
                signal = checkpointsToConfirm.take();
            }
        }

        @Override
        public Future<Void> cancel() throws Exception {
            checkpointsToConfirm.add(CANCEL_SIGNAL);
            return FutureUtils.completedVoidFuture();
        }

        @Override
        public CompletableFuture<Boolean> triggerCheckpointAsync(
                CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions) {
            checkpointsToConfirm.add(checkpointMetaData.getCheckpointId());
            return CompletableFuture.completedFuture(true);
        }

        @Override
        public Future<Void> notifyCheckpointCompleteAsync(long checkpointId) {
            atLeastOneCheckpointCompleted.countDown();
            return CompletableFuture.completedFuture(null);
        }
    }
}
