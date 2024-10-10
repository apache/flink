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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
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
import org.apache.flink.runtime.scheduler.adaptive.AdaptiveSchedulerClusterITCase;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

import static java.nio.file.StandardOpenOption.CREATE;
import static org.apache.flink.runtime.entrypoint.component.FileJobGraphRetriever.JOB_GRAPH_FILE_PATH;

/** An integration test which recovers from checkpoint after regaining the leadership. */
@ExtendWith(TestLoggerExtension.class)
public class JobDispatcherITCase {
    @RegisterExtension
    static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

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
    public void testRecoverFromCheckpointAfterLosingAndRegainingLeadership(@TempDir Path tmpPath)
            throws Exception {
        final Configuration configuration = new Configuration();
        configuration.set(HighAvailabilityOptions.HA_MODE, HighAvailabilityMode.ZOOKEEPER.name());
        final TestingMiniClusterConfiguration clusterConfiguration =
                TestingMiniClusterConfiguration.newBuilder()
                        .setConfiguration(configuration)
                        .build();
        final EmbeddedHaServicesWithLeadershipControl haServices =
                new EmbeddedHaServicesWithLeadershipControl(EXECUTOR_RESOURCE.getExecutor());

        final Configuration newConfiguration =
                new Configuration(clusterConfiguration.getConfiguration());
        final long checkpointInterval = 100;
        final JobID jobID =
                generateAndPersistJobGraph(newConfiguration, checkpointInterval, tmpPath);

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
            // make sure requestJobResult was processed by job master
            cluster.getJobStatus(jobID).get();

            haServices.revokeDispatcherLeadership();
            // make sure the leadership is revoked to avoid race conditions
            Assertions.assertEquals(
                    ApplicationStatus.UNKNOWN, firstJobResult.get().getApplicationStatus());

            haServices.grantDispatcherLeadership();

            // job is suspended, wait until it's running
            awaitJobStatus(cluster, jobID, JobStatus.RUNNING);

            CommonTestUtils.waitUntilCondition(
                    () ->
                            cluster.getArchivedExecutionGraph(jobID)
                                            .get()
                                            .getCheckpointStatsSnapshot()
                                            .getLatestRestoredCheckpoint()
                                    != null);
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

    private static void awaitJobStatus(MiniCluster cluster, JobID jobId, JobStatus status)
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
                });
    }

    /**
     * An invokable that supports checkpointing and counts down when there is at least one
     * checkpoint.
     */
    public static class AtLeastOneCheckpointInvokable
            extends AdaptiveSchedulerClusterITCase.CheckpointingNoOpInvokable {

        private static volatile CountDownLatch atLeastOneCheckpointCompleted;

        private static void reset() {
            atLeastOneCheckpointCompleted = new CountDownLatch(1);
        }

        public AtLeastOneCheckpointInvokable(Environment environment) {
            super(environment);
        }

        @Override
        public Future<Void> notifyCheckpointCompleteAsync(long checkpointId) {
            atLeastOneCheckpointCompleted.countDown();
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public Future<Void> notifyCheckpointAbortAsync(
                long checkpointId, long latestCompletedCheckpointId) {
            return CompletableFuture.completedFuture(null);
        }
    }
}
