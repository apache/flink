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

package org.apache.flink.client.deployment.application;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.client.cli.ClientOptions;
import org.apache.flink.client.deployment.application.executors.EmbeddedExecutor;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.testjar.BlockingJob;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.dispatcher.SessionDispatcherFactory;
import org.apache.flink.runtime.dispatcher.runner.DefaultDispatcherRunnerFactory;
import org.apache.flink.runtime.entrypoint.component.DefaultDispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.highavailability.nonha.embedded.EmbeddedHaServicesWithLeadershipControl;
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

import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

/** Integration tests related to {@link ApplicationDispatcherBootstrap}. */
public class ApplicationDispatcherBootstrapITCase extends TestLogger {

    private static final Duration TIMEOUT = Duration.ofMinutes(10);

    private static Supplier<DispatcherResourceManagerComponentFactory>
            createApplicationModeDispatcherResourceManagerComponentFactorySupplier(
                    Configuration configuration, PackagedProgram program) {
        return () -> {
            final ApplicationDispatcherLeaderProcessFactoryFactory
                    applicationDispatcherLeaderProcessFactoryFactory =
                            ApplicationDispatcherLeaderProcessFactoryFactory.create(
                                    new Configuration(configuration),
                                    SessionDispatcherFactory.INSTANCE,
                                    program);
            return new DefaultDispatcherResourceManagerComponentFactory(
                    new DefaultDispatcherRunnerFactory(
                            applicationDispatcherLeaderProcessFactoryFactory),
                    StandaloneResourceManagerFactory.getInstance(),
                    JobRestEndpointFactory.INSTANCE);
        };
    }

    @Test
    public void testDispatcherRecoversAfterLosingAndRegainingLeadership() throws Exception {
        final String blockId = UUID.randomUUID().toString();
        final Deadline deadline = Deadline.fromNow(TIMEOUT);
        final Configuration configuration = new Configuration();
        configuration.set(HighAvailabilityOptions.HA_MODE, HighAvailabilityMode.ZOOKEEPER.name());
        configuration.set(DeploymentOptions.TARGET, EmbeddedExecutor.NAME);
        configuration.set(ClientOptions.CLIENT_RETRY_PERIOD, Duration.ofMillis(100));
        final TestingMiniClusterConfiguration clusterConfiguration =
                TestingMiniClusterConfiguration.newBuilder()
                        .setConfiguration(configuration)
                        .build();
        final EmbeddedHaServicesWithLeadershipControl haServices =
                new EmbeddedHaServicesWithLeadershipControl(TestingUtils.defaultExecutor());
        final TestingMiniCluster.Builder clusterBuilder =
                TestingMiniCluster.newBuilder(clusterConfiguration)
                        .setHighAvailabilityServicesSupplier(() -> haServices)
                        .setDispatcherResourceManagerComponentFactorySupplier(
                                createApplicationModeDispatcherResourceManagerComponentFactorySupplier(
                                        clusterConfiguration.getConfiguration(),
                                        BlockingJob.getProgram(blockId)));
        try (final MiniCluster cluster = clusterBuilder.build()) {

            // start mini cluster and submit the job
            cluster.start();

            // wait until job is running
            awaitJobStatus(
                    cluster,
                    ApplicationDispatcherBootstrap.ZERO_JOB_ID,
                    JobStatus.RUNNING,
                    deadline);

            // make sure the operator is actually running
            BlockingJob.awaitRunning(blockId);

            final CompletableFuture<JobResult> firstJobResult =
                    cluster.requestJobResult(ApplicationDispatcherBootstrap.ZERO_JOB_ID);
            haServices.revokeDispatcherLeadership();
            // make sure the leadership is revoked to avoid race conditions
            Assertions.assertEquals(
                    ApplicationStatus.UNKNOWN, firstJobResult.get().getApplicationStatus());
            haServices.grantDispatcherLeadership();

            // job is suspended, wait until it's running
            awaitJobStatus(
                    cluster,
                    ApplicationDispatcherBootstrap.ZERO_JOB_ID,
                    JobStatus.RUNNING,
                    deadline);

            // unblock processing so the job can finish
            BlockingJob.unblock(blockId);

            // and wait for it to actually finish
            final CompletableFuture<JobResult> secondJobResult =
                    cluster.requestJobResult(ApplicationDispatcherBootstrap.ZERO_JOB_ID);
            Assertions.assertTrue(secondJobResult.get().isSuccess());
            Assertions.assertEquals(
                    ApplicationStatus.SUCCEEDED, secondJobResult.get().getApplicationStatus());

            // the cluster should shut down automatically once the application completes
            awaitClusterStopped(cluster, deadline);
        } finally {
            BlockingJob.cleanUp(blockId);
        }
    }

    private static void awaitClusterStopped(MiniCluster cluster, Deadline deadline)
            throws Exception {
        CommonTestUtils.waitUntilCondition(() -> !cluster.isRunning(), deadline);
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
}
