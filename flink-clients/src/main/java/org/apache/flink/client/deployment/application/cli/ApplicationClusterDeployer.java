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

package org.apache.flink.client.deployment.application.cli;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.cli.ApplicationDeployer;
import org.apache.flink.client.cli.ExecutionConfigAccessor;
import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.client.deployment.ClusterClientJobClientAdapter;
import org.apache.flink.client.deployment.ClusterClientServiceLoader;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.ShutdownHookUtil;
import org.apache.flink.util.function.FunctionWithException;
import org.apache.flink.util.function.SupplierWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.configuration.DeploymentOptions.SHUTDOWN_ON_APPLICATION_FINISH;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An entity responsible for submitting an application for execution in "Application Mode", i.e. on
 * a dedicated cluster that is created on application submission and torn down upon application
 * termination, and with its {@code main()} executed on the cluster, rather than the client.
 */
@Internal
public class ApplicationClusterDeployer implements ApplicationDeployer {

    private static final Logger LOG = LoggerFactory.getLogger(ApplicationClusterDeployer.class);

    private final ClusterClientServiceLoader clientServiceLoader;

    public ApplicationClusterDeployer(final ClusterClientServiceLoader clientServiceLoader) {
        this.clientServiceLoader = checkNotNull(clientServiceLoader);
    }

    public <ClusterID> void run(
            final Configuration configuration,
            final ApplicationConfiguration applicationConfiguration)
            throws Exception {
        checkNotNull(configuration);
        checkNotNull(applicationConfiguration);

        LOG.info("Submitting application in 'Application Mode'.");

        final ClusterClientFactory<ClusterID> clientFactory =
                clientServiceLoader.getClusterClientFactory(configuration);
        try (final ClusterDescriptor<ClusterID> clusterDescriptor =
                clientFactory.createClusterDescriptor(configuration)) {
            final ClusterSpecification clusterSpecification =
                    clientFactory.getClusterSpecification(configuration);

            final ExecutionConfigAccessor configAccessor =
                    ExecutionConfigAccessor.fromConfiguration(configuration);

            if (!configAccessor.getDetachedMode()) {
                configuration.setBoolean(SHUTDOWN_ON_APPLICATION_FINISH, false);
            }

            ClusterClientProvider<ClusterID> clusterClientProvider =
                    clusterDescriptor.deployApplicationCluster(
                            clusterSpecification,
                            applicationConfiguration,
                            configAccessor.getDetachedMode());

            if (configAccessor.getDetachedMode()) {
                LOG.debug(
                        "The client is in detached mode and will exit directly once application is accepted.");
                return;
            }

            Thread shutdownHook =
                    ShutdownHookUtil.addShutdownHook(
                            () -> {
                                clusterClientProvider.getClusterClient().shutDownCluster();
                            },
                            ApplicationClusterDeployer.class.getSimpleName(),
                            LOG);

            JobID jobID =
                    clusterClientProvider.getClusterClient().listJobs().get().stream()
                            .findFirst()
                            .get()
                            .getJobId();

            ClusterClientJobClientAdapter adapter =
                    new ClusterClientJobClientAdapter<>(clusterClientProvider, jobID);

            waitUtilTargetStatusReached(
                    jobID,
                    () -> (JobStatus) adapter.getJobStatus().get(),
                    JobStatus::isGloballyTerminalState);

            LOG.info("Shutting down the cluster.");
            clusterClientProvider.getClusterClient().shutDownCluster();
            LOG.info("Flink cluster has been shut down.");

            ShutdownHookUtil.removeShutdownHook(
                    shutdownHook, ApplicationClusterDeployer.class.getSimpleName(), LOG);
        }
    }

    @VisibleForTesting
    public static void waitUtilTargetStatusReached(
            JobID jobID,
            SupplierWithException<JobStatus, Exception> jobStatusSupplier,
            FunctionWithException<JobStatus, Boolean, Exception> isInTargetStatusFunction)
            throws JobExecutionException {
        LOG.debug("Wait until target status reached.");
        try {
            JobStatus status = jobStatusSupplier.get();
            while (!isInTargetStatusFunction.apply(status)) {
                Thread.sleep(1000);
                status = jobStatusSupplier.get();
            }
            if (status == JobStatus.FAILED) {
                throw new JobExecutionException(
                        jobID,
                        "Errors on job execution, more details could be found on JobManager log.");
            }
        } catch (JobExecutionException executionException) {
            throw executionException;
        } catch (Throwable throwable) {
            ExceptionUtils.checkInterrupted(throwable);
            throw new RuntimeException(
                    "Error while waiting for job to reach target status", throwable);
        }
    }
}
