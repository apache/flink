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

package org.apache.flink.tests.util.flink.container;

import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.rest.messages.ClusterOverviewHeaders;
import org.apache.flink.util.NetUtils;

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.images.builder.ImageFromDockerfile;

import java.io.IOException;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collection;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * JobManager container based on Testcontainers.
 *
 * <p>Here we extends {@link FixedHostPortGenericContainer} because the exposed port on host should
 * keep consistent after restarting the JobManager under some HA related test cases, otherwise the
 * cluster client might lose connection with the restarted JobManager
 */
class JobManagerContainer extends FixedHostPortGenericContainer<JobManagerContainer> {

    private static final Logger LOG = LoggerFactory.getLogger(JobManagerContainer.class);

    // Default timeout of operations
    public static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);

    private RestClusterClient<StandaloneClusterId> restClusterClient;
    private final Configuration configuration;
    private NetUtils.Port restPortOnHost;

    JobManagerContainer(ImageFromDockerfile jobManagerImage, Configuration configuration) {
        super(jobManagerImage.get());
        this.configuration = configuration;
    }

    /** Get {@link RestClusterClient} binding with this JobManager. */
    public RestClusterClient<StandaloneClusterId> getRestClusterClient() {
        checkNotNull(
                restClusterClient,
                "REST client is not initialized because the JobManager container is not started");
        return restClusterClient;
    }

    @Override
    public void start() {
        // Assign random port for JobManager
        if (restPortOnHost == null) {
            restPortOnHost = NetUtils.getAvailablePort();
        }
        withFixedExposedPort(restPortOnHost.getPort(), configuration.get(RestOptions.PORT));
        super.start();
        waitUntilJobManagerRESTReachable();
        createClusterClient();
        LOG.info(
                "JobManager is running with REST address {}",
                getHost() + ":" + getMappedPort(configuration.get(RestOptions.PORT)));
    }

    @Override
    public void stop() {
        deleteJobManagerTemporaryFiles();
        super.stop();
        LOG.info("Stopping JobManager container");
        if (restClusterClient != null) {
            restClusterClient.close();
        }
        if (restPortOnHost != null) {
            try {
                restPortOnHost.close();
            } catch (Exception e) {
                throw new RuntimeException("Failed to close JobManager port", e);
            }
        }
    }

    /** Restart JobManager container. */
    public void restart() {
        LOG.info("Restarting JobManager container");
        // We need to keep temporary files for job recovery, so we use GenericContainer#stop instead
        // of JobManager#stop()
        super.stop();
        super.start();
        waitUntilJobManagerRESTReachable();
    }

    private void createClusterClient() {
        checkState(isRunning(), "JobManager should be running for creating a REST client");
        final Configuration clientConfiguration = new Configuration();
        clientConfiguration.set(RestOptions.ADDRESS, getHost());
        clientConfiguration.set(
                RestOptions.PORT, getMappedPort(configuration.get(RestOptions.PORT)));
        try {
            restClusterClient =
                    new RestClusterClient<>(clientConfiguration, StandaloneClusterId.getInstance());
        } catch (Exception e) {
            throw new RuntimeException("Failed to construct REST cluster client", e);
        }
    }

    private void waitUntilJobManagerRESTReachable() {
        LOG.debug("Waiting for JobManager's REST interface getting ready");
        new HttpWaitStrategy()
                .forPort(configuration.get(RestOptions.PORT))
                // Specify URL here because using root path will be redirected to Flink Web UI,
                // which might not be built by using "-Pskip-webui-build"
                .forPath(ClusterOverviewHeaders.URL)
                .forStatusCode(200)
                .withReadTimeout(DEFAULT_TIMEOUT)
                .waitUntilReady(this);
    }

    private void deleteJobManagerTemporaryFiles() {
        final String checkpointDir = configuration.get(CheckpointingOptions.CHECKPOINTS_DIRECTORY);
        final String haDir = configuration.get(HighAvailabilityOptions.HA_STORAGE_PATH);
        final Collection<String> usedPaths =
                Lists.newArrayList(checkpointDir, haDir).stream()
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
        if (usedPaths.isEmpty()) {
            return;
        }
        final StringBuilder deletionBaseCommand = new StringBuilder("rm -rf");
        usedPaths.forEach(p -> deletionBaseCommand.append(formatFilePathForDeletion(p)));
        final String[] command = {"bash", "-c", deletionBaseCommand.toString()};
        final Container.ExecResult result;
        try {
            result = execInContainer(command);
            if (result.getExitCode() != 0) {
                throw new IllegalStateException(
                        String.format(
                                "Command \"%s\" returned non-zero exit code %d. \nSTDOUT: %s\nSTDERR: %s",
                                String.join(" ", command),
                                result.getExitCode(),
                                result.getStdout(),
                                result.getStderr()));
            }
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to delete temporary files generated by the flink cluster.", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(
                    "Failed to delete temporary files generated by the flink cluster.", e);
        }
    }

    private String formatFilePathForDeletion(String path) {
        return " " + Paths.get(path).toString().split("file:")[1] + "/*";
    }
}
