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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.rest.handler.legacy.messages.ClusterOverviewWithVersion;
import org.apache.flink.runtime.rest.messages.ClusterOverviewHeaders;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.tests.util.flink.SQLJobSubmission;
import org.apache.flink.util.function.RunnableWithException;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.MountableFile;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.util.Preconditions.checkState;

/** A Flink cluster running JM and TM on different containers. */
public class FlinkContainer implements BeforeAllCallback, AfterAllCallback {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkContainer.class);

    // Hostname of components within container network
    public static final String JOB_MANAGER_HOSTNAME = "jobmanager";
    public static final String TASK_MANAGER_HOSTNAME_PREFIX = "taskmanager-";
    public static final String ZOOKEEPER_HOSTNAME = "zookeeper";

    // Directories for storing states
    public static final Path CHECKPOINT_PATH = Paths.get("/flink/checkpoint");
    public static final Path HA_STORAGE_PATH = Paths.get("/flink/recovery");

    private final GenericContainer<?> jobManager;
    private final List<GenericContainer<?>> taskManagers;
    private final GenericContainer<?> haService;
    private final Configuration conf;

    private RestClusterClient<StandaloneClusterId> restClusterClient;
    private boolean isRunning;

    /** Creates a builder for {@link FlinkContainer}. */
    public static FlinkContainerBuilder builder() {
        return new FlinkContainerBuilder();
    }

    FlinkContainer(
            GenericContainer<?> jobManager,
            List<GenericContainer<?>> taskManagers,
            @Nullable GenericContainer<?> haService,
            Configuration conf) {
        this.jobManager = jobManager;
        this.taskManagers = taskManagers;
        this.haService = haService;
        this.conf = conf;
    }

    /** Starts all containers. */
    public void start() throws Exception {
        if (haService != null) {
            LOG.debug("Starting HA service container");
            this.haService.start();
        }
        LOG.debug("Starting JobManager container");
        this.jobManager.start();
        LOG.debug("Starting TaskManager containers");
        this.taskManagers.parallelStream().forEach(GenericContainer::start);
        LOG.debug("Creating REST cluster client");
        this.restClusterClient = createClusterClient();
        waitUntilAllTaskManagerConnected();
        isRunning = true;
    }

    /** Stops all containers. */
    public void stop() {
        isRunning = false;
        if (restClusterClient != null) {
            restClusterClient.close();
        }
        this.taskManagers.forEach(GenericContainer::stop);
        this.jobManager.stop();
        if (this.haService != null) {
            this.haService.stop();
        }
    }

    /** Gets the running state of the cluster. */
    public boolean isRunning() {
        return isRunning;
    }

    /** Gets JobManager container. */
    public GenericContainer<?> getJobManager() {
        return this.jobManager;
    }

    /** Gets JobManager's hostname on the host machine. */
    public String getJobManagerHost() {
        return jobManager.getHost();
    }

    /** Gets JobManager's port on the host machine. */
    public int getJobManagerPort() {
        return jobManager.getMappedPort(this.conf.get(RestOptions.PORT));
    }

    /** Gets REST client connected to JobManager. */
    public RestClusterClient<StandaloneClusterId> getRestClusterClient() {
        return this.restClusterClient;
    }

    /**
     * Restarts JobManager container.
     *
     * <p>Note that the REST port will be changed because the new JM container will be mapped to
     * another random port. Pleas make sure to get the REST cluster client again after this method
     * is invoked.
     */
    public void restartJobManager(RunnableWithException afterFailAction) throws Exception {
        if (this.haService == null) {
            LOG.warn(
                    "Restarting JobManager without HA service. This might drop all your running jobs");
        }
        jobManager.stop();
        afterFailAction.run();
        jobManager.start();
        // Recreate client because JobManager REST port might have been changed in new container
        this.restClusterClient = createClusterClient();
        waitUntilAllTaskManagerConnected();
    }

    /** Restarts all TaskManager containers. */
    public void restartTaskManager(RunnableWithException afterFailAction) throws Exception {
        taskManagers.forEach(GenericContainer::stop);
        afterFailAction.run();
        taskManagers.forEach(GenericContainer::start);
    }

    /**
     * Submits an SQL job to the running cluster.
     *
     * <p><b>NOTE:</b> You should not use {@code '\t'}.
     */
    public void submitSQLJob(SQLJobSubmission job) throws IOException, InterruptedException {
        checkState(isRunning(), "SQL job submission is only applicable for a running cluster");
        // Create SQL script and copy it to JobManager
        final List<String> commands = new ArrayList<>();
        Path script = Files.createTempDirectory("sql-script").resolve("script");
        Files.write(script, job.getSqlLines());
        jobManager.copyFileToContainer(MountableFile.forHostPath(script), "/tmp/script.sql");

        // Construct SQL client command
        commands.add("cat /tmp/script.sql | ");
        commands.add("flink/bin/sql-client.sh");
        for (String jar : job.getJars()) {
            commands.add("--jar");
            Path path = Paths.get(jar);
            String containerPath = "/tmp/" + path.getFileName();
            jobManager.copyFileToContainer(MountableFile.forHostPath(path), containerPath);
            commands.add(containerPath);
        }

        // Execute command in JobManager
        Container.ExecResult execResult =
                jobManager.execInContainer("bash", "-c", String.join(" ", commands));
        LOG.info(execResult.getStdout());
        LOG.error(execResult.getStderr());
        if (execResult.getExitCode() != 0) {
            throw new AssertionError("Failed when submitting the SQL job.");
        }
    }

    // ------------------------ JUnit 5 lifecycle management ------------------------
    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        this.start();
    }

    @Override
    public void afterAll(ExtensionContext context) {
        this.stop();
    }

    // ----------------------------- Helper functions --------------------------------

    private RestClusterClient<StandaloneClusterId> createClusterClient() throws Exception {
        checkState(
                jobManager.isRunning(), "JobManager should be running for creating a REST client");
        final Configuration clientConfiguration = new Configuration();
        clientConfiguration.set(RestOptions.ADDRESS, "localhost");
        clientConfiguration.set(
                RestOptions.PORT, jobManager.getMappedPort(conf.get(RestOptions.PORT)));
        return new RestClusterClient<>(clientConfiguration, StandaloneClusterId.getInstance());
    }

    private void waitUntilAllTaskManagerConnected() throws InterruptedException, TimeoutException {
        LOG.debug("Waiting for all TaskManagers connecting to JobManager");
        CommonTestUtils.waitUtil(
                () -> {
                    final ClusterOverviewWithVersion clusterOverview;
                    try {
                        clusterOverview =
                                this.restClusterClient
                                        .sendRequest(
                                                ClusterOverviewHeaders.getInstance(),
                                                EmptyMessageParameters.getInstance(),
                                                EmptyRequestBody.getInstance())
                                        .get();
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to get cluster overview", e);
                    }
                    return clusterOverview.getNumTaskManagersConnected() == taskManagers.size();
                },
                Duration.ofSeconds(30),
                "TaskManagers are not ready within 30 seconds");
    }
}
