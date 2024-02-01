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

package org.apache.flink.connector.testframe.container;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.rest.handler.legacy.messages.ClusterOverviewWithVersion;
import org.apache.flink.runtime.rest.messages.ClusterOverviewHeaders;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.test.util.JobSubmission;
import org.apache.flink.test.util.SQLJobSubmission;
import org.apache.flink.util.function.RunnableWithException;

import org.apache.flink.shaded.guava31.com.google.common.collect.Lists;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.utility.MountableFile;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A Flink cluster running JM and TMs on containers.
 *
 * <p>This containerized Flink cluster is based on <a
 * href="https://www.testcontainers.org/">Testcontainers</a>, which simulates a truly distributed
 * environment for E2E tests. This class can also be used as an {@link Extension} of JUnit 5 so that
 * the lifecycle of the cluster can be easily managed by JUnit Jupiter engine.
 *
 * <h2>Example usage</h2>
 *
 * <pre>{@code
 * public class E2ETest {
 *     // Create a Flink cluster using default configurations.
 *     // Remember to declare it as "static" as required by
 *     // JUnit 5.
 *     @RegisterExtension
 *     static FlinkContainers flink =
 *          FlinkContainers.builder().build();
 *
 * ---
 *
 *     // To work together with other containers
 *     static TestcontainersSettings testcontainersSettings =
 *          TestcontainersSettings.builder()
 *                 .dependsOn(kafkaContainer)
 *                 .build());
 *
 *     @RegisterExtension
 *     static FlinkContainers flink =
 *             FlinkContainers.builder()
 *              .withTestcontainersSettings(testcontainersSettings)
 *              .build();
 *
 * ---
 *     // Customize a Flink cluster
 *     static FlinkContainersSettings flinkContainersSettings =
 *          FlinkContainersSettings.builder()
 *             .numTaskManagers(3)
 *             .numSlotsPerTaskManager(6)
 *             .baseImage(
 *               "ghcr.io/apache/flink-docker:1.16-snapshot-scala_2.12-java11-debian")
 *             .enableZookeeperHA()
 *             .build();
 *
 *     static TestcontainersSettings testcontainersSettings =
 *          TestcontainersSettings.builder()
 *                 .setLogger(
 *                      LoggerFactory.getLogger(E2ETest.class))
 *                 .build());
 *
 *     @RegisterExtension
 *     static FlinkContainers flink =
 *         FlinkContainers.builder()
 *           .withFlinkContainersSettings(flinkContainersSettings)
 *           .withTestcontainersSettings(testcontainersSettings)
 *           .build();
 *     ...
 * }
 * }</pre>
 *
 * <p>See {@link FlinkContainersSettings} and {@link TestcontainersSettings} for available options.
 *
 * <h2>Prerequisites</h2>
 *
 * <p>Docker environment is required in the running machine since this class is based on
 * Testcontainers.
 *
 * <p>Make sure you have an already-built flink-dist either under the current project, or specify
 * the path manually in builder.
 */
public class FlinkContainers implements BeforeAllCallback, AfterAllCallback {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkContainers.class);

    // Default timeout of operations
    public static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);

    private final GenericContainer<?> jobManager;
    private final List<GenericContainer<?>> taskManagers;
    private final GenericContainer<?> haService;
    private final Configuration conf;

    @Nullable private RestClusterClient<StandaloneClusterId> restClusterClient;
    private boolean isStarted = false;

    /** The {@link FlinkContainers} builder. */
    public static final class Builder {
        private FlinkContainersSettings flinkContainersSettings =
                FlinkContainersSettings.defaultConfig();
        private TestcontainersSettings testcontainersSettings =
                TestcontainersSettings.defaultSettings();

        private Builder() {}

        /**
         * Allows to optionally provide Flink containers settings. {@link FlinkContainersSettings}
         * based on defaults will be used otherwise.
         *
         * @param flinkContainersSettings The Flink containers settings.
         * @return A reference to this Builder.
         */
        public Builder withFlinkContainersSettings(
                FlinkContainersSettings flinkContainersSettings) {
            this.flinkContainersSettings = flinkContainersSettings;
            return this;
        }

        /**
         * Allows to optionally provide Testcontainers settings. {@link TestcontainersSettings}
         * based on defaults will be used otherwise.
         *
         * @param testcontainersSettings The Testcontainers settings.
         * @return A reference to this Builder.
         */
        public Builder withTestcontainersSettings(TestcontainersSettings testcontainersSettings) {
            this.testcontainersSettings = testcontainersSettings;
            return this;
        }

        /**
         * Returns {@code FlinkContainers} built from the provided settings.
         *
         * @return {@code FlinkContainers} built with parameters of this {@code
         *     FlinkContainers.Builder}.
         */
        public FlinkContainers build() {
            FlinkTestcontainersConfigurator configurator =
                    new FlinkTestcontainersConfigurator(
                            flinkContainersSettings, testcontainersSettings);
            return configurator.configure();
        }
    }

    /** Creates a builder for {@link FlinkContainers}. */
    public static Builder builder() {
        return new Builder();
    }

    FlinkContainers(
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
        waitUntilJobManagerRESTReachable(jobManager);
        LOG.debug("Starting TaskManager containers");
        this.taskManagers.parallelStream().forEach(GenericContainer::start);
        LOG.debug("Creating REST cluster client");
        this.restClusterClient = createClusterClient();
        waitUntilAllTaskManagerConnected();
        isStarted = true;
    }

    /** Stops all containers. */
    public void stop() {
        isStarted = false;
        if (restClusterClient != null) {
            restClusterClient.close();
        }
        this.taskManagers.forEach(GenericContainer::stop);
        deleteJobManagerTemporaryFiles();
        this.jobManager.stop();
        if (this.haService != null) {
            this.haService.stop();
        }
    }

    /** Gets the running state of the cluster. */
    public boolean isStarted() {
        return isStarted;
    }

    /** Gets JobManager container. */
    public GenericContainer<?> getJobManager() {
        return this.jobManager;
    }

    /** Gets TaskManager containers. */
    public List<GenericContainer<?>> getTaskManagers() {
        return this.taskManagers;
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
    public @Nullable RestClusterClient<StandaloneClusterId> getRestClusterClient() {
        return this.restClusterClient;
    }

    /**
     * Restarts JobManager container.
     *
     * <p>Note that the REST port will be changed because the new JM container will be mapped to
     * another random port. Please make sure to get the REST cluster client again after this method
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
        waitUntilJobManagerRESTReachable(jobManager);
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
        checkState(isStarted(), "SQL job submission is only applicable for a running cluster");
        // Create SQL script and copy it to JobManager
        final List<String> commands = new ArrayList<>();
        Path script = Files.createTempDirectory("sql-script").resolve("script");
        Files.write(script, job.getSqlLines());
        jobManager.copyFileToContainer(MountableFile.forHostPath(script), "/tmp/script.sql");

        // Construct SQL client command
        commands.add("cat /tmp/script.sql | ");
        commands.add("bin/sql-client.sh");
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

    /**
     * Submits the given job to the cluster.
     *
     * @param job job to submit
     */
    public JobID submitJob(JobSubmission job) throws IOException, InterruptedException {
        final List<String> commands = new ArrayList<>();
        commands.add("bin/flink");
        commands.add("run");

        if (job.isDetached()) {
            commands.add("-d");
        }
        if (job.getParallelism() > 0) {
            commands.add("-p");
            commands.add(String.valueOf(job.getParallelism()));
        }
        job.getMainClass()
                .ifPresent(
                        mainClass -> {
                            commands.add("--class");
                            commands.add(mainClass);
                        });
        final Path jobJar = job.getJar();
        final String containerPath = "/tmp/" + jobJar.getFileName();
        commands.add(containerPath);
        jobManager.copyFileToContainer(
                MountableFile.forHostPath(jobJar.toAbsolutePath()), containerPath);
        commands.addAll(job.getArguments());

        LOG.info("Running {}.", commands.stream().collect(Collectors.joining(" ")));

        // Execute command in JobManager
        Container.ExecResult execResult =
                jobManager.execInContainer("bash", "-c", String.join(" ", commands));

        final Pattern pattern =
                job.isDetached()
                        ? Pattern.compile("Job has been submitted with JobID (.*)")
                        : Pattern.compile("Job with JobID (.*) has finished.");

        final String stdout = execResult.getStdout();
        LOG.info(stdout);
        LOG.error(execResult.getStderr());
        final Matcher matcher = pattern.matcher(stdout);
        checkState(matcher.find(), "Cannot extract JobID from stdout.");
        return JobID.fromHexString(matcher.group(1));
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
        // Close potentially existing REST cluster client
        if (restClusterClient != null) {
            restClusterClient.close();
        }
        final Configuration clientConfiguration = new Configuration();
        clientConfiguration.set(RestOptions.ADDRESS, getJobManagerHost());
        clientConfiguration.set(
                RestOptions.PORT, jobManager.getMappedPort(conf.get(RestOptions.PORT)));
        return new RestClusterClient<>(clientConfiguration, StandaloneClusterId.getInstance());
    }

    private void waitUntilJobManagerRESTReachable(GenericContainer<?> jobManager) {
        LOG.debug("Waiting for JobManager's REST interface getting ready");
        new HttpWaitStrategy()
                .forPort(conf.get(RestOptions.PORT))
                // Specify URL here because using root path will be redirected to Flink Web UI,
                // which might not be built by using "-Pskip-webui-build"
                .forPath(ClusterOverviewHeaders.URL)
                .forStatusCode(200)
                .withReadTimeout(DEFAULT_TIMEOUT)
                .waitUntilReady(jobManager);
    }

    private void waitUntilAllTaskManagerConnected() throws InterruptedException, TimeoutException {
        LOG.debug("Waiting for all TaskManagers connecting to JobManager");
        checkNotNull(
                restClusterClient,
                "REST cluster client should not be null when checking TaskManager status");
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
                DEFAULT_TIMEOUT,
                "TaskManagers are not ready within 30 seconds");
    }

    private void deleteJobManagerTemporaryFiles() {
        final String checkpointDir = conf.get(CheckpointingOptions.CHECKPOINTS_DIRECTORY);
        final String haDir = conf.get(HighAvailabilityOptions.HA_STORAGE_PATH);
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
            result = jobManager.execInContainer(command);
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
