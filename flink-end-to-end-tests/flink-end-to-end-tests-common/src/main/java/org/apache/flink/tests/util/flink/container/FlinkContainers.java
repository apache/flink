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

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.rest.handler.legacy.messages.ClusterOverviewWithVersion;
import org.apache.flink.runtime.rest.messages.ClusterOverviewHeaders;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.tests.util.flink.JobSubmission;
import org.apache.flink.tests.util.flink.SQLJobSubmission;
import org.apache.flink.util.function.RunnableWithException;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.Extension;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
 *     // Remember to declare it as "static" as required by JUnit 5.
 *     @RegisterExtension
 *     static FlinkContainers flink = FlinkContainers.builder().build();
 *
 *     // To work together with other containers
 *     @RegisterExtension
 *     static FlinkContainers flink =
 *         FlinkContainers.builder()
 *             .dependsOn(kafkaContainer)
 *             .build();
 *
 *     // Customize a Flink cluster
 *     // Remember to declare it as "static" as required by JUnit 5.
 *     @RegisterExtension
 *     static FlinkContainers flink =
 *         FlinkContainers.builder()
 *             .setNumTaskManagers(3)
 *             .setConfiguration(TaskManagerOptions.NUM_TASK_SLOTS, 6)
 *             .setLogger(LoggerFactory.getLogger(E2ETest.class))
 *             .enableZookeeperHA()
 *             .build();
 *     ...
 * }
 * }</pre>
 *
 * <p>Detailed usages can be found in the JavaDoc of {@link FlinkContainersBuilder}.
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

    private final JobManagerContainer jobManager;
    private final List<TaskManagerContainer> taskManagers;
    @Nullable private final ZookeeperContainer zookeeper;
    private final Configuration conf;

    private boolean isStarted = false;

    /** Creates a builder for {@link FlinkContainers}. */
    public static FlinkContainersBuilder builder() {
        return new FlinkContainersBuilder();
    }

    FlinkContainers(
            JobManagerContainer jobManager,
            List<TaskManagerContainer> taskManagers,
            @Nullable ZookeeperContainer zookeeper,
            Configuration conf) {
        this.jobManager = jobManager;
        this.taskManagers = taskManagers;
        this.zookeeper = zookeeper;
        this.conf = conf;
    }

    /** Starts all containers. */
    public void start() throws Exception {
        if (zookeeper != null) {
            zookeeper.start();
        }
        jobManager.start();
        taskManagers.parallelStream().forEach(TaskManagerContainer::start);
        waitUntilAllTaskManagerConnected();
        isStarted = true;
    }

    /** Stops all containers. */
    public void stop() {
        isStarted = false;
        taskManagers.forEach(TaskManagerContainer::stop);
        jobManager.stop();
        if (zookeeper != null) {
            zookeeper.stop();
        }
    }

    /** Gets the running state of the cluster. */
    public boolean isStarted() {
        return isStarted;
    }

    /** Gets JobManager container. */
    public GenericContainer<JobManagerContainer> getJobManager() {
        return jobManager;
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
        return jobManager.getRestClusterClient();
    }

    /**
     * Restarts JobManager container.
     *
     * <p>Note that the REST port will be changed because the new JM container will be mapped to
     * another random port. Please make sure to get the REST cluster client again after this method
     * is invoked.
     */
    public void restartJobManager(@SuppressWarnings("unused") RunnableWithException afterFailAction)
            throws Exception {
        if (this.zookeeper == null) {
            LOG.warn(
                    "Restarting JobManager without HA service. This might drop all your running jobs");
        }
        jobManager.restart();
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

    /**
     * Submits the given job to the cluster.
     *
     * @param job job to submit
     */
    @SuppressWarnings("UnusedReturnValue")
    public JobID submitJob(JobSubmission job) throws IOException, InterruptedException {
        final List<String> commands = new ArrayList<>();
        commands.add("flink/bin/flink");
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

        LOG.info("Running {}.", String.join(" ", commands));

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

    private void waitUntilAllTaskManagerConnected() throws InterruptedException, TimeoutException {
        LOG.debug("Waiting for all TaskManagers connecting to JobManager");
        checkNotNull(
                jobManager.getRestClusterClient(),
                "REST cluster client should not be null when checking TaskManager status");
        CommonTestUtils.waitUtil(
                () -> {
                    final ClusterOverviewWithVersion clusterOverview;
                    try {
                        clusterOverview =
                                jobManager
                                        .getRestClusterClient()
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
}
