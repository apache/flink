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

package org.apache.flink.connectors.test.common.utils;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connectors.test.common.TestResource;
import org.apache.flink.connectors.test.common.source.ControllableSource;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobExceptionsHeaders;
import org.apache.flink.runtime.rest.messages.JobExceptionsInfo;
import org.apache.flink.runtime.rest.messages.job.JobExceptionsMessageParameters;

import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Flink cluster running on <a href="https://www.testcontainers.org/">Testcontainers</a>.
 *
 * <p>This cluster is integrated with components below:
 * <li>Job manager and task managers
 * <li>REST cluster client for job status tracking
 * <li>Workspace directory for storing files generated in flink jobs
 * <li>Checkpoint directory for storing checkpoints
 * <li>Job directory for storing Flink job JAR files
 */
public class FlinkContainers implements TestResource {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkContainers.class);

    private final GenericContainer<?> jobManager;
    private final List<GenericContainer<?>> taskManagers;

    // Workspace directory for file exchange between testing framework and Flink containers
    private final TemporaryFolder workspaceDirOutside = new TemporaryFolder();
    private static final File workspaceDirInside = new File("/workspace");

    // Checkpoint directory for storing checkpoints
    private final TemporaryFolder checkpointDirOutside = new TemporaryFolder();
    private static final File checkpointDirInside = new File("/checkpoint");

    // Job directory for saving job JARs inside Flink containers
    private static final File jobDirInside = new File("/jobs");

    // Flink client for monitoring job status
    private RestClusterClient<String> client;

    // Whether keep all temporary folders for post-test checking
    boolean keepTestScene = false;

    /**
     * Construct a flink container group.
     *
     * @param jobManager Job manager container
     * @param taskManagers List of task manager containers
     */
    private FlinkContainers(
            GenericContainer<?> jobManager, List<GenericContainer<?>> taskManagers) {
        this.jobManager = Objects.requireNonNull(jobManager);
        this.taskManagers = Objects.requireNonNull(taskManagers);
    }

    /**
     * Get a builder of {@link FlinkContainers}.
     *
     * @param appName Name of the cluster
     * @param numTaskManagers Number of task managers
     * @return Builder of org.apache.flink.connectors.e2e.common.util.FlinkContainers
     */
    public static Builder builder(String appName, int numTaskManagers) {
        return new Builder(appName, numTaskManagers);
    }

    @Override
    public void startUp() throws Exception {
        LOG.info("🐳 Launching Flink cluster on Docker...");

        // Create temporary workspace and link it to Flink containers
        workspaceDirOutside.create();
        jobManager.withFileSystemBind(
                workspaceDirOutside.getRoot().getAbsolutePath(),
                workspaceDirInside.getAbsolutePath(),
                BindMode.READ_WRITE);
        taskManagers.forEach(
                tm ->
                        tm.withFileSystemBind(
                                workspaceDirOutside.getRoot().getAbsolutePath(),
                                workspaceDirInside.getAbsolutePath(),
                                BindMode.READ_WRITE));

        // Create checkpoint folder and link to Flink containers
        checkpointDirOutside.create();
        jobManager.withFileSystemBind(
                checkpointDirOutside.getRoot().getAbsolutePath(),
                checkpointDirInside.getAbsolutePath(),
                BindMode.READ_WRITE);
        taskManagers.forEach(
                tm ->
                        tm.withFileSystemBind(
                                checkpointDirOutside.getRoot().getAbsolutePath(),
                                checkpointDirInside.getAbsolutePath(),
                                BindMode.READ_WRITE));

        // Launch JM
        jobManager.start();
        LOG.debug(
                "Flink Job Manager is running on {}:{}",
                getJobManagerHost(),
                getJobManagerRESTPort());

        // Launch TMs
        taskManagers.forEach(GenericContainer::start);
        LOG.debug("{} Flink TaskManager(s) are running", taskManagers.size());

        // Flink configurations
        Configuration flinkConf = new Configuration();
        flinkConf.setString(JobManagerOptions.ADDRESS, getJobManagerHost());
        flinkConf.setInteger(RestOptions.PORT, getJobManagerRESTPort());

        // Prepare RestClusterClient
        Configuration clientConf = new Configuration();
        clientConf.setString(JobManagerOptions.ADDRESS, getJobManagerHost());
        clientConf.setInteger(RestOptions.PORT, getJobManagerRESTPort());
        client = new RestClusterClient<>(clientConf, "docker-cluster");
    }

    @Override
    public void tearDown() {
        LOG.info("🐳 Tearing down Flink cluster on Docker...");

        if (!keepTestScene) {
            workspaceDirOutside.delete();
            checkpointDirOutside.delete();
        } else {
            LOG.debug(
                    "Workspace directory is kept at {}",
                    workspaceDirOutside.getRoot().getAbsolutePath());
            LOG.debug(
                    "Checkpoint directory is kept at {}",
                    checkpointDirOutside.getRoot().getAbsolutePath());
        }
        jobManager.stop();
        LOG.debug("Flink JobManager is stopped");
        taskManagers.forEach(GenericContainer::stop);
        LOG.debug("{} Flink TaskManager(s) are stopped", taskManagers.size());
        client.close();
    }

    public void keepTestScene() {
        keepTestScene = true;
    }

    // ---------------------------- Flink job controlling ---------------------------------

    /**
     * Submit a JAR file to Flink cluster.
     *
     * @param jarFileOutside JAR file on the host machine outside the Flink containers
     * @param mainClass Name of the Flink job's main class
     * @return ID of the job
     * @throws FileNotFoundException if the JAR file does not exist
     */
    public JobID submitJob(File jarFileOutside, String mainClass) throws FileNotFoundException {
        return copyAndSubmitJarJob(jarFileOutside, mainClass, null);
    }

    /**
     * Copy the JAR file into the job directory and submit it to Flink cluster.
     *
     * @param jarFileOutside JAR file on the host machine outside the Flink containers
     * @param mainClass Name of the Flink job's main class
     * @param args Arguments for running 'flink job' command
     * @return ID of the job
     * @throws FileNotFoundException if the JAR file does not exist
     */
    public JobID copyAndSubmitJarJob(File jarFileOutside, String mainClass, String[] args)
            throws FileNotFoundException {
        // Validate JAR file first
        if (!jarFileOutside.exists()) {
            throw new FileNotFoundException(
                    "JAR file '" + jarFileOutside.getAbsolutePath() + "' does not exist");
        }

        jobManager.copyFileToContainer(
                MountableFile.forHostPath(jarFileOutside.getAbsolutePath()),
                Paths.get(jobDirInside.getAbsolutePath(), jarFileOutside.getName()).toString());
        Path jarPathInside = Paths.get(jobDirInside.getAbsolutePath(), jarFileOutside.getName());
        return submitJarJob(jarPathInside.toAbsolutePath().toString(), mainClass, args);
    }

    /**
     * Submit the JAR with the file path inside the container.
     *
     * @param jarPathInside Path of the JAR inside the container
     * @param mainClass Name of the Flink job's main class
     * @param args Arguments for running 'flink job' command
     * @return ID of the job
     */
    public JobID submitJarJob(String jarPathInside, String mainClass, String[] args) {
        try {
            LOG.debug("Submitting job {} ...", mainClass);
            List<String> commandLine = new ArrayList<>();
            commandLine.add("flink");
            commandLine.add("run");
            commandLine.add("-d");
            commandLine.add("-c");
            commandLine.add(mainClass);
            commandLine.add(jarPathInside);
            if (args != null && args.length > 0) {
                commandLine.addAll(Arrays.asList(args));
            }
            LOG.debug("Executing command in JM: {}", String.join(" ", commandLine));
            Container.ExecResult result =
                    jobManager.execInContainer(commandLine.toArray(new String[0]));
            if (result.getExitCode() != 0) {
                LOG.error(
                        "Command \"flink run\" exited with code {}. \nSTDOUT: {}\nSTDERR: {}",
                        result.getExitCode(),
                        result.getStdout(),
                        result.getStderr());
                throw new IllegalStateException(
                        "Command \"flink run\" exited with code " + result.getExitCode());
            }
            LOG.debug(result.getStdout());
            JobID jobID = parseJobID(result.getStdout());
            LOG.debug("Job {} has been submitted with JobID {}", mainClass, jobID);
            return jobID;
        } catch (Exception e) {
            throw new RuntimeException("Failed to submit JAR to Flink cluster", e);
        }
    }

    /**
     * Get the status of a job.
     *
     * @param jobID ID of the job
     * @return CompletableFuture of JobStatus
     */
    public CompletableFuture<JobStatus> getJobStatus(JobID jobID) {
        return client.getJobStatus(jobID);
    }

    /**
     * Wait for job entering the given expected status.
     *
     * @param jobID ID of the job
     * @param expectedStatus The expected status of the job
     * @return CompletableFuture for waiting the job status transition
     */
    public CompletableFuture<Void> waitForJobStatus(JobID jobID, JobStatus expectedStatus) {
        return CompletableFuture.runAsync(
                () -> {
                    JobStatus status = null;
                    try {
                        while (status == null || !status.equals(expectedStatus)) {
                            status = getJobStatus(jobID).get();
                            if (status.isTerminalState()) {
                                break;
                            }
                        }
                    } catch (Exception e) {
                        LOG.error("Get job status failed", e);
                        throw new CompletionException(e);
                    }
                    // If the job is entering an unexpected terminal status
                    if (status.isTerminalState() && !status.equals(expectedStatus)) {
                        try {
                            LOG.error(
                                    "Job has entered a terminal status {}, but expected {}",
                                    status,
                                    expectedStatus);
                            if (status.equals(JobStatus.FAILED)) {
                                JobExceptionsInfo exceptionsInfo = getJobRootException(jobID).get();
                                LOG.error(
                                        "Root exception of the job: \n{}",
                                        exceptionsInfo.getRootException());
                            }
                        } catch (Exception e) {
                            LOG.error("Error when processing job status", e);
                            throw new CompletionException(e);
                        }
                        throw new CompletionException(
                                new IllegalStateException(
                                        "Job has entered unexpected termination status"));
                    }
                });
    }

    /**
     * Wait for job being terminated by {@link SuccessException} thrown by map operator.
     *
     * @param jobID ID of the job
     * @return CompletableFuture for waiting the job status transition
     */
    public CompletableFuture<Void> waitForFailingWithSuccessException(JobID jobID) {
        return CompletableFuture.runAsync(
                () -> {
                    try {
                        waitForJobStatus(jobID, JobStatus.FAILED).get();
                        JobExceptionsInfo exceptionsInfo = getJobRootException(jobID).get();
                        if (exceptionsInfo.getAllExceptions().isEmpty()) {
                            throw new CompletionException(
                                    new IllegalStateException(
                                            "No exception are thrown in the job"));
                        }
                        // Analyze exception
                        boolean anyMatch =
                                Arrays.stream(exceptionsInfo.getRootException().split("\n"))
                                        .filter(line -> line.startsWith("Caused by: "))
                                        .anyMatch(
                                                line ->
                                                        line.contains(
                                                                SuccessException.class
                                                                        .getCanonicalName()));
                        if (!anyMatch) {
                            LOG.error("Root exception: {}", exceptionsInfo.getRootException());
                            throw new CompletionException(
                                    new IllegalStateException(
                                            "Cannot find SuccessException in stacktrace"));
                        }
                    } catch (Exception e) {
                        throw new CompletionException(e);
                    }
                });
    }

    /**
     * Wait for job entering a termination status.
     *
     * @param jobID ID of the job
     * @return CompletableFuture of the final job status
     */
    public CompletableFuture<JobStatus> waitForJobTermination(JobID jobID) {
        return CompletableFuture.supplyAsync(
                () -> {
                    JobStatus status = null;
                    try {
                        while (status == null || !status.isTerminalState()) {
                            status = getJobStatus(jobID).get();
                        }
                    } catch (Exception e) {
                        LOG.error("Get job status failed", e);
                        throw new CompletionException(e);
                    }
                    return status;
                });
    }

    /**
     * Get the root exception of the job for failure analyzing.
     *
     * @param jobID ID of the job
     * @return CompletableFuture of job exception information
     */
    public CompletableFuture<JobExceptionsInfo> getJobRootException(JobID jobID) {
        final JobExceptionsHeaders exceptionsHeaders = JobExceptionsHeaders.getInstance();
        final JobExceptionsMessageParameters params =
                exceptionsHeaders.getUnresolvedMessageParameters();
        params.jobPathParameter.resolve(jobID);
        return client.sendRequest(exceptionsHeaders, params, EmptyRequestBody.getInstance());
    }

    private JobID parseJobID(String stdoutString) {
        Pattern pattern = Pattern.compile("JobID ([a-f0-9]*)");
        Matcher matcher = pattern.matcher(stdoutString);
        if (matcher.find()) {
            return JobID.fromHexString(matcher.group(1));
        } else {
            throw new IllegalStateException("Cannot find JobID from the output of \"flink run\"");
        }
    }

    // ---------------------------- Flink containers properties ------------------------------

    /**
     * Get the hostname of job manager.
     *
     * @return Hostname of job manager in string
     */
    public String getJobManagerHost() {
        return jobManager.getHost();
    }

    /**
     * Get the port of job master's REST service running on.
     *
     * @return Port number in int
     */
    public int getJobManagerRESTPort() {
        return jobManager.getMappedPort(Builder.JOBMANAGER_REST_PORT);
    }

    /**
     * Get workspace folder from host machine.
     *
     * @return Workspace folder in File
     */
    public File getWorkspaceFolderOutside() {
        return workspaceDirOutside.getRoot();
    }

    /**
     * Get workspace folder inside containers.
     *
     * @return Workspace folder in File
     */
    public static File getWorkspaceDirInside() {
        return workspaceDirInside;
    }

    public GenericContainer<?> getJobManager() {
        return jobManager;
    }

    public List<Integer> getTaskManagerRMIPorts() {
        List<Integer> rmiPortList = new ArrayList<>();
        for (GenericContainer<?> taskManager : taskManagers) {
            rmiPortList.add(taskManager.getMappedPort(ControllableSource.RMI_PORT));
        }
        return rmiPortList;
    }

    // --------------------------- Introduce failure ---------------------------------

    /**
     * Shutdown a task manager container and restart it.
     *
     * @param taskManagerIndex Index of task manager container to be restarted
     */
    public void restartTaskManagers(int taskManagerIndex) {
        if (taskManagerIndex >= taskManagers.size()) {
            throw new IndexOutOfBoundsException(
                    "Invalid TaskManager index "
                            + taskManagerIndex
                            + ". Valid values are 0 to "
                            + (taskManagers.size() - 1));
        }
        final GenericContainer<?> taskManager = taskManagers.get(taskManagerIndex);
        taskManager.stop();
        taskManager.start();
    }

    /** Builder of {@link FlinkContainers}. */
    public static final class Builder {

        private static final String FLINK_IMAGE_NAME = "flink:1.13-SNAPSHOT";
        private static final String JOBMANAGER_HOSTNAME = "jobmaster";
        private static final int JOBMANAGER_REST_PORT = 8081;

        private final String appName;
        private final int numTaskManagers;
        private final Map<String, String> flinkProperties = new HashMap<>();
        private final Network flinkNetwork = Network.newNetwork();
        private final List<GenericContainer<?>> dependentContainers = new ArrayList<>();

        public Builder(String appName, int numTaskManagers) {
            this.appName = appName;
            this.numTaskManagers = numTaskManagers;
            this.flinkProperties.put("jobmanager.rpc.address", JOBMANAGER_HOSTNAME);
            this.flinkProperties.put(
                    "state.checkpoints.dir", "file://" + checkpointDirInside.getAbsolutePath());
        }

        public FlinkContainers.Builder dependOn(GenericContainer<?> container) {
            container.withNetwork(flinkNetwork);
            dependentContainers.add(container);
            return this;
        }

        public FlinkContainers build() {
            return new FlinkContainers(
                    createJobMasterContainer(flinkProperties, flinkNetwork, dependentContainers),
                    createTaskManagerContainers(flinkProperties, flinkNetwork, numTaskManagers));
        }

        private static GenericContainer<?> createJobMasterContainer(
                Map<String, String> flinkProperties,
                Network flinkNetwork,
                List<GenericContainer<?>> dependentContainers) {
            return new GenericContainer<>(DockerImageName.parse(FLINK_IMAGE_NAME))
                    .withExposedPorts(JOBMANAGER_REST_PORT)
                    .withEnv("FLINK_PROPERTIES", toFlinkPropertiesString(flinkProperties))
                    .withCommand("jobmanager")
                    .withNetwork(flinkNetwork)
                    .withNetworkAliases(JOBMANAGER_HOSTNAME)
                    .dependsOn(dependentContainers);
        }

        private static List<GenericContainer<?>> createTaskManagerContainers(
                Map<String, String> flinkProperties, Network flinkNetwork, int numTaskManagers) {
            List<GenericContainer<?>> taskManagers = new ArrayList<>();
            for (int i = 0; i < numTaskManagers; ++i) {
                taskManagers.add(
                        new GenericContainer<>(DockerImageName.parse(FLINK_IMAGE_NAME))
                                .withExposedPorts(ControllableSource.RMI_PORT)
                                .withEnv(
                                        "FLINK_PROPERTIES",
                                        toFlinkPropertiesString(flinkProperties))
                                .withCommand("taskmanager")
                                .withNetwork(flinkNetwork)
                                .waitingFor(
                                        Wait.forLogMessage(
                                                ".*Successful registration at resource manager.*",
                                                1)));
            }
            return taskManagers;
        }

        private static String toFlinkPropertiesString(Map<String, String> flinkProperties) {
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, String> property : flinkProperties.entrySet()) {
                sb.append(property.getKey()).append(": ").append(property.getValue()).append("\n");
            }
            return sb.toString();
        }
    }
}
