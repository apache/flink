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

package org.apache.flink.yarn;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.deployment.ClusterDeploymentException;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;
import org.apache.flink.runtime.rest.messages.job.metrics.JobMetricsHeaders;
import org.apache.flink.runtime.rest.messages.job.metrics.JobMetricsMessageParameters;
import org.apache.flink.runtime.rest.messages.job.metrics.Metric;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.ZooKeeperTestUtils;
import org.apache.flink.util.OperatingSystem;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.entrypoint.YarnSessionClusterEntrypoint;
import org.apache.flink.yarn.testjob.YarnTestJob;
import org.apache.flink.yarn.util.TestUtils;

import org.apache.flink.shaded.guava31.com.google.common.collect.Iterables;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.NMTokenIdentifier;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkState;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/** Tests that verify correct HA behavior. */
class YARNHighAvailabilityITCase extends YarnTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(YARNHighAvailabilityITCase.class);

    private static final String LOG_DIR = "flink-yarn-tests-ha";

    private static TestingServer zkServer;
    private static String storageDir;

    private YarnTestJob.StopJobSignal stopJobSignal;
    private JobGraph job;

    @BeforeAll
    static void setup(@TempDir File tempDir) throws Exception {
        zkServer = ZooKeeperTestUtils.createAndStartZookeeperTestingServer();

        storageDir = tempDir.getAbsolutePath();

        // startYARNWithConfig should be implemented by subclass
        YARN_CONFIGURATION.setClass(
                YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class, ResourceScheduler.class);
        YARN_CONFIGURATION.set(YarnTestBase.TEST_CLUSTER_NAME_KEY, LOG_DIR);
        YARN_CONFIGURATION.setInt(YarnConfiguration.NM_PMEM_MB, 4096);
        startYARNWithConfig(YARN_CONFIGURATION);
    }

    @AfterAll
    static void teardown() throws Exception {
        try {
            YarnTestBase.teardown();
        } finally {
            if (zkServer != null) {
                zkServer.close();
                zkServer = null;
            }
        }
    }

    @BeforeEach
    void setUp(@TempDir File tempDir) {
        stopJobSignal = YarnTestJob.StopJobSignal.usingMarkerFile(tempDir.toPath());
        job = YarnTestJob.stoppableJob(stopJobSignal);
        final File testingJar =
                TestUtils.findFile("..", new TestUtils.TestJarFinder("flink-yarn-tests"));

        assertThat(testingJar).isNotNull();

        job.addJar(new org.apache.flink.core.fs.Path(testingJar.toURI()));
    }

    /**
     * Tests that Yarn will restart a killed {@link YarnSessionClusterEntrypoint} which will then
     * resume a persisted {@link JobGraph}.
     */
    @Test
    void testKillYarnSessionClusterEntrypoint() throws Exception {
        runTest(
                () -> {
                    assumeThat(
                                    OperatingSystem.isLinux()
                                            || OperatingSystem.isMac()
                                            || OperatingSystem.isFreeBSD()
                                            || OperatingSystem.isSolaris())
                            .as(
                                    "This test kills processes via the pkill command. Thus, it only runs on Linux, Mac OS, Free BSD and Solaris.")
                            .isTrue();

                    final YarnClusterDescriptor yarnClusterDescriptor =
                            setupYarnClusterDescriptor();
                    final RestClusterClient<ApplicationId> restClusterClient =
                            deploySessionCluster(yarnClusterDescriptor);

                    try {
                        final JobID jobId = submitJob(restClusterClient);
                        final ApplicationId id = restClusterClient.getClusterId();

                        waitUntilJobIsRunning(restClusterClient, jobId);

                        killApplicationMaster(
                                yarnClusterDescriptor.getYarnSessionClusterEntrypoint());
                        waitForApplicationAttempt(id, 2);

                        waitForJobTermination(restClusterClient, jobId);

                        killApplicationAndWait(id);
                    } finally {
                        restClusterClient.close();
                    }
                });
    }

    @Test
    void testJobRecoversAfterKillingTaskManager() throws Exception {
        runTest(
                () -> {
                    final YarnClusterDescriptor yarnClusterDescriptor =
                            setupYarnClusterDescriptor();
                    try (RestClusterClient<ApplicationId> restClusterClient =
                            deploySessionCluster(yarnClusterDescriptor)) {
                        final JobID jobId = submitJob(restClusterClient);
                        waitUntilJobIsRunning(restClusterClient, jobId);

                        stopTaskManagerContainer();
                        waitUntilJobIsRestarted(restClusterClient, jobId, 1);

                        waitForJobTermination(restClusterClient, jobId);

                        killApplicationAndWait(restClusterClient.getClusterId());
                    }
                });
    }

    /**
     * Tests that we can retrieve an HA enabled cluster by only specifying the application id if no
     * other high-availability.cluster-id has been configured. See FLINK-20866.
     */
    @Test
    void testClusterClientRetrieval() throws Exception {
        runTest(
                () -> {
                    final YarnClusterDescriptor yarnClusterDescriptor =
                            setupYarnClusterDescriptor();
                    final RestClusterClient<ApplicationId> restClusterClient =
                            deploySessionCluster(yarnClusterDescriptor);

                    ClusterClient<ApplicationId> newClusterClient = null;
                    try {
                        final ApplicationId clusterId = restClusterClient.getClusterId();

                        final YarnClusterDescriptor newClusterDescriptor =
                                setupYarnClusterDescriptor();
                        newClusterClient =
                                newClusterDescriptor.retrieve(clusterId).getClusterClient();

                        assertThat(newClusterClient.listJobs().join()).isEmpty();

                        newClusterClient.shutDownCluster();
                    } finally {
                        restClusterClient.close();

                        if (newClusterClient != null) {
                            newClusterClient.close();
                        }
                    }
                });
    }

    private void waitForApplicationAttempt(final ApplicationId applicationId, final int attemptId)
            throws Exception {
        final YarnClient yarnClient = getYarnClient();
        checkState(yarnClient != null, "yarnClient must be initialized");

        CommonTestUtils.waitUntilCondition(
                () -> {
                    final ApplicationReport applicationReport =
                            yarnClient.getApplicationReport(applicationId);
                    return applicationReport.getCurrentApplicationAttemptId().getAttemptId()
                            >= attemptId;
                });
        LOG.info("Attempt {} id detected.", attemptId);
    }

    /** Stops a container running {@link YarnTaskExecutorRunner}. */
    private void stopTaskManagerContainer() throws Exception {
        // find container id of taskManager:
        ContainerId taskManagerContainer = null;
        NodeManager nodeManager = null;
        NMTokenIdentifier nmIdent = null;
        UserGroupInformation remoteUgi = UserGroupInformation.getCurrentUser();

        for (int nmId = 0; nmId < NUM_NODEMANAGERS; nmId++) {
            NodeManager nm = yarnCluster.getNodeManager(nmId);
            ConcurrentMap<ContainerId, Container> containers = nm.getNMContext().getContainers();
            for (Map.Entry<ContainerId, Container> entry : containers.entrySet()) {
                String command =
                        StringUtils.join(entry.getValue().getLaunchContext().getCommands(), " ");
                if (command.contains(YarnTaskExecutorRunner.class.getSimpleName())) {
                    taskManagerContainer = entry.getKey();
                    nodeManager = nm;
                    nmIdent =
                            new NMTokenIdentifier(
                                    taskManagerContainer.getApplicationAttemptId(), null, "", 0);
                    // allow myself to do stuff with the container
                    // remoteUgi.addCredentials(entry.getValue().getCredentials());
                    remoteUgi.addTokenIdentifier(nmIdent);
                }
            }
        }

        assertThat(taskManagerContainer).isNotNull();
        assertThat(nodeManager).isNotNull();

        StopContainersRequest scr =
                StopContainersRequest.newInstance(Collections.singletonList(taskManagerContainer));

        nodeManager.getNMContext().getContainerManager().stopContainers(scr);

        // cleanup auth for the subsequent tests.
        remoteUgi.getTokenIdentifiers().remove(nmIdent);
    }

    private void killApplicationAndWait(final ApplicationId id) throws Exception {
        final YarnClient yarnClient = getYarnClient();
        checkState(yarnClient != null, "yarnClient must be initialized");

        yarnClient.killApplication(id);

        CommonTestUtils.waitUntilCondition(
                () ->
                        !getApplicationReportWithRetryOnNPE(
                                        yarnClient,
                                        EnumSet.of(
                                                YarnApplicationState.KILLED,
                                                YarnApplicationState.FINISHED))
                                .isEmpty());
    }

    private void waitForJobTermination(
            final RestClusterClient<ApplicationId> restClusterClient, final JobID jobId)
            throws Exception {
        LOG.info("Sending stop job signal");
        stopJobSignal.signal();
        final CompletableFuture<JobResult> jobResult = restClusterClient.requestJobResult(jobId);
        jobResult.get(200, TimeUnit.SECONDS);
    }

    @Nonnull
    private YarnClusterDescriptor setupYarnClusterDescriptor() {
        final Configuration flinkConfiguration = new Configuration();
        flinkConfiguration.set(JobManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.ofMebiBytes(768));
        flinkConfiguration.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.parse("1g"));
        flinkConfiguration.setString(YarnConfigOptions.APPLICATION_ATTEMPTS, "10");
        flinkConfiguration.setString(HighAvailabilityOptions.HA_MODE, "zookeeper");
        flinkConfiguration.setString(HighAvailabilityOptions.HA_STORAGE_PATH, storageDir);
        flinkConfiguration.setString(
                HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, zkServer.getConnectString());
        flinkConfiguration.setInteger(HighAvailabilityOptions.ZOOKEEPER_SESSION_TIMEOUT, 20000);

        flinkConfiguration.setString(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        flinkConfiguration.setInteger(
                RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, Integer.MAX_VALUE);

        return createYarnClusterDescriptor(flinkConfiguration);
    }

    private RestClusterClient<ApplicationId> deploySessionCluster(
            YarnClusterDescriptor yarnClusterDescriptor) throws ClusterDeploymentException {
        final int masterMemory =
                yarnClusterDescriptor
                        .getFlinkConfiguration()
                        .get(JobManagerOptions.TOTAL_PROCESS_MEMORY)
                        .getMebiBytes();
        final int taskManagerMemory = 1024;
        final ClusterClient<ApplicationId> yarnClusterClient =
                yarnClusterDescriptor
                        .deploySessionCluster(
                                new ClusterSpecification.ClusterSpecificationBuilder()
                                        .setMasterMemoryMB(masterMemory)
                                        .setTaskManagerMemoryMB(taskManagerMemory)
                                        .setSlotsPerTaskManager(1)
                                        .createClusterSpecification())
                        .getClusterClient();

        assertThat(yarnClusterClient).isInstanceOf(RestClusterClient.class);
        return (RestClusterClient<ApplicationId>) yarnClusterClient;
    }

    private JobID submitJob(RestClusterClient<ApplicationId> restClusterClient)
            throws InterruptedException, java.util.concurrent.ExecutionException {
        return restClusterClient.submitJob(job).get();
    }

    private void killApplicationMaster(final String processName) throws Exception {
        final Set<Integer> origPids = getApplicationMasterPids(processName);
        assertThat(origPids).isNotEmpty();

        final Process exec = Runtime.getRuntime().exec("pkill -f " + processName);
        assertThat(exec.waitFor()).isEqualTo(0);

        CommonTestUtils.waitUntilCondition(
                () -> {
                    final Set<Integer> curPids = getApplicationMasterPids(processName);
                    return origPids.stream().noneMatch(curPids::contains);
                });
    }

    private Set<Integer> getApplicationMasterPids(final String processName)
            throws IOException, InterruptedException {
        final Process exec = Runtime.getRuntime().exec("pgrep -f " + processName);

        if (exec.waitFor() != 0) {
            return Collections.emptySet();
        }

        return Arrays.stream(
                        IOUtils.toString(exec.getInputStream(), StandardCharsets.UTF_8)
                                .split("\\s+"))
                .map(Integer::valueOf)
                .collect(Collectors.toSet());
    }

    private static void waitUntilJobIsRunning(
            RestClusterClient<ApplicationId> restClusterClient, JobID jobId) throws Exception {
        CommonTestUtils.waitUntilCondition(
                () -> {
                    final JobDetailsInfo jobDetails = restClusterClient.getJobDetails(jobId).get();
                    return jobDetails.getJobStatus() == JobStatus.RUNNING
                            && jobDetails.getJobVertexInfos().stream()
                                    .map(toExecutionState())
                                    .allMatch(isRunning());
                });
    }

    private static Function<JobDetailsInfo.JobVertexDetailsInfo, ExecutionState>
            toExecutionState() {
        return JobDetailsInfo.JobVertexDetailsInfo::getExecutionState;
    }

    private static Predicate<ExecutionState> isRunning() {
        return executionState -> executionState == ExecutionState.RUNNING;
    }

    private static void waitUntilJobIsRestarted(
            final RestClusterClient<ApplicationId> restClusterClient,
            final JobID jobId,
            final int expectedFullRestarts)
            throws Exception {
        CommonTestUtils.waitUntilCondition(
                () -> getJobFullRestarts(restClusterClient, jobId) >= expectedFullRestarts);
    }

    private static int getJobFullRestarts(
            final RestClusterClient<ApplicationId> restClusterClient, final JobID jobId)
            throws Exception {

        return getJobMetric(restClusterClient, jobId, "fullRestarts")
                .map(Metric::getValue)
                .map(Integer::parseInt)
                .orElse(0);
    }

    private static Optional<Metric> getJobMetric(
            final RestClusterClient<ApplicationId> restClusterClient,
            final JobID jobId,
            final String metricName)
            throws Exception {

        final JobMetricsMessageParameters messageParameters = new JobMetricsMessageParameters();
        messageParameters.jobPathParameter.resolve(jobId);
        messageParameters.metricsFilterParameter.resolveFromString(metricName);

        final Collection<Metric> metrics =
                restClusterClient
                        .sendRequest(
                                JobMetricsHeaders.getInstance(),
                                messageParameters,
                                EmptyRequestBody.getInstance())
                        .get()
                        .getMetrics();

        final Metric metric = Iterables.getOnlyElement(metrics, null);
        checkState(metric == null || metric.getId().equals(metricName));
        return Optional.ofNullable(metric);
    }
}
