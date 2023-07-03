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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.client.cli.CliFrontend;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.rest.RestClient;
import org.apache.flink.runtime.rest.handler.legacy.messages.ClusterOverviewWithVersion;
import org.apache.flink.runtime.rest.messages.ClusterConfigurationInfoHeaders;
import org.apache.flink.runtime.rest.messages.ClusterOverviewHeaders;
import org.apache.flink.runtime.rest.messages.ConfigurationInfo;
import org.apache.flink.runtime.rest.messages.ConfigurationInfoEntry;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerInfo;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagersHeaders;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagersInfo;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.test.testdata.WordCountData;
import org.apache.flink.testutils.logging.LoggerAuditingExtension;
import org.apache.flink.yarn.cli.FlinkYarnSessionCli;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.util.TestUtils;

import org.apache.flink.shaded.guava31.com.google.common.net.HostAndPort;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.apache.flink.util.Preconditions.checkState;
import static org.apache.flink.yarn.util.TestUtils.getTestJarPath;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Fail.fail;

/**
 * This test starts a MiniYARNCluster with a CapacityScheduler. Is has, by default a queue called
 * "default". The configuration here adds another queue: "qa-team".
 */
class YARNSessionCapacitySchedulerITCase extends YarnTestBase {
    private static final Logger LOG =
            LoggerFactory.getLogger(YARNSessionCapacitySchedulerITCase.class);

    /** RestClient to query Flink cluster. */
    private static RestClient restClient;

    /**
     * ExecutorService for {@link RestClient}.
     *
     * @see #restClient
     */
    private static ExecutorService restClientExecutor;

    /** Toggles checking for prohibited strings in logs after the test has run. */
    private boolean checkForProhibitedLogContents = true;

    @RegisterExtension
    private final LoggerAuditingExtension cliLoggerAuditingExtension =
            new LoggerAuditingExtension(CliFrontend.class, Level.INFO);

    @RegisterExtension
    private final LoggerAuditingExtension yarLoggerAuditingExtension =
            new LoggerAuditingExtension(YarnClusterDescriptor.class, Level.WARN);

    @BeforeAll
    static void setup() throws Exception {
        YARN_CONFIGURATION.setClass(
                YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class, ResourceScheduler.class);
        YARN_CONFIGURATION.set("yarn.scheduler.capacity.root.queues", "default,qa-team");
        YARN_CONFIGURATION.setInt("yarn.scheduler.capacity.root.default.capacity", 40);
        YARN_CONFIGURATION.setInt("yarn.scheduler.capacity.root.qa-team.capacity", 60);
        YARN_CONFIGURATION.set(
                YarnTestBase.TEST_CLUSTER_NAME_KEY, "flink-yarn-tests-capacityscheduler");
        startYARNWithConfig(YARN_CONFIGURATION);

        restClientExecutor = Executors.newSingleThreadExecutor();
        restClient = new RestClient(new Configuration(), restClientExecutor);
    }

    @AfterAll
    static void teardown() throws Exception {
        try {
            YarnTestBase.teardown();
        } finally {
            if (restClient != null) {
                restClient.shutdown(Time.seconds(5));
            }

            if (restClientExecutor != null) {
                restClientExecutor.shutdownNow();
            }
        }
    }

    /**
     * Tests that a session cluster, that uses the resources from the <i>qa-team</i> queue, can be
     * started from the command line.
     */
    @Test
    void testStartYarnSessionClusterInQaTeamQueue() throws Exception {
        runTest(
                () ->
                        runWithArgs(
                                new String[] {
                                    "-j",
                                    flinkUberjar.getAbsolutePath(),
                                    "-t",
                                    flinkLibFolder.getAbsolutePath(),
                                    "-jm",
                                    "768m",
                                    "-tm",
                                    "1024m",
                                    "-qu",
                                    "qa-team"
                                },
                                "JobManager Web Interface:",
                                null,
                                RunTypes.YARN_SESSION,
                                0));
    }

    /**
     * Test per-job yarn cluster
     *
     * <p>This also tests the prefixed CliFrontend options for the YARN case We also test if the
     * requested parallelism of 2 is passed through. The parallelism is requested at the YARN client
     * (-ys).
     */
    @Test
    void perJobYarnCluster() throws Exception {
        runTest(
                () -> {
                    LOG.info("Starting perJobYarnCluster()");
                    File exampleJarLocation = getTestJarPath("BatchWordCount.jar");
                    runWithArgs(
                            new String[] {
                                "run",
                                "-m",
                                "yarn-cluster",
                                "-yj",
                                flinkUberjar.getAbsolutePath(),
                                "-yt",
                                flinkLibFolder.getAbsolutePath(),
                                "-ys",
                                "2", // test that the job is executed with a DOP of 2
                                "-yjm",
                                "768m",
                                "-ytm",
                                "1024m",
                                exampleJarLocation.getAbsolutePath()
                            },
                            /* test succeeded after this string */
                            "Program execution finished",
                            /* prohibited strings: (to verify the parallelism) */
                            // (we should see "DataSink (...) (1/2)" and "DataSink (...) (2/2)"
                            // instead)
                            new String[] {"DataSink \\(.*\\) \\(1/1\\) switched to FINISHED"},
                            RunTypes.CLI_FRONTEND,
                            0,
                            cliLoggerAuditingExtension::getMessages);
                    LOG.info("Finished perJobYarnCluster()");
                });
    }

    /**
     * Test per-job yarn cluster and memory calculations for off-heap use (see FLINK-7400) with the
     * same job as {@link #perJobYarnCluster()}.
     *
     * <p>This ensures that with (any) pre-allocated off-heap memory by us, there is some off-heap
     * memory remaining for Flink's libraries. Creating task managers will thus fail if no off-heap
     * memory remains.
     */
    @Test
    void perJobYarnClusterOffHeap() throws Exception {
        runTest(
                () -> {
                    LOG.info("Starting perJobYarnCluster()");
                    File exampleJarLocation = getTestJarPath("BatchWordCount.jar");

                    // set memory constraints (otherwise this is the same test as
                    // perJobYarnCluster() above)
                    final long taskManagerMemoryMB = 1024;

                    runWithArgs(
                            new String[] {
                                "run",
                                "-m",
                                "yarn-cluster",
                                "-yj",
                                flinkUberjar.getAbsolutePath(),
                                "-yt",
                                flinkLibFolder.getAbsolutePath(),
                                "-ys",
                                "2", // test that the job is executed with a DOP of 2
                                "-yjm",
                                "768m",
                                "-ytm",
                                taskManagerMemoryMB + "m",
                                exampleJarLocation.getAbsolutePath()
                            },
                            /* test succeeded after this string */
                            "Program execution finished",
                            /* prohibited strings: (to verify the parallelism) */
                            // (we should see "DataSink (...) (1/2)" and "DataSink (...) (2/2)"
                            // instead)
                            new String[] {"DataSink \\(.*\\) \\(1/1\\) switched to FINISHED"},
                            RunTypes.CLI_FRONTEND,
                            0,
                            cliLoggerAuditingExtension::getMessages);
                    LOG.info("Finished perJobYarnCluster()");
                });
    }

    /**
     * Starts a session cluster on YARN, and submits a streaming job.
     *
     * <p>Tests
     *
     * <ul>
     *   <li>if a custom YARN application name can be set from the command line,
     *   <li>if the number of TaskManager slots can be set from the command line,
     *   <li>if dynamic properties from the command line are set,
     *   <li>if the vcores are set correctly (FLINK-2213),
     *   <li>if jobmanager hostname/port are shown in web interface (FLINK-1902)
     * </ul>
     *
     * <p><b>Hint: </b> If you think it is a good idea to add more assertions to this test, think
     * again!
     */
    @Test
    void
            testVCoresAreSetCorrectlyAndJobManagerHostnameAreShownInWebInterfaceAndDynamicPropertiesAndYarnApplicationNameAndTaskManagerSlots()
                    throws Exception {
        runTest(
                () -> {
                    checkForProhibitedLogContents = false;
                    final Runner yarnSessionClusterRunner =
                            startWithArgs(
                                    new String[] {
                                        "-j",
                                        flinkUberjar.getAbsolutePath(),
                                        "-t",
                                        flinkLibFolder.getAbsolutePath(),
                                        "-jm",
                                        "768m",
                                        "-tm",
                                        "1024m",
                                        "-s",
                                        "3", // set the slots 3 to check if the vCores are set
                                        // properly!
                                        "-nm",
                                        "customName",
                                        "-Dfancy-configuration-value=veryFancy",
                                        "-D" + YarnConfigOptions.VCORES.key() + "=2"
                                    },
                                    "JobManager Web Interface:",
                                    RunTypes.YARN_SESSION);

                    try {
                        final String logs = outContent.toString();
                        final HostAndPort hostAndPort = parseJobManagerHostname(logs);
                        final String host = hostAndPort.getHost();
                        final int port = hostAndPort.getPort();
                        LOG.info("Extracted hostname:port: {}:{}", host, port);

                        submitJob("WindowJoin.jar");

                        //
                        // Assert that custom YARN application name "customName" is set
                        //
                        final ApplicationReport applicationReport = getOnlyApplicationReport();
                        assertThat(applicationReport.getName()).isEqualTo("customName");

                        //
                        // Assert the number of TaskManager slots are set
                        //
                        waitForTaskManagerRegistration(host, port);
                        assertNumberOfSlotsPerTask(host, port, 3);

                        final Map<String, String> flinkConfig = getFlinkConfig(host, port);

                        //
                        // Assert dynamic properties
                        //

                        assertThat(flinkConfig)
                                .containsEntry("fancy-configuration-value", "veryFancy")
                                //
                                // FLINK-2213: assert that vcores are set
                                //
                                .containsEntry(YarnConfigOptions.VCORES.key(), "2")
                                //
                                // FLINK-1902: check if jobmanager hostname is shown in web
                                // interface
                                //
                                .containsEntry(JobManagerOptions.ADDRESS.key(), host);
                    } finally {
                        yarnSessionClusterRunner.sendStop();
                        yarnSessionClusterRunner.join();
                    }
                });
    }

    private static HostAndPort parseJobManagerHostname(final String logs) {
        final Pattern p =
                Pattern.compile("JobManager Web Interface: http://([a-zA-Z0-9.-]+):([0-9]+)");
        final Matcher matches = p.matcher(logs);
        String hostname = null;
        String port = null;

        while (matches.find()) {
            hostname = matches.group(1).toLowerCase();
            port = matches.group(2);
        }

        checkState(hostname != null, "hostname not found in log");
        checkState(port != null, "port not found in log");

        return HostAndPort.fromParts(hostname, Integer.parseInt(port));
    }

    private void submitJob(final String jobFileName) throws IOException, InterruptedException {
        Runner jobRunner =
                startWithArgs(
                        new String[] {
                            "run", "--detached", getTestJarPath(jobFileName).getAbsolutePath()
                        },
                        "Job has been submitted with JobID",
                        RunTypes.CLI_FRONTEND);
        jobRunner.join();
    }

    private static void waitForTaskManagerRegistration(final String host, final int port)
            throws Exception {
        CommonTestUtils.waitUntilCondition(() -> getNumberOfTaskManagers(host, port) > 0);
    }

    private static void assertNumberOfSlotsPerTask(
            final String host, final int port, final int slotsNumber) throws Exception {
        try {
            CommonTestUtils.waitUntilCondition(
                    () -> getNumberOfSlotsPerTaskManager(host, port) == slotsNumber);
        } catch (final TimeoutException e) {
            final int currentNumberOfSlots = getNumberOfSlotsPerTaskManager(host, port);
            fail(
                    String.format(
                            "Expected slots per TM to be %d, was: %d",
                            slotsNumber, currentNumberOfSlots));
        }
    }

    private static int getNumberOfTaskManagers(final String host, final int port) throws Exception {
        final ClusterOverviewWithVersion clusterOverviewWithVersion =
                restClient
                        .sendRequest(host, port, ClusterOverviewHeaders.getInstance())
                        .get(30_000, TimeUnit.MILLISECONDS);

        return clusterOverviewWithVersion.getNumTaskManagersConnected();
    }

    private static int getNumberOfSlotsPerTaskManager(final String host, final int port)
            throws Exception {
        final TaskManagersInfo taskManagersInfo =
                restClient.sendRequest(host, port, TaskManagersHeaders.getInstance()).get();

        return taskManagersInfo.getTaskManagerInfos().stream()
                .map(TaskManagerInfo::getNumberSlots)
                .findFirst()
                .orElse(0);
    }

    private static Map<String, String> getFlinkConfig(final String host, final int port)
            throws Exception {
        final ConfigurationInfo configurationInfoEntries =
                restClient
                        .sendRequest(host, port, ClusterConfigurationInfoHeaders.getInstance())
                        .get();

        return configurationInfoEntries.stream()
                .collect(
                        Collectors.toMap(
                                ConfigurationInfoEntry::getKey, ConfigurationInfoEntry::getValue));
    }

    /**
     * Test deployment to non-existing queue & ensure that the system logs a WARN message for the
     * user. (Users had unexpected behavior of Flink on YARN because they mistyped the target queue.
     * With an error message, we can help users identifying the issue)
     */
    @Test
    void testNonexistingQueueWARNmessage() throws Exception {
        runTest(
                () -> {
                    LOG.info("Starting testNonexistingQueueWARNmessage()");
                    assertThatThrownBy(
                                    () ->
                                            runWithArgs(
                                                    new String[] {
                                                        "-j",
                                                        flinkUberjar.getAbsolutePath(),
                                                        "-t",
                                                        flinkLibFolder.getAbsolutePath(),
                                                        "-jm",
                                                        "768m",
                                                        "-tm",
                                                        "1024m",
                                                        "-qu",
                                                        "doesntExist"
                                                    },
                                                    "to unknown queue: doesntExist",
                                                    null,
                                                    RunTypes.YARN_SESSION,
                                                    1))
                            .isInstanceOf(Exception.class)
                            .satisfies(anyCauseMatches("to unknown queue: doesntExist"));

                    assertThat(yarLoggerAuditingExtension.getMessages())
                            .anySatisfy(
                                    s ->
                                            assertThat(s)
                                                    .contains(
                                                            "The specified queue 'doesntExist' does not exist. Available queues"));
                    LOG.info("Finished testNonexistingQueueWARNmessage()");
                });
    }

    /**
     * Test per-job yarn cluster with the parallelism set at the CliFrontend instead of the YARN
     * client.
     */
    @Test
    void perJobYarnClusterWithParallelism() throws Exception {
        runTest(
                () -> {
                    LOG.info("Starting perJobYarnClusterWithParallelism()");
                    File exampleJarLocation = getTestJarPath("BatchWordCount.jar");
                    runWithArgs(
                            new String[] {
                                "run",
                                "-p",
                                "2", // test that the job is executed with a DOP of 2
                                "-m",
                                "yarn-cluster",
                                "-yj",
                                flinkUberjar.getAbsolutePath(),
                                "-yt",
                                flinkLibFolder.getAbsolutePath(),
                                "-ys",
                                "2",
                                "-yjm",
                                "768m",
                                "-ytm",
                                "1024m",
                                exampleJarLocation.getAbsolutePath()
                            },
                            /* test succeeded after this string */
                            "Program execution finished",
                            /* prohibited strings: (we want to see "DataSink (...) (2/2) switched to FINISHED") */
                            new String[] {"DataSink \\(.*\\) \\(1/1\\) switched to FINISHED"},
                            RunTypes.CLI_FRONTEND,
                            0,
                            cliLoggerAuditingExtension::getMessages);
                    LOG.info("Finished perJobYarnClusterWithParallelism()");
                });
    }

    /** Test a fire-and-forget job submission to a YARN cluster. */
    @Test
    void testDetachedPerJobYarnCluster(@TempDir File tempDir) throws Exception {
        runTest(
                () -> {
                    LOG.info("Starting testDetachedPerJobYarnCluster()");

                    File exampleJarLocation = getTestJarPath("BatchWordCount.jar");

                    testDetachedPerJobYarnClusterInternal(
                            tempDir, exampleJarLocation.getAbsolutePath());

                    LOG.info("Finished testDetachedPerJobYarnCluster()");
                });
    }

    /** Test a fire-and-forget job submission to a YARN cluster. */
    @Test
    void testDetachedPerJobYarnClusterWithStreamingJob(@TempDir File tempDir) throws Exception {
        runTest(
                () -> {
                    LOG.info("Starting testDetachedPerJobYarnClusterWithStreamingJob()");

                    File exampleJarLocation = getTestJarPath("StreamingWordCount.jar");

                    testDetachedPerJobYarnClusterInternal(
                            tempDir, exampleJarLocation.getAbsolutePath());

                    LOG.info("Finished testDetachedPerJobYarnClusterWithStreamingJob()");
                });
    }

    private void testDetachedPerJobYarnClusterInternal(File tempDir, String job) throws Exception {
        YarnClient yc = YarnClient.createYarnClient();
        yc.init(YARN_CONFIGURATION);
        yc.start();

        // get temporary file for reading input data for wordcount example
        File tmpInFile = tempDir.toPath().resolve(UUID.randomUUID().toString()).toFile();
        tmpInFile.createNewFile();
        try {
            FileUtils.writeStringToFile(tmpInFile, WordCountData.TEXT, Charset.defaultCharset());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Runner runner =
                startWithArgs(
                        new String[] {
                            "run",
                            "-m",
                            "yarn-cluster",
                            "-yj",
                            flinkUberjar.getAbsolutePath(),
                            "-yt",
                            flinkLibFolder.getAbsolutePath(),
                            "-yjm",
                            "768m",
                            "-yD",
                            YarnConfigOptions.APPLICATION_TAGS.key() + "=test-tag",
                            "-ytm",
                            "1024m",
                            "-ys",
                            "2", // test requesting slots from YARN.
                            "-p",
                            "2",
                            "--detached",
                            job,
                            "--input",
                            tmpInFile.getAbsoluteFile().toString(),
                            "--output",
                            tempDir.getAbsoluteFile().toString()
                        },
                        "Job has been submitted with JobID",
                        RunTypes.CLI_FRONTEND);

        // it should usually be 2, but on slow machines, the number varies
        assertThat(getRunningContainers()).isLessThanOrEqualTo(2);
        // give the runner some time to detach
        for (int attempt = 0; runner.isAlive() && attempt < 5; attempt++) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException ignored) {
            }
        }
        assertThat(runner.isAlive()).isFalse();
        LOG.info("CLI Frontend has returned, so the job is running");

        // find out the application id and wait until it has finished.
        try {
            List<ApplicationReport> apps =
                    getApplicationReportWithRetryOnNPE(
                            yc, EnumSet.of(YarnApplicationState.RUNNING));

            ApplicationId tmpAppId;
            if (apps.size() == 1) {
                // Better method to find the right appId. But sometimes the app is shutting down
                // very fast
                // Only one running
                tmpAppId = apps.get(0).getApplicationId();

                LOG.info("waiting for the job with appId {} to finish", tmpAppId);
                // wait until the app has finished
                while (getApplicationReportWithRetryOnNPE(
                                        yc, EnumSet.of(YarnApplicationState.RUNNING))
                                .size()
                        > 0) {
                    sleep(500);
                }
            } else {
                // get appId by finding the latest finished appid
                apps = getApplicationReportWithRetryOnNPE(yc);
                Collections.sort(
                        apps,
                        (o1, o2) -> o1.getApplicationId().compareTo(o2.getApplicationId()) * -1);
                tmpAppId = apps.get(0).getApplicationId();
                LOG.info(
                        "Selected {} as the last appId from {}",
                        tmpAppId,
                        Arrays.toString(apps.toArray()));
            }
            final ApplicationId id = tmpAppId;

            // now it has finished.
            // check the output files.
            File[] listOfOutputFiles = tempDir.listFiles();

            assertThat(listOfOutputFiles).isNotNull();
            LOG.info("The job has finished. TaskManager output files found in {}", tempDir);

            // read all output files in output folder to one output string
            StringBuilder content = new StringBuilder();
            for (File f : listOfOutputFiles) {
                if (f.isFile()) {
                    content.append(FileUtils.readFileToString(f, Charset.defaultCharset()))
                            .append("\n");
                }
            }

            // check if the heap size for the TaskManager was set correctly
            File jobmanagerLog =
                    TestUtils.findFile(
                            "..",
                            (dir, name) ->
                                    name.contains("jobmanager.log")
                                            && dir.getAbsolutePath().contains(id.toString()));
            assertThat(jobmanagerLog).isNotNull();
            content =
                    new StringBuilder(
                            FileUtils.readFileToString(jobmanagerLog, Charset.defaultCharset()));
            assertThat(content.toString())
                    .contains("Starting TaskManagers")
                    .contains(" (2/2) (attempt #0) with attempt id ");

            // make sure the detached app is really finished.
            LOG.info("Checking again that app has finished");
            ApplicationReport rep;
            do {
                sleep(500);
                rep = yc.getApplicationReport(id);
                LOG.info("Got report {}", rep);
            } while (rep.getYarnApplicationState() == YarnApplicationState.RUNNING);

            verifyApplicationTags(rep);
        } finally {

            // cleanup the yarn-properties file
            String confDirPath = System.getenv("FLINK_CONF_DIR");
            File configDirectory = new File(confDirPath);
            LOG.info(
                    "testDetachedPerJobYarnClusterInternal: Using configuration directory "
                            + configDirectory.getAbsolutePath());

            // load the configuration
            LOG.info("testDetachedPerJobYarnClusterInternal: Trying to load configuration file");
            Configuration configuration =
                    GlobalConfiguration.loadConfiguration(configDirectory.getAbsolutePath());

            try {
                File yarnPropertiesFile =
                        FlinkYarnSessionCli.getYarnPropertiesLocation(
                                configuration.getValue(YarnConfigOptions.PROPERTIES_FILE_LOCATION));
                if (yarnPropertiesFile.exists()) {
                    LOG.info(
                            "testDetachedPerJobYarnClusterInternal: Cleaning up temporary Yarn address reference: {}",
                            yarnPropertiesFile.getAbsolutePath());
                    yarnPropertiesFile.delete();
                }
            } catch (Exception e) {
                LOG.warn(
                        "testDetachedPerJobYarnClusterInternal: Exception while deleting the JobManager address file",
                        e);
            }

            try {
                LOG.info("testDetachedPerJobYarnClusterInternal: Closing the yarn client");
                yc.stop();
            } catch (Exception e) {
                LOG.warn(
                        "testDetachedPerJobYarnClusterInternal: Exception while close the yarn client",
                        e);
            }
        }
    }

    /**
     * Ensures that the YARN application tags were set properly.
     *
     * <p>Since YARN application tags were only added in Hadoop 2.4, but Flink still supports Hadoop
     * 2.3, reflection is required to invoke the methods. If the method does not exist, this test
     * passes.
     */
    private void verifyApplicationTags(final ApplicationReport report)
            throws InvocationTargetException, IllegalAccessException {

        final Method applicationTagsMethod;

        Class<ApplicationReport> clazz = ApplicationReport.class;
        try {
            // this method is only supported by Hadoop 2.4.0 onwards
            applicationTagsMethod = clazz.getMethod("getApplicationTags");
        } catch (NoSuchMethodException e) {
            // only verify the tags if the method exists
            return;
        }

        @SuppressWarnings("unchecked")
        Set<String> applicationTags = (Set<String>) applicationTagsMethod.invoke(report);

        assertThat(applicationTags).containsOnly("test-tag");
    }

    @AfterEach
    void checkForProhibitedLogContents() {
        if (checkForProhibitedLogContents) {
            ensureNoProhibitedStringInLogFiles(PROHIBITED_STRINGS, WHITELISTED_STRINGS);
        }
    }
}
