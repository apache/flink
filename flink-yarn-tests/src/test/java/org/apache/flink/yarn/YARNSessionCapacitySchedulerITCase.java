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

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.client.cli.CliFrontend;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.rest.RestClient;
import org.apache.flink.runtime.rest.handler.legacy.messages.ClusterOverviewWithVersion;
import org.apache.flink.runtime.rest.messages.ClusterConfigurationInfo;
import org.apache.flink.runtime.rest.messages.ClusterConfigurationInfoEntry;
import org.apache.flink.runtime.rest.messages.ClusterConfigurationInfoHeaders;
import org.apache.flink.runtime.rest.messages.ClusterOverviewHeaders;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerInfo;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagersHeaders;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagersInfo;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.test.testdata.WordCountData;
import org.apache.flink.testutils.logging.TestLoggerResource;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.yarn.cli.FlinkYarnSessionCli;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.util.TestUtils;

import org.apache.flink.shaded.guava30.com.google.common.net.HostAndPort;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static junit.framework.TestCase.assertTrue;
import static org.apache.flink.util.Preconditions.checkState;
import static org.apache.flink.yarn.util.TestUtils.getTestJarPath;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * This test starts a MiniYARNCluster with a CapacityScheduler. Is has, by default a queue called
 * "default". The configuration here adds another queue: "qa-team".
 */
public class YARNSessionCapacitySchedulerITCase extends YarnTestBase {
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

    @Rule
    public final TestLoggerResource cliTestLoggerResource =
            new TestLoggerResource(CliFrontend.class, Level.INFO);

    @Rule
    public final TestLoggerResource yarTestLoggerResource =
            new TestLoggerResource(YarnClusterDescriptor.class, Level.WARN);

    @BeforeClass
    public static void setup() throws Exception {
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

    @AfterClass
    public static void tearDown() throws Exception {
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
    public void testStartYarnSessionClusterInQaTeamQueue() throws Exception {
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
    public void perJobYarnCluster() throws Exception {
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
                            cliTestLoggerResource::getMessages);
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
    public void perJobYarnClusterOffHeap() throws Exception {
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
                            cliTestLoggerResource::getMessages);
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
    public void
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
                        assertEquals("customName", applicationReport.getName());

                        //
                        // Assert the number of TaskManager slots are set
                        //
                        waitForTaskManagerRegistration(host, port, Duration.ofMillis(30_000));
                        assertNumberOfSlotsPerTask(host, port, 3);

                        final Map<String, String> flinkConfig = getFlinkConfig(host, port);

                        //
                        // Assert dynamic properties
                        //
                        assertThat(flinkConfig, hasEntry("fancy-configuration-value", "veryFancy"));

                        //
                        // FLINK-2213: assert that vcores are set
                        //
                        assertThat(flinkConfig, hasEntry(YarnConfigOptions.VCORES.key(), "2"));

                        //
                        // FLINK-1902: check if jobmanager hostname is shown in web interface
                        //
                        assertThat(flinkConfig, hasEntry(JobManagerOptions.ADDRESS.key(), host));
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

    private static void waitForTaskManagerRegistration(
            final String host, final int port, final Duration waitDuration) throws Exception {
        CommonTestUtils.waitUntilCondition(
                () -> getNumberOfTaskManagers(host, port) > 0, Deadline.fromNow(waitDuration));
    }

    private static void assertNumberOfSlotsPerTask(
            final String host, final int port, final int slotsNumber) throws Exception {
        try {
            CommonTestUtils.waitUntilCondition(
                    () -> getNumberOfSlotsPerTaskManager(host, port) == slotsNumber,
                    Deadline.fromNow(Duration.ofSeconds(30)));
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
        final ClusterConfigurationInfo clusterConfigurationInfoEntries =
                restClient
                        .sendRequest(host, port, ClusterConfigurationInfoHeaders.getInstance())
                        .get();

        return clusterConfigurationInfoEntries.stream()
                .collect(
                        Collectors.toMap(
                                ClusterConfigurationInfoEntry::getKey,
                                ClusterConfigurationInfoEntry::getValue));
    }

    /**
     * Test deployment to non-existing queue & ensure that the system logs a WARN message for the
     * user. (Users had unexpected behavior of Flink on YARN because they mistyped the target queue.
     * With an error message, we can help users identifying the issue)
     */
    @Test
    public void testNonexistingQueueWARNmessage() throws Exception {
        runTest(
                () -> {
                    LOG.info("Starting testNonexistingQueueWARNmessage()");
                    try {
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
                                1);
                    } catch (Exception e) {
                        assertTrue(
                                ExceptionUtils.findThrowableWithMessage(
                                                e, "to unknown queue: doesntExist")
                                        .isPresent());
                    }
                    assertThat(
                            yarTestLoggerResource.getMessages(),
                            hasItem(
                                    containsString(
                                            "The specified queue 'doesntExist' does not exist. Available queues")));
                    LOG.info("Finished testNonexistingQueueWARNmessage()");
                });
    }

    /**
     * Test per-job yarn cluster with the parallelism set at the CliFrontend instead of the YARN
     * client.
     */
    @Test
    public void perJobYarnClusterWithParallelism() throws Exception {
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
                            cliTestLoggerResource::getMessages);
                    LOG.info("Finished perJobYarnClusterWithParallelism()");
                });
    }

    /** Test a fire-and-forget job submission to a YARN cluster. */
    @Test(timeout = 60000)
    public void testDetachedPerJobYarnCluster() throws Exception {
        runTest(
                () -> {
                    LOG.info("Starting testDetachedPerJobYarnCluster()");

                    File exampleJarLocation = getTestJarPath("BatchWordCount.jar");

                    testDetachedPerJobYarnClusterInternal(exampleJarLocation.getAbsolutePath());

                    LOG.info("Finished testDetachedPerJobYarnCluster()");
                });
    }

    /** Test a fire-and-forget job submission to a YARN cluster. */
    @Test(timeout = 60000)
    public void testDetachedPerJobYarnClusterWithStreamingJob() throws Exception {
        runTest(
                () -> {
                    LOG.info("Starting testDetachedPerJobYarnClusterWithStreamingJob()");

                    File exampleJarLocation = getTestJarPath("StreamingWordCount.jar");

                    testDetachedPerJobYarnClusterInternal(exampleJarLocation.getAbsolutePath());

                    LOG.info("Finished testDetachedPerJobYarnClusterWithStreamingJob()");
                });
    }

    private void testDetachedPerJobYarnClusterInternal(String job) throws Exception {
        YarnClient yc = YarnClient.createYarnClient();
        yc.init(YARN_CONFIGURATION);
        yc.start();

        // get temporary folder for writing output of wordcount example
        File tmpOutFolder = null;
        try {
            tmpOutFolder = tmp.newFolder();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // get temporary file for reading input data for wordcount example
        File tmpInFile;
        try {
            tmpInFile = tmp.newFile();
            FileUtils.writeStringToFile(tmpInFile, WordCountData.TEXT);
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
                            tmpOutFolder.getAbsoluteFile().toString()
                        },
                        "Job has been submitted with JobID",
                        RunTypes.CLI_FRONTEND);

        // it should usually be 2, but on slow machines, the number varies
        Assert.assertTrue(
                "There should be at most 2 containers running", getRunningContainers() <= 2);
        // give the runner some time to detach
        for (int attempt = 0; runner.isAlive() && attempt < 5; attempt++) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
            }
        }
        Assert.assertFalse("The runner should detach.", runner.isAlive());
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
                        new Comparator<ApplicationReport>() {
                            @Override
                            public int compare(ApplicationReport o1, ApplicationReport o2) {
                                return o1.getApplicationId().compareTo(o2.getApplicationId()) * -1;
                            }
                        });
                tmpAppId = apps.get(0).getApplicationId();
                LOG.info(
                        "Selected {} as the last appId from {}",
                        tmpAppId,
                        Arrays.toString(apps.toArray()));
            }
            final ApplicationId id = tmpAppId;

            // now it has finished.
            // check the output files.
            File[] listOfOutputFiles = tmpOutFolder.listFiles();

            Assert.assertNotNull("Taskmanager output not found", listOfOutputFiles);
            LOG.info("The job has finished. TaskManager output files found in {}", tmpOutFolder);

            // read all output files in output folder to one output string
            String content = "";
            for (File f : listOfOutputFiles) {
                if (f.isFile()) {
                    content += FileUtils.readFileToString(f) + "\n";
                }
            }
            // String content = FileUtils.readFileToString(taskmanagerOut);
            // check for some of the wordcount outputs.
            Assert.assertTrue(
                    "Expected string 'da 5' or '(all,2)' not found in string '" + content + "'",
                    content.contains("da 5")
                            || content.contains("(da,5)")
                            || content.contains("(all,2)"));
            Assert.assertTrue(
                    "Expected string 'der 29' or '(mind,1)' not found in string'" + content + "'",
                    content.contains("der 29")
                            || content.contains("(der,29)")
                            || content.contains("(mind,1)"));

            // check if the heap size for the TaskManager was set correctly
            File jobmanagerLog =
                    TestUtils.findFile(
                            "..",
                            new FilenameFilter() {
                                @Override
                                public boolean accept(File dir, String name) {
                                    return name.contains("jobmanager.log")
                                            && dir.getAbsolutePath().contains(id.toString());
                                }
                            });
            Assert.assertNotNull("Unable to locate JobManager log", jobmanagerLog);
            content = FileUtils.readFileToString(jobmanagerLog);
            String expected = "Starting TaskManagers";
            Assert.assertTrue(
                    "Expected string '"
                            + expected
                            + "' not found in JobManager log: '"
                            + jobmanagerLog
                            + "'",
                    content.contains(expected));
            expected = " (2/2) (attempt #0) with attempt id ";
            Assert.assertTrue(
                    "Expected string '"
                            + expected
                            + "' not found in JobManager log."
                            + "This string checks that the job has been started with a parallelism of 2. Log contents: '"
                            + jobmanagerLog
                            + "'",
                    content.contains(expected));

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

        assertEquals(Collections.singleton("test-tag"), applicationTags);
    }

    @After
    public void checkForProhibitedLogContents() {
        if (checkForProhibitedLogContents) {
            ensureNoProhibitedStringInLogFiles(PROHIBITED_STRINGS, WHITELISTED_STRINGS);
        }
    }
}
