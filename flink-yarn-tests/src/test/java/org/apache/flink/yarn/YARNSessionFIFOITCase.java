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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.test.testdata.WordCountData;
import org.apache.flink.testutils.logging.LoggerAuditingExtension;
import org.apache.flink.yarn.cli.FlinkYarnSessionCli;
import org.apache.flink.yarn.configuration.YarnConfigOptions;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.io.File;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.flink.yarn.util.TestUtils.getTestJarPath;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * This test starts a MiniYARNCluster with a FIFO scheduler. There are no queues for that scheduler.
 */
class YARNSessionFIFOITCase extends YarnTestBase {
    private static final Logger log = LoggerFactory.getLogger(YARNSessionFIFOITCase.class);

    protected static final String VIEW_ACLS = "user group";
    protected static final String MODIFY_ACLS = "admin groupAdmin";
    protected static final String VIEW_ACLS_WITH_WILDCARD = "*";
    protected static final String MODIFY_ACLS_WITH_WILDCARD = "*";
    protected static final String WILDCARD = "*";

    @RegisterExtension
    protected final LoggerAuditingExtension yarLoggerAuditingExtension =
            new LoggerAuditingExtension(YarnClusterDescriptor.class, Level.WARN);

    /** Override init with FIFO scheduler. */
    @BeforeAll
    public static void setup() {
        YARN_CONFIGURATION.setClass(
                YarnConfiguration.RM_SCHEDULER, FifoScheduler.class, ResourceScheduler.class);
        YARN_CONFIGURATION.setInt(YarnConfiguration.NM_PMEM_MB, 768);
        YARN_CONFIGURATION.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 512);
        YARN_CONFIGURATION.set(YarnTestBase.TEST_CLUSTER_NAME_KEY, "flink-yarn-tests-fifo");
        startYARNWithConfig(YARN_CONFIGURATION);
    }

    @AfterEach
    void checkForProhibitedLogContents() {
        ensureNoProhibitedStringInLogFiles(PROHIBITED_STRINGS, WHITELISTED_STRINGS);
    }

    @Test
    void testDetachedMode() throws Exception {
        runTest(() -> runDetachedModeTest(Collections.emptyMap(), VIEW_ACLS, MODIFY_ACLS));
    }

    /** Test regular operation, including command line parameter parsing. */
    ApplicationId runDetachedModeTest(
            Map<String, String> securityProperties, String viewAcls, String modifyAcls)
            throws Exception {
        log.info("Starting testDetachedMode()");

        File exampleJarLocation = getTestJarPath("StreamingWordCount.jar");
        // get temporary file for reading input data for wordcount example
        File tmpInFile = tmp.toPath().resolve(UUID.randomUUID().toString()).toFile();
        tmpInFile.createNewFile();
        FileUtils.writeStringToFile(tmpInFile, WordCountData.TEXT, Charset.defaultCharset());

        ArrayList<String> args = new ArrayList<>();
        args.add("-j");
        args.add(flinkUberjar.getAbsolutePath());

        args.add("-t");
        args.add(flinkLibFolder.getAbsolutePath());

        args.add("-jm");
        args.add("768m");

        args.add("-tm");
        args.add("1024m");

        if (securityProperties != null) {
            for (Map.Entry<String, String> property : securityProperties.entrySet()) {
                args.add("-D" + property.getKey() + "=" + property.getValue());
            }
        }

        args.add("-D" + YarnConfigOptions.APPLICATION_VIEW_ACLS.key() + "=" + viewAcls);
        args.add("-D" + YarnConfigOptions.APPLICATION_MODIFY_ACLS.key() + "=" + modifyAcls);

        args.add("--name");
        args.add("MyCustomName");

        args.add("--applicationType");
        args.add("Apache Flink 1.x");

        args.add("--detached");

        Runner clusterRunner =
                startWithArgs(
                        args.toArray(new String[args.size()]),
                        "JobManager Web Interface:",
                        RunTypes.YARN_SESSION);

        // before checking any strings outputted by the CLI, first give it time to
        // return
        clusterRunner.join();

        // actually run a program, otherwise we wouldn't necessarily see any
        // TaskManagers
        // be brought up
        Runner jobRunner =
                startWithArgs(
                        new String[] {
                            "run",
                            "--detached",
                            exampleJarLocation.getAbsolutePath(),
                            "--input",
                            tmpInFile.getAbsoluteFile().toString()
                        },
                        "Job has been submitted with JobID",
                        RunTypes.CLI_FRONTEND);

        jobRunner.join();

        final long testConditionIntervalInMillis = 500;
        // in "new" mode we can only wait after the job is submitted, because TMs
        // are spun up lazily
        // wait until two containers are running
        log.info("Waiting until two containers are running");
        CommonTestUtils.waitUntilCondition(
                () -> getRunningContainers() >= 2, testConditionIntervalInMillis);

        log.info("Waiting until the job reaches FINISHED state");
        final ApplicationId applicationId = getOnlyApplicationReport().getApplicationId();
        CommonTestUtils.waitUntilCondition(
                () ->
                        verifyStringsInNamedLogFiles(
                                new String[] {"switched from state RUNNING to FINISHED"},
                                applicationId,
                                "jobmanager.log"),
                testConditionIntervalInMillis);

        // kill application "externally".
        try {
            YarnClient yc = YarnClient.createYarnClient();
            yc.init(YARN_CONFIGURATION);
            yc.start();
            List<ApplicationReport> apps =
                    getApplicationReportWithRetryOnNPE(
                            yc, EnumSet.of(YarnApplicationState.RUNNING));
            assertThat(apps).hasSize(1); // Only one running
            ApplicationReport app = apps.get(0);

            assertThat(app.getName()).isEqualTo("MyCustomName");
            assertThat(app.getApplicationType()).isEqualTo("Apache Flink 1.x");
            ApplicationId id = app.getApplicationId();
            yc.killApplication(id);

            CommonTestUtils.waitUntilCondition(
                    () ->
                            !getApplicationReportWithRetryOnNPE(
                                            yc,
                                            EnumSet.of(
                                                    YarnApplicationState.KILLED,
                                                    YarnApplicationState.FINISHED))
                                    .isEmpty(),
                    testConditionIntervalInMillis);
        } catch (Throwable t) {
            log.warn("Killing failed", t);
        } finally {

            // cleanup the yarn-properties file
            String confDirPath = System.getenv("FLINK_CONF_DIR");
            File configDirectory = new File(confDirPath);
            log.info(
                    "testDetachedPerJobYarnClusterInternal: Using configuration directory "
                            + configDirectory.getAbsolutePath());

            // load the configuration
            log.info("testDetachedPerJobYarnClusterInternal: Trying to load configuration file");
            Configuration configuration =
                    GlobalConfiguration.loadConfiguration(configDirectory.getAbsolutePath());

            try {
                File yarnPropertiesFile =
                        FlinkYarnSessionCli.getYarnPropertiesLocation(
                                configuration.getString(
                                        YarnConfigOptions.PROPERTIES_FILE_LOCATION));
                if (yarnPropertiesFile.exists()) {
                    log.info(
                            "testDetachedPerJobYarnClusterInternal: Cleaning up temporary Yarn address reference: {}",
                            yarnPropertiesFile.getAbsolutePath());
                    yarnPropertiesFile.delete();
                }
            } catch (Exception e) {
                log.warn(
                        "testDetachedPerJobYarnClusterInternal: Exception while deleting the JobManager address file",
                        e);
            }
        }

        log.info("Finished testDetachedMode()");
        return applicationId;
    }

    /**
     * Test querying the YARN cluster.
     *
     * <p>This test validates through 666*2 cores in the "cluster".
     */
    @Test
    void testQueryCluster() throws Exception {
        runTest(
                () -> {
                    log.info("Starting testQueryCluster()");
                    runWithArgs(
                            new String[] {"-q"},
                            "Summary: totalMemory 8.000gb (8589934592 bytes) totalCores 1332",
                            null,
                            RunTypes.YARN_SESSION,
                            0); // we have 666*2 cores.
                    log.info("Finished testQueryCluster()");
                });
    }
}
