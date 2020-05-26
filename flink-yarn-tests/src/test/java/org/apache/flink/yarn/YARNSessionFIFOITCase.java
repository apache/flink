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
import org.apache.flink.test.testdata.WordCountData;
import org.apache.flink.testutils.logging.TestLoggerResource;
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
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.yarn.util.TestUtils.getTestJarPath;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertThat;

/**
 * This test starts a MiniYARNCluster with a FIFO scheduler.
 * There are no queues for that scheduler.
 */
public class YARNSessionFIFOITCase extends YarnTestBase {
	private static final Logger LOG = LoggerFactory.getLogger(YARNSessionFIFOITCase.class);

	@Rule
	public final TestLoggerResource yarTestLoggerResource = new TestLoggerResource(YarnClusterDescriptor.class, Level.WARN);

	/*
	Override init with FIFO scheduler.
	 */
	@BeforeClass
	public static void setup() {
		YARN_CONFIGURATION.setClass(YarnConfiguration.RM_SCHEDULER, FifoScheduler.class, ResourceScheduler.class);
		YARN_CONFIGURATION.setInt(YarnConfiguration.NM_PMEM_MB, 768);
		YARN_CONFIGURATION.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 512);
		YARN_CONFIGURATION.set(YarnTestBase.TEST_CLUSTER_NAME_KEY, "flink-yarn-tests-fifo");
		startYARNWithConfig(YARN_CONFIGURATION);
	}

	@After
	public void checkForProhibitedLogContents() {
		ensureNoProhibitedStringInLogFiles(PROHIBITED_STRINGS, WHITELISTED_STRINGS);
	}

	@Test(timeout = 60000)
	public void testDetachedMode() throws Exception {
		runTest(() -> runDetachedModeTest(Collections.emptyMap()));
	}

	/**
	 * Test regular operation, including command line parameter parsing.
	 */
	void runDetachedModeTest(Map<String, String> securityProperties) throws Exception {
		runTest(() -> {
			LOG.info("Starting testDetachedMode()");

			File exampleJarLocation = getTestJarPath("StreamingWordCount.jar");
			// get temporary file for reading input data for wordcount example
			File tmpInFile = tmp.newFile();
			FileUtils.writeStringToFile(tmpInFile, WordCountData.TEXT);

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

			args.add("--name");
			args.add("MyCustomName");

			args.add("--applicationType");
			args.add("Apache Flink 1.x");

			args.add("--detached");

			Runner clusterRunner =
				startWithArgs(
					args.toArray(new String[args.size()]),
					"JobManager Web Interface:", RunTypes.YARN_SESSION);

			// before checking any strings outputted by the CLI, first give it time to return
			clusterRunner.join();

			// actually run a program, otherwise we wouldn't necessarily see any TaskManagers
			// be brought up
			Runner jobRunner = startWithArgs(new String[]{"run",
					"--detached", exampleJarLocation.getAbsolutePath(),
					"--input", tmpInFile.getAbsoluteFile().toString()},
				"Job has been submitted with JobID", RunTypes.CLI_FRONTEND);

			jobRunner.join();

			// in "new" mode we can only wait after the job is submitted, because TMs
			// are spun up lazily
			LOG.info("Waiting until two containers are running");
			// wait until two containers are running
			while (getRunningContainers() < 2) {
				sleep(500);
			}

			// make sure we have two TMs running in either mode
			long startTime = System.nanoTime();
			while (System.nanoTime() - startTime < TimeUnit.NANOSECONDS.convert(10, TimeUnit.SECONDS) &&
				!(verifyStringsInNamedLogFiles(
					new String[]{"switched from state RUNNING to FINISHED"}, "jobmanager.log"))) {
				LOG.info("Still waiting for cluster to finish job...");
				sleep(500);
			}

			LOG.info("Two containers are running. Killing the application");

			// kill application "externally".
			try {
				YarnClient yc = YarnClient.createYarnClient();
				yc.init(YARN_CONFIGURATION);
				yc.start();
				List<ApplicationReport> apps = yc.getApplications(EnumSet.of(YarnApplicationState.RUNNING));
				Assert.assertEquals(1, apps.size()); // Only one running
				ApplicationReport app = apps.get(0);

				Assert.assertEquals("MyCustomName", app.getName());
				Assert.assertEquals("Apache Flink 1.x", app.getApplicationType());
				ApplicationId id = app.getApplicationId();
				yc.killApplication(id);

				while (yc.getApplications(EnumSet.of(YarnApplicationState.KILLED)).size() == 0 &&
					yc.getApplications(EnumSet.of(YarnApplicationState.FINISHED)).size() == 0) {
					sleep(500);
				}
			} catch (Throwable t) {
				LOG.warn("Killing failed", t);
				Assert.fail();
			} finally {

				//cleanup the yarn-properties file
				String confDirPath = System.getenv("FLINK_CONF_DIR");
				File configDirectory = new File(confDirPath);
				LOG.info("testDetachedPerJobYarnClusterInternal: Using configuration directory " + configDirectory.getAbsolutePath());

				// load the configuration
				LOG.info("testDetachedPerJobYarnClusterInternal: Trying to load configuration file");
				Configuration configuration = GlobalConfiguration.loadConfiguration(configDirectory.getAbsolutePath());

				try {
					File yarnPropertiesFile = FlinkYarnSessionCli.getYarnPropertiesLocation(configuration.getString(YarnConfigOptions.PROPERTIES_FILE_LOCATION));
					if (yarnPropertiesFile.exists()) {
						LOG.info("testDetachedPerJobYarnClusterInternal: Cleaning up temporary Yarn address reference: {}", yarnPropertiesFile.getAbsolutePath());
						yarnPropertiesFile.delete();
					}
				} catch (Exception e) {
					LOG.warn("testDetachedPerJobYarnClusterInternal: Exception while deleting the JobManager address file", e);
				}
			}

			LOG.info("Finished testDetachedMode()");
		});
	}

	/**
	 * Test querying the YARN cluster.
	 *
	 * <p>This test validates through 666*2 cores in the "cluster".
	 */
	@Test
	public void testQueryCluster() throws Exception {
		runTest(() -> {
			LOG.info("Starting testQueryCluster()");
			runWithArgs(new String[]{"-q"}, "Summary: totalMemory 8192 totalCores 1332", null, RunTypes.YARN_SESSION, 0); // we have 666*2 cores.
			LOG.info("Finished testQueryCluster()");
		});
	}

	/**
	 * The test cluster has the following resources:
	 * - 2 Nodes with 4096 MB each.
	 * - RM_SCHEDULER_MINIMUM_ALLOCATION_MB is 512
	 *
	 * <p>We allocate:
	 * 1 JobManager with 256 MB (will be automatically upgraded to 512 due to min alloc mb)
	 * 5 TaskManagers with 1585 MB
	 *
	 * <p>user sees a total request of: 8181 MB (fits)
	 * system sees a total request of: 8437 (doesn't fit due to min alloc mb)
	 */
	@Ignore("The test is too resource consuming (8.5 GB of memory)")
	@Test
	public void testResourceComputation() throws Exception {
		runTest(() -> {
			LOG.info("Starting testResourceComputation()");
			runWithArgs(new String[]{
				"-j", flinkUberjar.getAbsolutePath(),
				"-t", flinkLibFolder.getAbsolutePath(),
				"-jm", "256m",
				"-tm", "1585m"}, "Number of connected TaskManagers changed to", null, RunTypes.YARN_SESSION, 0);
			LOG.info("Finished testResourceComputation()");
			assertThat(
				yarTestLoggerResource.getMessages(),
				hasItem(containsString("This YARN session requires 8437MB of memory in the cluster. There are currently only 8192MB available.")));
		});
	}

	/**
	 * The test cluster has the following resources:
	 * - 2 Nodes with 4096 MB each.
	 * - RM_SCHEDULER_MINIMUM_ALLOCATION_MB is 512
	 *
	 * <p>We allocate:
	 * 1 JobManager with 256 MB (will be automatically upgraded to 512 due to min alloc mb)
	 * 2 TaskManagers with 3840 MB
	 *
	 * <p>the user sees a total request of: 7936 MB (fits)
	 * the system sees a request of: 8192 MB (fits)
	 * HOWEVER: one machine is going to need 3840 + 512 = 4352 MB, which doesn't fit.
	 *
	 * <p>--> check if the system properly rejects allocating this session.
	 */
	@Ignore("The test is too resource consuming (8 GB of memory)")
	@Test
	public void testfullAlloc() throws Exception {
		runTest(() -> {
			LOG.info("Starting testfullAlloc()");
			runWithArgs(new String[]{
				"-j", flinkUberjar.getAbsolutePath(),
				"-t", flinkLibFolder.getAbsolutePath(),
				"-jm", "256m",
				"-tm", "3840m"}, "Number of connected TaskManagers changed to", null, RunTypes.YARN_SESSION, 0);
			LOG.info("Finished testfullAlloc()");
			assertThat(
				yarTestLoggerResource.getMessages(),
				hasItem(containsString("There is not enough memory available in the YARN cluster. The TaskManager(s) require 3840MB each. NodeManagers available: [4096, 4096]\n" +
					"After allocating the JobManager (512MB) and (1/2) TaskManagers, the following NodeManagers are available: [3584, 256]")));
		});
	}
}
