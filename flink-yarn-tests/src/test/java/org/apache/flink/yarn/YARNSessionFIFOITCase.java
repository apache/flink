/**
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

import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.runtime.clusterframework.messages.GetClusterStatusResponse;
import org.apache.flink.test.testdata.WordCountData;
import org.apache.flink.test.util.SecureTestEnvironment;
import org.apache.flink.yarn.cli.FlinkYarnSessionCli;
import org.apache.flink.yarn.configuration.YarnConfigOptions;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.yarn.UtilsTest.addTestAppender;
import static org.apache.flink.yarn.UtilsTest.checkForLogString;
import static org.apache.flink.yarn.util.YarnTestUtils.getTestJarPath;

/**
 * This test starts a MiniYARNCluster with a FIFO scheduler.
 * There are no queues for that scheduler.
 */
public class YARNSessionFIFOITCase extends YarnTestBase {
	private static final Logger LOG = LoggerFactory.getLogger(YARNSessionFIFOITCase.class);

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

	/**
	 * Test regular operation, including command line parameter parsing.
	 */
	@Test(timeout = 60000) // timeout after a minute.
	public void testDetachedMode() throws InterruptedException, IOException {
		LOG.info("Starting testDetachedMode()");
		addTestAppender(FlinkYarnSessionCli.class, Level.INFO);

		File exampleJarLocation = getTestJarPath("StreamingWordCount.jar");
		// get temporary file for reading input data for wordcount example
		File tmpInFile = tmp.newFile();
		FileUtils.writeStringToFile(tmpInFile, WordCountData.TEXT);

		ArrayList<String> args = new ArrayList<>();
		args.add("-j");
		args.add(flinkUberjar.getAbsolutePath());

		args.add("-t");
		args.add(flinkLibFolder.getAbsolutePath());

		args.add("-n");
		args.add("1");

		args.add("-jm");
		args.add("768m");

		args.add("-tm");
		args.add("1024m");

		if (SecureTestEnvironment.getTestKeytab() != null) {
			args.add("-D" + SecurityOptions.KERBEROS_LOGIN_KEYTAB.key() + "=" + SecureTestEnvironment.getTestKeytab());
		}
		if (SecureTestEnvironment.getHadoopServicePrincipal() != null) {
			args.add("-D" + SecurityOptions.KERBEROS_LOGIN_PRINCIPAL.key() + "=" + SecureTestEnvironment.getHadoopServicePrincipal());
		}
		args.add("--name");
		args.add("MyCustomName");

		args.add("--detached");

		Runner clusterRunner =
			startWithArgs(
				args.toArray(new String[args.size()]),
				"Flink JobManager is now running on", RunTypes.YARN_SESSION);

		// before checking any strings outputted by the CLI, first give it time to return
		clusterRunner.join();

		if (!isNewMode) {
			checkForLogString("The Flink YARN client has been started in detached mode");

			// in legacy mode we have to wait until the TMs are up until we can submit the job
			LOG.info("Waiting until two containers are running");
			// wait until two containers are running
			while (getRunningContainers() < 2) {
				sleep(500);
			}

			// additional sleep for the JM/TM to start and establish connection
			long startTime = System.nanoTime();
			while (System.nanoTime() - startTime < TimeUnit.NANOSECONDS.convert(10, TimeUnit.SECONDS) &&
				!(verifyStringsInNamedLogFiles(
					new String[]{"YARN Application Master started"}, "jobmanager.log") &&
					verifyStringsInNamedLogFiles(
						new String[]{"Starting TaskManager actor"}, "taskmanager.log"))) {
				LOG.info("Still waiting for JM/TM to initialize...");
				sleep(500);
			}
		}

		// actually run a program, otherwise we wouldn't necessarily see any TaskManagers
		// be brought up
		Runner jobRunner = startWithArgs(new String[]{"run",
				"--detached", exampleJarLocation.getAbsolutePath(),
				"--input", tmpInFile.getAbsoluteFile().toString()},
			"Job has been submitted with JobID", RunTypes.CLI_FRONTEND);

		jobRunner.join();

		if (isNewMode) {
			// in "new" mode we can only wait after the job is submitted, because TMs
			// are spun up lazily
			LOG.info("Waiting until two containers are running");
			// wait until two containers are running
			while (getRunningContainers() < 2) {
				sleep(500);
			}
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
	}

	/**
	 * Test querying the YARN cluster.
	 *
	 * <p>This test validates through 666*2 cores in the "cluster".
	 */
	@Test
	public void testQueryCluster() throws IOException {
		LOG.info("Starting testQueryCluster()");
		runWithArgs(new String[] {"-q"}, "Summary: totalMemory 8192 totalCores 1332", null, RunTypes.YARN_SESSION, 0); // we have 666*2 cores.
		LOG.info("Finished testQueryCluster()");
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
	public void testResourceComputation() throws IOException {
		addTestAppender(AbstractYarnClusterDescriptor.class, Level.WARN);
		LOG.info("Starting testResourceComputation()");
		runWithArgs(new String[]{"-j", flinkUberjar.getAbsolutePath(), "-t", flinkLibFolder.getAbsolutePath(),
				"-n", "5",
				"-jm", "256m",
				"-tm", "1585m"}, "Number of connected TaskManagers changed to", null, RunTypes.YARN_SESSION, 0);
		LOG.info("Finished testResourceComputation()");
		checkForLogString("This YARN session requires 8437MB of memory in the cluster. There are currently only 8192MB available.");
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
	public void testfullAlloc() throws IOException {
		addTestAppender(AbstractYarnClusterDescriptor.class, Level.WARN);
		LOG.info("Starting testfullAlloc()");
		runWithArgs(new String[]{"-j", flinkUberjar.getAbsolutePath(), "-t", flinkLibFolder.getAbsolutePath(),
				"-n", "2",
				"-jm", "256m",
				"-tm", "3840m"}, "Number of connected TaskManagers changed to", null, RunTypes.YARN_SESSION, 0);
		LOG.info("Finished testfullAlloc()");
		checkForLogString("There is not enough memory available in the YARN cluster. The TaskManager(s) require 3840MB each. NodeManagers available: [4096, 4096]\n" +
				"After allocating the JobManager (512MB) and (1/2) TaskManagers, the following NodeManagers are available: [3584, 256]");
	}

	/**
	 * Test the YARN Java API.
	 */
	@Test
	public void testJavaAPI() throws Exception {
		final int waitTime = 15;
		LOG.info("Starting testJavaAPI()");

		String confDirPath = System.getenv(ConfigConstants.ENV_FLINK_CONF_DIR);
		Configuration configuration = GlobalConfiguration.loadConfiguration();

		try (final AbstractYarnClusterDescriptor clusterDescriptor = new LegacyYarnClusterDescriptor(
			configuration,
			getYarnConfiguration(),
			confDirPath,
			getYarnClient(),
			true)) {
			Assert.assertNotNull("unable to get yarn client", clusterDescriptor);
			clusterDescriptor.setLocalJarPath(new Path(flinkUberjar.getAbsolutePath()));
			clusterDescriptor.addShipFiles(Arrays.asList(flinkLibFolder.listFiles()));

			final ClusterSpecification clusterSpecification = new ClusterSpecification.ClusterSpecificationBuilder()
				.setMasterMemoryMB(768)
				.setTaskManagerMemoryMB(1024)
				.setNumberTaskManagers(1)
				.setSlotsPerTaskManager(1)
				.createClusterSpecification();
			// deploy
			ClusterClient<ApplicationId> yarnClusterClient = null;
			try {
				yarnClusterClient = clusterDescriptor.deploySessionCluster(clusterSpecification);

				GetClusterStatusResponse expectedStatus = new GetClusterStatusResponse(1, 1);
				for (int second = 0; second < waitTime * 2; second++) { // run "forever"
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						LOG.warn("Interrupted", e);
					}
					GetClusterStatusResponse status = yarnClusterClient.getClusterStatus();
					if (status != null && status.equals(expectedStatus)) {
						LOG.info("ClusterClient reached status " + status);
						break; // all good, cluster started
					}
					if (second > waitTime) {
						// we waited for 15 seconds. cluster didn't come up correctly
						Assert.fail("The custer didn't start after " + waitTime + " seconds");
					}
				}

				// use the cluster
				Assert.assertNotNull(yarnClusterClient.getClusterConnectionInfo());
				Assert.assertNotNull(yarnClusterClient.getWebInterfaceURL());
				LOG.info("All tests passed.");
			} finally {
				if (yarnClusterClient != null) {
					// shutdown cluster
					LOG.info("Shutting down the Flink Yarn application.");
					yarnClusterClient.shutDownCluster();
					yarnClusterClient.shutdown();
				}
			}
		}
		LOG.info("Finished testJavaAPI()");
	}
}
