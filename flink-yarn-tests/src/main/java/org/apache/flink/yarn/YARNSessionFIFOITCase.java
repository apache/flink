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

import org.apache.flink.client.FlinkYarnSessionCli;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.clusterframework.messages.GetClusterStatusResponse;
import org.apache.flink.runtime.yarn.AbstractFlinkYarnClient;
import org.apache.flink.runtime.yarn.AbstractFlinkYarnCluster;

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
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;

import static org.apache.flink.yarn.UtilsTest.addTestAppender;
import static org.apache.flink.yarn.UtilsTest.checkForLogString;


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
		yarnConfiguration.setClass(YarnConfiguration.RM_SCHEDULER, FifoScheduler.class, ResourceScheduler.class);
		yarnConfiguration.setInt(YarnConfiguration.NM_PMEM_MB, 768);
		yarnConfiguration.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 512);
		yarnConfiguration.set(YarnTestBase.TEST_CLUSTER_NAME_KEY, "flink-yarn-tests-fifo");
		startYARNWithConfig(yarnConfiguration);
	}

	@After
	public void checkForProhibitedLogContents() {
		ensureNoProhibitedStringInLogFiles(PROHIBITED_STRINGS, WHITELISTED_STRINGS);
	}

	/**
	 * Test regular operation, including command line parameter parsing.
	 */
	@Test(timeout=60000) // timeout after a minute.
	public void testDetachedMode() {
		LOG.info("Starting testDetachedMode()");
		addTestAppender(FlinkYarnSessionCli.class, Level.INFO);
		Runner runner = startWithArgs(new String[]{"-j", flinkUberjar.getAbsolutePath(),
						"-t", flinkLibFolder.getAbsolutePath(),
						"-n", "1",
						"-jm", "768",
						"-tm", "1024",
						"--name", "MyCustomName", // test setting a custom name
						"--detached"},
				"Flink JobManager is now running on", RunTypes.YARN_SESSION);

		checkForLogString("The Flink YARN client has been started in detached mode");

		Assert.assertFalse("The runner should detach.", runner.isAlive());

		LOG.info("Waiting until two containers are running");
		// wait until two containers are running
		while(getRunningContainers() < 2) {
			sleep(500);
		}
		LOG.info("Two containers are running. Killing the application");

		// kill application "externally".
		try {
			YarnClient yc = YarnClient.createYarnClient();
			yc.init(yarnConfiguration);
			yc.start();
			List<ApplicationReport> apps = yc.getApplications(EnumSet.of(YarnApplicationState.RUNNING));
			Assert.assertEquals(1, apps.size()); // Only one running
			ApplicationReport app = apps.get(0);

			Assert.assertEquals("MyCustomName", app.getName());
			ApplicationId id = app.getApplicationId();
			yc.killApplication(id);

			while(yc.getApplications(EnumSet.of(YarnApplicationState.KILLED)).size() == 0) {
				sleep(500);
			}
		} catch(Throwable t) {
			LOG.warn("Killing failed", t);
			Assert.fail();
		}

		LOG.info("Finished testDetachedMode()");
	}

	/**
	 * Test querying the YARN cluster.
	 *
	 * This test validates through 666*2 cores in the "cluster".
	 */
	@Test
	public void testQueryCluster() {
		LOG.info("Starting testQueryCluster()");
		runWithArgs(new String[] {"-q"}, "Summary: totalMemory 8192 totalCores 1332",null, RunTypes.YARN_SESSION, 0); // we have 666*2 cores.
		LOG.info("Finished testQueryCluster()");
	}

	/**
	 * Test deployment to non-existing queue. (user-reported error)
	 * Deployment to the queue is possible because there are no queues, so we don't check.
	 */
	@Test
	public void testNonexistingQueue() {
		LOG.info("Starting testNonexistingQueue()");
		runWithArgs(new String[]{"-j", flinkUberjar.getAbsolutePath(),
				"-t", flinkLibFolder.getAbsolutePath(),
				"-n", "1",
				"-jm", "768",
				"-tm", "1024",
				"-qu", "doesntExist"}, "Number of connected TaskManagers changed to 1. Slots available: 1", null, RunTypes.YARN_SESSION, 0);
		LOG.info("Finished testNonexistingQueue()");
	}

	/**
	 * The test cluster has the following resources:
	 * - 2 Nodes with 4096 MB each.
	 * - RM_SCHEDULER_MINIMUM_ALLOCATION_MB is 512
	 *
	 * We allocate:
	 * 1 JobManager with 256 MB (will be automatically upgraded to 512 due to min alloc mb)
	 * 5 TaskManagers with 1585 MB
	 *
	 * user sees a total request of: 8181 MB (fits)
	 * system sees a total request of: 8437 (doesn't fit due to min alloc mb)
	 */
	@Ignore("The test is too resource consuming (8.5 GB of memory)")
	@Test
	public void testResourceComputation() {
		addTestAppender(FlinkYarnClient.class, Level.WARN);
		LOG.info("Starting testResourceComputation()");
		runWithArgs(new String[]{"-j", flinkUberjar.getAbsolutePath(), "-t", flinkLibFolder.getAbsolutePath(),
				"-n", "5",
				"-jm", "256",
				"-tm", "1585"}, "Number of connected TaskManagers changed to", null, RunTypes.YARN_SESSION, 0);
		LOG.info("Finished testResourceComputation()");
		checkForLogString("This YARN session requires 8437MB of memory in the cluster. There are currently only 8192MB available.");
	}

	/**
	 * The test cluster has the following resources:
	 * - 2 Nodes with 4096 MB each.
	 * - RM_SCHEDULER_MINIMUM_ALLOCATION_MB is 512
	 *
	 * We allocate:
	 * 1 JobManager with 256 MB (will be automatically upgraded to 512 due to min alloc mb)
	 * 2 TaskManagers with 3840 MB
	 *
	 * the user sees a total request of: 7936 MB (fits)
	 * the system sees a request of: 8192 MB (fits)
	 * HOWEVER: one machine is going to need 3840 + 512 = 4352 MB, which doesn't fit.
	 *
	 * --> check if the system properly rejects allocating this session.
	 */
	@Ignore("The test is too resource consuming (8 GB of memory)")
	@Test
	public void testfullAlloc() {
		addTestAppender(FlinkYarnClient.class, Level.WARN);
		LOG.info("Starting testfullAlloc()");
		runWithArgs(new String[]{"-j", flinkUberjar.getAbsolutePath(), "-t", flinkLibFolder.getAbsolutePath(),
				"-n", "2",
				"-jm", "256",
				"-tm", "3840"}, "Number of connected TaskManagers changed to", null, RunTypes.YARN_SESSION, 0);
		LOG.info("Finished testfullAlloc()");
		checkForLogString("There is not enough memory available in the YARN cluster. The TaskManager(s) require 3840MB each. NodeManagers available: [4096, 4096]\n" +
				"After allocating the JobManager (512MB) and (1/2) TaskManagers, the following NodeManagers are available: [3584, 256]");
	}

	/**
	 * Test the YARN Java API
	 */
	@Test
	public void testJavaAPI() {
		final int WAIT_TIME = 15;
		LOG.info("Starting testJavaAPI()");

		AbstractFlinkYarnClient flinkYarnClient = FlinkYarnSessionCli.getFlinkYarnClient();
		Assert.assertNotNull("unable to get yarn client", flinkYarnClient);
		flinkYarnClient.setTaskManagerCount(1);
		flinkYarnClient.setJobManagerMemory(768);
		flinkYarnClient.setTaskManagerMemory(1024);
		flinkYarnClient.setLocalJarPath(new Path(flinkUberjar.getAbsolutePath()));
		flinkYarnClient.setShipFiles(Arrays.asList(flinkLibFolder.listFiles()));
		String confDirPath = System.getenv("FLINK_CONF_DIR");
		flinkYarnClient.setConfigurationDirectory(confDirPath);
		flinkYarnClient.setFlinkConfigurationObject(GlobalConfiguration.getConfiguration());
		flinkYarnClient.setConfigurationFilePath(new Path(confDirPath + File.separator + "flink-conf.yaml"));

		// deploy
		AbstractFlinkYarnCluster yarnCluster = null;
		try {
			yarnCluster = flinkYarnClient.deploy();
			yarnCluster.connectToCluster();
		} catch (Exception e) {
			LOG.warn("Failing test", e);
			Assert.fail("Error while deploying YARN cluster: "+e.getMessage());
		}
		GetClusterStatusResponse expectedStatus = new GetClusterStatusResponse(1, 1);
		for(int second = 0; second < WAIT_TIME * 2; second++) { // run "forever"
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				LOG.warn("Interrupted", e);
			}
			GetClusterStatusResponse status = yarnCluster.getClusterStatus();
			if(status != null && status.equals(expectedStatus)) {
				LOG.info("Cluster reached status " + status);
				break; // all good, cluster started
			}
			if(second > WAIT_TIME) {
				// we waited for 15 seconds. cluster didn't come up correctly
				Assert.fail("The custer didn't start after " + WAIT_TIME + " seconds");
			}
		}

		// use the cluster
		Assert.assertNotNull(yarnCluster.getJobManagerAddress());
		Assert.assertNotNull(yarnCluster.getWebInterfaceURL());

		LOG.info("Shutting down cluster. All tests passed");
		// shutdown cluster
		yarnCluster.shutdown(false);
		LOG.info("Finished testJavaAPI()");
	}
}
