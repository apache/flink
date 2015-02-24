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
import org.apache.flink.runtime.yarn.AbstractFlinkYarnClient;
import org.apache.flink.runtime.yarn.AbstractFlinkYarnCluster;
import org.apache.flink.runtime.yarn.FlinkYarnClusterStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;


/**
 * This test starts a MiniYARNCluster with a FIFO scheudler.
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
		startYARNWithConfig(yarnConfiguration);
	}
	/**
	 * Test regular operation, including command line parameter parsing.
	 */
	@Test
	public void testClientStartup() {
		LOG.info("Starting testClientStartup()");
		runWithArgs(new String[] {"-j", flinkUberjar.getAbsolutePath(),
						"-n", "1",
						"-jm", "512",
						"-tm", "1024"},
				"Number of connected TaskManagers changed to 1. Slots available: 1", RunTypes.YARN_SESSION);
		LOG.info("Finished testClientStartup()");
	}

	/**
	 * Test querying the YARN cluster.
	 *
	 * This test validates through 666*2 cores in the "cluster".
	 */
	@Test
	public void testQueryCluster() {
		LOG.info("Starting testQueryCluster()");
		runWithArgs(new String[] {"-q"}, "Summary: totalMemory 8192 totalCores 1332", RunTypes.YARN_SESSION); // we have 666*2 cores.
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
				"-n", "1",
				"-jm", "512",
				"-tm", "1024",
				"-qu", "doesntExist"}, "Number of connected TaskManagers changed to 1. Slots available: 1", RunTypes.YARN_SESSION);
		LOG.info("Finished testNonexistingQueue()");
	}

	/**
	 * Test requesting more resources than available.
	 */
	@Test
	public void testMoreNodesThanAvailable() {
		if(ignoreOnTravis()) {
			return;
		}
		addTestAppender();
		LOG.info("Starting testMoreNodesThanAvailable()");
		runWithArgs(new String[] {"-j", flinkUberjar.getAbsolutePath(),
				"-n", "10",
				"-jm", "512",
				"-tm", "1024"}, "Number of connected TaskManagers changed to", RunTypes.YARN_SESSION); // the number of TMs depends on the speed of the test hardware
		LOG.info("Finished testMoreNodesThanAvailable()");
		checkForLogString("This YARN session requires 10752MB of memory in the cluster. There are currently only 8192MB available.");
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
	@Test
	public void testResourceComputation() {
		if(ignoreOnTravis()) {
			return;
		}
		addTestAppender();
		LOG.info("Starting testResourceComputation()");
		runWithArgs(new String[] {"-j", flinkUberjar.getAbsolutePath(),
				"-n", "5",
				"-jm", "256",
				"-tm", "1585"}, "Number of connected TaskManagers changed to", RunTypes.YARN_SESSION);
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
	@Test
	public void testfullAlloc() {
		if(ignoreOnTravis()) {
			return;
		}
		addTestAppender();
		LOG.info("Starting testfullAlloc()");
		runWithArgs(new String[] {"-j", flinkUberjar.getAbsolutePath(),
				"-n", "2",
				"-jm", "256",
				"-tm", "3840"}, "Number of connected TaskManagers changed to", RunTypes.YARN_SESSION);
		LOG.info("Finished testfullAlloc()");
		checkForLogString("There is not enough memory available in the YARN cluster. The TaskManager(s) require 3840MB each. NodeManagers available: [4096, 4096]\n" +
				"After allocating the JobManager (512MB) and (1/2) TaskManagers, the following NodeManagers are available: [3584, 256]");
	}

	/**
	 * Test per-job yarn cluster
	 *
	 * This also tests the prefixed CliFrontend options for the YARN case
	 */
	@Test
	public void perJobYarnCluster() {
		LOG.info("Starting perJobYarnCluster()");
		File exampleJarLocation = YarnTestBase.findFile(".", new ContainsName("-WordCount.jar", "streaming")); // exclude streaming wordcount here.
		runWithArgs(new String[] {"run", "-m", "yarn-cluster",
				"-yj", flinkUberjar.getAbsolutePath(),
				"-yn", "1",
				"-yjm", "512",
				"-ytm", "1024", exampleJarLocation.getAbsolutePath()}, "Job execution switched to status FINISHED.", RunTypes.CLI_FRONTEND);
		LOG.info("Finished perJobYarnCluster()");
	}

	/**
	 * Test the YARN Java API
	 */
	@Test
	public void testJavaAPI() {
		final int WAIT_TIME = 15;
		LOG.info("Starting testJavaAPI()");

		AbstractFlinkYarnClient flinkYarnClient = FlinkYarnSessionCli.getFlinkYarnClient();
		flinkYarnClient.setTaskManagerCount(1);
		flinkYarnClient.setJobManagerMemory(512);
		flinkYarnClient.setTaskManagerMemory(512);
		flinkYarnClient.setLocalJarPath(new Path(flinkUberjar.getAbsolutePath()));
		String confDirPath = System.getenv("FLINK_CONF_DIR");
		flinkYarnClient.setConfigurationDirectory(confDirPath);
		flinkYarnClient.setFlinkConfigurationObject(GlobalConfiguration.getConfiguration());
		flinkYarnClient.setConfigurationFilePath(new Path(confDirPath + File.separator + "flink-conf.yaml"));

		// deploy
		AbstractFlinkYarnCluster yarnCluster = null;
		try {
			yarnCluster = flinkYarnClient.deploy(null);
		} catch (Exception e) {
			System.err.println("Error while deploying YARN cluster: "+e.getMessage());
			e.printStackTrace(System.err);
			Assert.fail();
		}
		FlinkYarnClusterStatus expectedStatus = new FlinkYarnClusterStatus(1, 1);
		for(int second = 0; second < WAIT_TIME * 2; second++) { // run "forever"
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				LOG.warn("Interrupted", e);
				Thread.interrupted();
			}
			FlinkYarnClusterStatus status = yarnCluster.getClusterStatus();
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
		yarnCluster.shutdown();
		LOG.info("Finished testJavaAPI()");
	}

	public boolean ignoreOnTravis() {
		if(System.getenv("TRAVIS") != null && System.getenv("TRAVIS").equals("true")) {
			// we skip the test until we are able to start a smaller yarn clsuter
			// right now, the miniyarncluster has the size of the nodemanagers fixed on 4 GBs.
			LOG.warn("Skipping test on travis for now");
			return true;
		}
		return false;
	}

	//
	// --------------- Tools to test if a certain string has been logged with Log4j. -------------
	// See :  http://stackoverflow.com/questions/3717402/how-to-test-w-junit-that-warning-was-logged-w-log4j
	//
	private static TestAppender testAppender;
	public static void addTestAppender() {
		testAppender = new TestAppender();
		org.apache.log4j.Logger.getRootLogger().addAppender(testAppender);
	}

	public static void checkForLogString(String expected) {
		if(testAppender == null) {
			throw new NullPointerException("Initialize it first");
		}
		LoggingEvent found = null;
		for(LoggingEvent event: testAppender.events) {
			if(event.getMessage().toString().contains(expected)) {
				found = event;
				break;
			}
		}
		if(found != null) {
			LOG.info("Found expected string '"+expected+"' in log message "+found);
			return;
		}
		Assert.fail("Unable to find expected string '"+expected+"' in log messages");
	}

	public static class TestAppender extends AppenderSkeleton {
		public List<LoggingEvent> events = new ArrayList<LoggingEvent>();
		public void close() {}
		public boolean requiresLayout() {return false;}
		@Override
		protected void append(LoggingEvent event) {
			events.add(event);
		}
	}
	
}
