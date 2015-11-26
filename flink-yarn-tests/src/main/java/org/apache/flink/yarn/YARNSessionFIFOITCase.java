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

import com.google.common.base.Joiner;
import org.apache.commons.io.FileUtils;
import org.apache.flink.client.FlinkYarnSessionCli;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.client.JobClient;
import org.apache.flink.runtime.webmonitor.WebMonitorUtils;
import org.apache.flink.runtime.yarn.AbstractFlinkYarnClient;
import org.apache.flink.runtime.yarn.AbstractFlinkYarnCluster;
import org.apache.flink.runtime.yarn.FlinkYarnClusterStatus;
import org.apache.flink.test.testdata.WordCountData;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.hadoop.fs.Path;
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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.log4j.Level;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
	@Test
	public void testClientStartup() {
		LOG.info("Starting testClientStartup()");
		runWithArgs(new String[]{"-j", flinkUberjar.getAbsolutePath(), "-t", flinkLibFolder.getAbsolutePath(),
						"-n", "1",
						"-jm", "768",
						"-tm", "1024",
						"-s", "2" // Test that 2 slots are started on the TaskManager.
				},
				"Number of connected TaskManagers changed to 1. Slots available: 2", null, RunTypes.YARN_SESSION, 0);
		LOG.info("Finished testClientStartup()");
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
	 * Test TaskManager failure
	 */
	@Test(timeout=100000) // timeout after 100 seconds
	public void testTaskManagerFailure() {
		LOG.info("Starting testTaskManagerFailure()");
		Runner runner = startWithArgs(new String[]{"-j", flinkUberjar.getAbsolutePath(), "-t", flinkLibFolder.getAbsolutePath(),
				"-n", "1",
				"-jm", "768",
				"-tm", "1024",
				"-nm", "customName",
				"-Dfancy-configuration-value=veryFancy",
				"-Dyarn.maximum-failed-containers=3"},
				"Number of connected TaskManagers changed to 1. Slots available: 1",
				RunTypes.YARN_SESSION);

		Assert.assertEquals(2, getRunningContainers());

		// ------------------------ Test if JobManager web interface is accessible -------
		try {
			YarnClient yc = YarnClient.createYarnClient();
			yc.init(yarnConfiguration);
			yc.start();
			List<ApplicationReport> apps = yc.getApplications(EnumSet.of(YarnApplicationState.RUNNING));
			Assert.assertEquals(1, apps.size()); // Only one running
			ApplicationReport app = apps.get(0);
			Assert.assertEquals("customName", app.getName());
			String url = app.getTrackingUrl();
			if(!url.endsWith("/")) {
				url += "/";
			}
			if(!url.startsWith("http://")) {
				url = "http://" + url;
			}
			LOG.info("Got application URL from YARN {}", url);

			String response = TestBaseUtils.getFromHTTP(url + "taskmanagers/");
			JSONObject parsedTMs = new JSONObject(response);
			JSONArray taskManagers = parsedTMs.getJSONArray("taskmanagers");
			Assert.assertNotNull(taskManagers);
			Assert.assertEquals(1, taskManagers.length());
			Assert.assertEquals(1, taskManagers.getJSONObject(0).getInt("slotsNumber"));

			// get the configuration from webinterface & check if the dynamic properties from YARN show up there.
			String jsonConfig = TestBaseUtils.getFromHTTP(url + "jobmanager/config");
			JSONArray parsed = new JSONArray(jsonConfig);
			Map<String, String> parsedConfig = WebMonitorUtils.fromKeyValueJsonArray(parsed);

			Assert.assertEquals("veryFancy", parsedConfig.get("fancy-configuration-value"));
			Assert.assertEquals("3", parsedConfig.get("yarn.maximum-failed-containers"));

			// -------------- FLINK-1902: check if jobmanager hostname/port are shown in web interface
			// first, get the hostname/port
			String oC = outContent.toString();
			Pattern p = Pattern.compile("Flink JobManager is now running on ([a-zA-Z0-9.-]+):([0-9]+)");
			Matcher matches = p.matcher(oC);
			String hostname = null;
			String port = null;
			while(matches.find()) {
				hostname = matches.group(1).toLowerCase();
				port = matches.group(2);
			}
			LOG.info("Extracted hostname:port: {} {}", hostname, port);

			Assert.assertEquals("unable to find hostname in " + parsed, hostname,
					parsedConfig.get(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY));
			Assert.assertEquals("unable to find port in " + parsed, port,
					parsedConfig.get(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY));

			// test logfile access
			String logs = TestBaseUtils.getFromHTTP(url + "jobmanager/log");
			Assert.assertTrue(logs.contains("Starting YARN ApplicationMaster/JobManager (Version"));
		} catch(Throwable e) {
			LOG.warn("Error while running test",e);
			Assert.fail(e.getMessage());
		}

		// ------------------------ Kill container with TaskManager  -------

		// find container id of taskManager:
		ContainerId taskManagerContainer = null;
		NodeManager nodeManager = null;
		UserGroupInformation remoteUgi = null;
		NMTokenIdentifier nmIdent = null;
		try {
			remoteUgi = UserGroupInformation.getCurrentUser();
		} catch (IOException e) {
			LOG.warn("Unable to get curr user", e);
			Assert.fail();
		}
		for(int nmId = 0; nmId < NUM_NODEMANAGERS; nmId++) {
			NodeManager nm = yarnCluster.getNodeManager(nmId);
			ConcurrentMap<ContainerId, Container> containers = nm.getNMContext().getContainers();
			for(Map.Entry<ContainerId, Container> entry : containers.entrySet()) {
				String command = Joiner.on(" ").join(entry.getValue().getLaunchContext().getCommands());
				if(command.contains(YarnTaskManagerRunner.class.getSimpleName())) {
					taskManagerContainer = entry.getKey();
					nodeManager = nm;
					nmIdent = new NMTokenIdentifier(taskManagerContainer.getApplicationAttemptId(), null, "",0);
					// allow myself to do stuff with the container
					// remoteUgi.addCredentials(entry.getValue().getCredentials());
					remoteUgi.addTokenIdentifier(nmIdent);
				}
			}
			sleep(500);
		}

		Assert.assertNotNull("Unable to find container with TaskManager", taskManagerContainer);
		Assert.assertNotNull("Illegal state", nodeManager);

		List<ContainerId> toStop = new LinkedList<ContainerId>();
		toStop.add(taskManagerContainer);
		StopContainersRequest scr = StopContainersRequest.newInstance(toStop);

		try {
			nodeManager.getNMContext().getContainerManager().stopContainers(scr);
		} catch (Throwable e) {
			LOG.warn("Error stopping container", e);
			Assert.fail("Error stopping container: "+e.getMessage());
		}

		// stateful termination check:
		// wait until we saw a container being killed and AFTERWARDS a new one launced
		boolean ok = false;
		do {
			LOG.debug("Waiting for correct order of events. Output: {}", errContent.toString());

			String o = errContent.toString();
			int killedOff = o.indexOf("Container killed by the ApplicationMaster");
			if(killedOff != -1) {
				o = o.substring(killedOff);
				ok = o.indexOf("Launching container") > 0;
			}
			sleep(1000);
		} while(!ok);


		// send "stop" command to command line interface
		runner.sendStop();
		// wait for the thread to stop
		try {
			runner.join(1000);
		} catch (InterruptedException e) {
			LOG.warn("Interrupted while stopping runner", e);
		}
		LOG.warn("stopped");

		// ----------- Send output to logger
		System.setOut(originalStdout);
		System.setErr(originalStderr);
		String oC = outContent.toString();
		String eC = errContent.toString();
		LOG.info("Sending stdout content through logger: \n\n{}\n\n", oC);
		LOG.info("Sending stderr content through logger: \n\n{}\n\n", eC);

		// ------ Check if everything happened correctly
		Assert.assertTrue("Expect to see failed container", eC.contains("New messages from the YARN cluster"));
		Assert.assertTrue("Expect to see failed container", eC.contains("Container killed by the ApplicationMaster"));
		Assert.assertTrue("Expect to see new container started", eC.contains("Launching container") && eC.contains("on host"));

		// cleanup auth for the subsequent tests.
		remoteUgi.getTokenIdentifiers().remove(nmIdent);

		LOG.info("Finished testTaskManagerFailure()");
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
	 * Test per-job yarn cluster
	 *
	 * This also tests the prefixed CliFrontend options for the YARN case
	 * We also test if the requested parallelism of 2 is passed through.
	 * The parallelism is requested at the YARN client (-ys).
	 */
	@Test
	public void perJobYarnCluster() {
		LOG.info("Starting perJobYarnCluster()");
		addTestAppender(JobClient.class, Level.INFO);
		File exampleJarLocation = YarnTestBase.findFile("..", new ContainsName(new String[] {"-WordCount.jar"} , "streaming")); // exclude streaming wordcount here.
		Assert.assertNotNull("Could not find wordcount jar", exampleJarLocation);
		runWithArgs(new String[]{"run", "-m", "yarn-cluster",
						"-yj", flinkUberjar.getAbsolutePath(), "-yt", flinkLibFolder.getAbsolutePath(),
						"-yn", "1",
						"-ys", "2", //test that the job is executed with a DOP of 2
						"-yjm", "768",
						"-ytm", "1024", exampleJarLocation.getAbsolutePath()},
				/* test succeeded after this string */
				"Job execution complete",
				/* prohibited strings: (we want to see (2/2)) */
				new String[]{"System.out)(1/1) switched to FINISHED "},
				RunTypes.CLI_FRONTEND, 0, true);
		LOG.info("Finished perJobYarnCluster()");
	}

	/**
	 * Test per-job yarn cluster with the parallelism set at the CliFrontend instead of the YARN client.
	 */
	@Test
	public void perJobYarnClusterWithParallelism() {
		LOG.info("Starting perJobYarnClusterWithParallelism()");
		// write log messages to stdout as well, so that the runWithArgs() method
		// is catching the log output
		addTestAppender(JobClient.class, Level.INFO);
		File exampleJarLocation = YarnTestBase.findFile("..", new ContainsName(new String[] {"-WordCount.jar"}, "streaming")); // exclude streaming wordcount here.
		Assert.assertNotNull("Could not find wordcount jar", exampleJarLocation);
		runWithArgs(new String[]{"run",
						"-p", "2", //test that the job is executed with a DOP of 2
						"-m", "yarn-cluster",
						"-yj", flinkUberjar.getAbsolutePath(),
						"-yt", flinkLibFolder.getAbsolutePath(),
						"-yn", "1",
						"-yjm", "768",
						"-ytm", "1024", exampleJarLocation.getAbsolutePath()},
				/* test succeeded after this string */
				"Job execution complete",
				/* prohibited strings: (we want to see (2/2)) */
				new String[]{"System.out)(1/1) switched to FINISHED "},
				RunTypes.CLI_FRONTEND, 0, true);
		LOG.info("Finished perJobYarnClusterWithParallelism()");
	}

	private void testDetachedPerJobYarnClusterInternal(String job) {
		YarnClient yc = YarnClient.createYarnClient();
		yc.init(yarnConfiguration);
		yc.start();

		// get temporary folder for writing output of wordcount example
		File tmpOutFolder = null;
		try{
			tmpOutFolder = tmp.newFolder();
		}
		catch(IOException e) {
			throw new RuntimeException(e);
		}

		// get temporary file for reading input data for wordcount example
		File tmpInFile;
		try{
			tmpInFile = tmp.newFile();
			FileUtils.writeStringToFile(tmpInFile,WordCountData.TEXT);
		}
		catch(IOException e) {
			throw new RuntimeException(e);
		}

		Runner runner = startWithArgs(new String[]{"run", "-m", "yarn-cluster", "-yj", flinkUberjar.getAbsolutePath(),
						"-yt", flinkLibFolder.getAbsolutePath(),
						"-yn", "1",
						"-yjm", "768",
						"-yD", "yarn.heap-cutoff-ratio=0.5", // test if the cutoff is passed correctly
						"-ytm", "1024",
						"-ys", "2", // test requesting slots from YARN.
						"--yarndetached", job, tmpInFile.getAbsoluteFile().toString() , tmpOutFolder.getAbsoluteFile().toString()},
				"Job has been submitted with JobID",
				RunTypes.CLI_FRONTEND);

		// it should usually be 2, but on slow machines, the number varies
		Assert.assertTrue("There should be at most 2 containers running", getRunningContainers() <= 2);
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
			List<ApplicationReport> apps = yc.getApplications(EnumSet.of(YarnApplicationState.RUNNING));

			ApplicationId tmpAppId;
			if (apps.size() == 1) {
				// Better method to find the right appId. But sometimes the app is shutting down very fast
				// Only one running
				tmpAppId = apps.get(0).getApplicationId();

				LOG.info("waiting for the job with appId {} to finish", tmpAppId);
				// wait until the app has finished
				while(yc.getApplications(EnumSet.of(YarnApplicationState.RUNNING)).size() > 0) {
					sleep(500);
				}
			} else {
				// get appId by finding the latest finished appid
				apps = yc.getApplications();
				Collections.sort(apps, new Comparator<ApplicationReport>() {
					@Override
					public int compare(ApplicationReport o1, ApplicationReport o2) {
						return o1.getApplicationId().compareTo(o2.getApplicationId())*-1;
					}
				});
				tmpAppId = apps.get(0).getApplicationId();
				LOG.info("Selected {} as the last appId from {}", tmpAppId, Arrays.toString(apps.toArray()));
			}
			final ApplicationId id = tmpAppId;

			// now it has finished.
			// check the output files.
			File[] listOfOutputFiles = tmpOutFolder.listFiles();


			Assert.assertNotNull("Taskmanager output not found", listOfOutputFiles);
			LOG.info("The job has finished. TaskManager output files found in {}", tmpOutFolder );

			// read all output files in output folder to one output string
			String content = "";
			for(File f:listOfOutputFiles)
			{
				if(f.isFile())
				{
					content += FileUtils.readFileToString(f) + "\n";
				}
			}
			//String content = FileUtils.readFileToString(taskmanagerOut);
			// check for some of the wordcount outputs.
			Assert.assertTrue("Expected string 'da 5' or '(all,2)' not found in string '"+content+"'", content.contains("da 5") || content.contains("(da,5)") || content.contains("(all,2)"));
			Assert.assertTrue("Expected string 'der 29' or '(mind,1)' not found in string'"+content+"'",content.contains("der 29") || content.contains("(der,29)") || content.contains("(mind,1)"));

			// check if the heap size for the TaskManager was set correctly
			File jobmanagerLog = YarnTestBase.findFile("..", new FilenameFilter() {
				@Override
				public boolean accept(File dir, String name) {
					return name.contains("jobmanager.log") && dir.getAbsolutePath().contains(id.toString());
				}
			});
			Assert.assertNotNull("Unable to locate JobManager log", jobmanagerLog);
			content = FileUtils.readFileToString(jobmanagerLog);
			// TM was started with 1024 but we cut off 50% (NOT THE DEFAULT VALUE)
			String expected = "Starting TM with command=$JAVA_HOME/bin/java -Xms424m -Xmx424m";
			Assert.assertTrue("Expected string '" + expected + "' not found in JobManager log: '"+jobmanagerLog+"'",
					content.contains(expected));
			expected = " (2/2) (attempt #0) to ";
			Assert.assertTrue("Expected string '" + expected + "' not found in JobManager log." +
							"This string checks that the job has been started with a parallelism of 2. Log contents: '"+jobmanagerLog+"'",
					content.contains(expected));

			// make sure the detached app is really finished.
			LOG.info("Checking again that app has finished");
			ApplicationReport rep;
			do {
				sleep(500);
				rep = yc.getApplicationReport(id);
				LOG.info("Got report {}", rep);
			} while(rep.getYarnApplicationState() == YarnApplicationState.RUNNING);

		} catch(Throwable t) {
			LOG.warn("Error while detached yarn session was running", t);
			Assert.fail(t.getMessage());
		}
	}

	/**
	 * Test a fire-and-forget job submission to a YARN cluster.
	 */
	@Test(timeout=60000)
	public void testDetachedPerJobYarnCluster() {
		LOG.info("Starting testDetachedPerJobYarnCluster()");

		File exampleJarLocation = YarnTestBase.findFile("..", new ContainsName(new String[] {"-WordCount.jar"}, "streaming")); // exclude streaming wordcount here.
		Assert.assertNotNull("Could not find wordcount jar", exampleJarLocation);

		testDetachedPerJobYarnClusterInternal(exampleJarLocation.getAbsolutePath());

		LOG.info("Finished testDetachedPerJobYarnCluster()");
	}

	/**
	 * Test a fire-and-forget job submission to a YARN cluster.
	 */
	@Test(timeout=60000)
	public void testDetachedPerJobYarnClusterWithStreamingJob() {
		LOG.info("Starting testDetachedPerJobYarnClusterWithStreamingJob()");

		File exampleJarLocation = YarnTestBase.findFile("..", new ContainsName(new String[] {"flink-streaming-examples", "-WordCount.jar"}));
		Assert.assertNotNull("Could not find wordcount jar", exampleJarLocation);

		testDetachedPerJobYarnClusterInternal(exampleJarLocation.getAbsolutePath());

		LOG.info("Finished testDetachedPerJobYarnClusterWithStreamingJob()");
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
		FlinkYarnClusterStatus expectedStatus = new FlinkYarnClusterStatus(1, 1);
		for(int second = 0; second < WAIT_TIME * 2; second++) { // run "forever"
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				LOG.warn("Interrupted", e);
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
		yarnCluster.shutdown(false);
		LOG.info("Finished testJavaAPI()");
	}
}
