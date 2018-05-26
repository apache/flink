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
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.runtime.client.JobClient;
import org.apache.flink.runtime.taskexecutor.TaskManagerServices;
import org.apache.flink.runtime.webmonitor.WebMonitorUtils;
import org.apache.flink.test.testdata.WordCountData;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.yarn.cli.FlinkYarnSessionCli;
import org.apache.flink.yarn.configuration.YarnConfigOptions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
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
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static junit.framework.TestCase.assertTrue;
import static org.apache.flink.yarn.UtilsTest.addTestAppender;
import static org.apache.flink.yarn.UtilsTest.checkForLogString;
import static org.apache.flink.yarn.util.YarnTestUtils.getTestJarPath;
import static org.junit.Assume.assumeTrue;

/**
 * This test starts a MiniYARNCluster with a CapacityScheduler.
 * Is has, by default a queue called "default". The configuration here adds another queue: "qa-team".
 */
public class YARNSessionCapacitySchedulerITCase extends YarnTestBase {
	private static final Logger LOG = LoggerFactory.getLogger(YARNSessionCapacitySchedulerITCase.class);

	@BeforeClass
	public static void setup() {
		YARN_CONFIGURATION.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class, ResourceScheduler.class);
		YARN_CONFIGURATION.set("yarn.scheduler.capacity.root.queues", "default,qa-team");
		YARN_CONFIGURATION.setInt("yarn.scheduler.capacity.root.default.capacity", 40);
		YARN_CONFIGURATION.setInt("yarn.scheduler.capacity.root.qa-team.capacity", 60);
		YARN_CONFIGURATION.set(YarnTestBase.TEST_CLUSTER_NAME_KEY, "flink-yarn-tests-capacityscheduler");
		startYARNWithConfig(YARN_CONFIGURATION);
	}

	/**
	 * Test regular operation, including command line parameter parsing.
	 */
	@Test
	public void testClientStartup() throws IOException {
		assumeTrue("The new mode does not start TMs upfront.", !isNewMode);
		LOG.info("Starting testClientStartup()");
		runWithArgs(new String[]{"-j", flinkUberjar.getAbsolutePath(), "-t", flinkLibFolder.getAbsolutePath(),
						"-n", "1",
						"-jm", "768",
						"-tm", "1024", "-qu", "qa-team"},
				"Number of connected TaskManagers changed to 1. Slots available: 1", null, RunTypes.YARN_SESSION, 0);
		LOG.info("Finished testClientStartup()");
	}

	/**
	 * Test per-job yarn cluster
	 *
	 * <p>This also tests the prefixed CliFrontend options for the YARN case
	 * We also test if the requested parallelism of 2 is passed through.
	 * The parallelism is requested at the YARN client (-ys).
	 */
	@Test
	public void perJobYarnCluster() throws IOException {
		LOG.info("Starting perJobYarnCluster()");
		addTestAppender(JobClient.class, Level.INFO);
		File exampleJarLocation = getTestJarPath("BatchWordCount.jar");
		runWithArgs(new String[]{"run", "-m", "yarn-cluster",
				"-yj", flinkUberjar.getAbsolutePath(), "-yt", flinkLibFolder.getAbsolutePath(),
				"-yn", "1",
				"-ys", "2", //test that the job is executed with a DOP of 2
				"-yjm", "768",
				"-ytm", "1024", exampleJarLocation.getAbsolutePath()},
			/* test succeeded after this string */
			"Program execution finished",
			/* prohibited strings: (to verify the parallelism) */
			// (we should see "DataSink (...) (1/2)" and "DataSink (...) (2/2)" instead)
			new String[]{"DataSink \\(.*\\) \\(1/1\\) switched to FINISHED"},
			RunTypes.CLI_FRONTEND, 0, true);
		LOG.info("Finished perJobYarnCluster()");
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
	public void perJobYarnClusterOffHeap() throws IOException {
		LOG.info("Starting perJobYarnCluster()");
		addTestAppender(JobClient.class, Level.INFO);
		File exampleJarLocation = getTestJarPath("BatchWordCount.jar");

		// set memory constraints (otherwise this is the same test as perJobYarnCluster() above)
		final long taskManagerMemoryMB = 1024;
		//noinspection NumericOverflow if the calculation of the total Java memory size overflows, default configuration parameters are wrong in the first place, so we can ignore this inspection
		final long networkBuffersMB = TaskManagerServices
			.calculateNetworkBufferMemory(
				(taskManagerMemoryMB -
					ResourceManagerOptions.CONTAINERIZED_HEAP_CUTOFF_MIN.defaultValue()) << 20,
				new Configuration()) >> 20;
		final long offHeapMemory = taskManagerMemoryMB
			- ResourceManagerOptions.CONTAINERIZED_HEAP_CUTOFF_MIN.defaultValue()
			// cutoff memory (will be added automatically)
			- networkBuffersMB // amount of memory used for network buffers
			- 100; // reserve something for the Java heap space

		runWithArgs(new String[]{"run", "-m", "yarn-cluster",
				"-yj", flinkUberjar.getAbsolutePath(), "-yt", flinkLibFolder.getAbsolutePath(),
				"-yn", "1",
				"-ys", "2", //test that the job is executed with a DOP of 2
				"-yjm", "768",
				"-ytm", String.valueOf(taskManagerMemoryMB),
				"-yD", "taskmanager.memory.off-heap=true",
				"-yD", "taskmanager.memory.size=" + offHeapMemory,
				"-yD", "taskmanager.memory.preallocate=true", exampleJarLocation.getAbsolutePath()},
			/* test succeeded after this string */
			"Program execution finished",
			/* prohibited strings: (to verify the parallelism) */
			// (we should see "DataSink (...) (1/2)" and "DataSink (...) (2/2)" instead)
			new String[]{"DataSink \\(.*\\) \\(1/1\\) switched to FINISHED"},
			RunTypes.CLI_FRONTEND, 0, true);
		LOG.info("Finished perJobYarnCluster()");
	}

	/**
	 * Test TaskManager failure and also if the vcores are set correctly (see issue FLINK-2213).
	 */
	@Test(timeout = 100000) // timeout after 100 seconds
	public void testTaskManagerFailure() throws Exception {
		assumeTrue("The new mode does not start TMs upfront.", !isNewMode);
		LOG.info("Starting testTaskManagerFailure()");
		Runner runner = startWithArgs(new String[]{"-j", flinkUberjar.getAbsolutePath(), "-t", flinkLibFolder.getAbsolutePath(),
				"-n", "1",
				"-jm", "768",
				"-tm", "1024",
				"-s", "3", // set the slots 3 to check if the vCores are set properly!
				"-nm", "customName",
				"-Dfancy-configuration-value=veryFancy",
				"-Dyarn.maximum-failed-containers=3",
				"-D" + YarnConfigOptions.VCORES.key() + "=2"},
			"Number of connected TaskManagers changed to 1. Slots available: 3",
			RunTypes.YARN_SESSION);

		Assert.assertEquals(2, getRunningContainers());

		// ------------------------ Test if JobManager web interface is accessible -------

		final YarnClient yc = YarnClient.createYarnClient();
		yc.init(YARN_CONFIGURATION);
		yc.start();

		List<ApplicationReport> apps = yc.getApplications(EnumSet.of(YarnApplicationState.RUNNING));
		Assert.assertEquals(1, apps.size()); // Only one running
		ApplicationReport app = apps.get(0);
		Assert.assertEquals("customName", app.getName());
		String url = app.getTrackingUrl();
		if (!url.endsWith("/")) {
			url += "/";
		}
		if (!url.startsWith("http://")) {
			url = "http://" + url;
		}
		LOG.info("Got application URL from YARN {}", url);

		String response = TestBaseUtils.getFromHTTP(url + "taskmanagers/");

		JsonNode parsedTMs = new ObjectMapper().readTree(response);
		ArrayNode taskManagers = (ArrayNode) parsedTMs.get("taskmanagers");
		Assert.assertNotNull(taskManagers);
		Assert.assertEquals(1, taskManagers.size());
		Assert.assertEquals(3, taskManagers.get(0).get("slotsNumber").asInt());

		// get the configuration from webinterface & check if the dynamic properties from YARN show up there.
		String jsonConfig = TestBaseUtils.getFromHTTP(url + "jobmanager/config");
		Map<String, String> parsedConfig = WebMonitorUtils.fromKeyValueJsonArray(jsonConfig);

		Assert.assertEquals("veryFancy", parsedConfig.get("fancy-configuration-value"));
		Assert.assertEquals("3", parsedConfig.get("yarn.maximum-failed-containers"));
		Assert.assertEquals("2", parsedConfig.get(YarnConfigOptions.VCORES.key()));

		// -------------- FLINK-1902: check if jobmanager hostname/port are shown in web interface
		// first, get the hostname/port
		String oC = outContent.toString();
		Pattern p = Pattern.compile("Flink JobManager is now running on ([a-zA-Z0-9.-]+):([0-9]+)");
		Matcher matches = p.matcher(oC);
		String hostname = null;
		String port = null;
		while (matches.find()) {
			hostname = matches.group(1).toLowerCase();
			port = matches.group(2);
		}
		LOG.info("Extracted hostname:port: {} {}", hostname, port);

		Assert.assertEquals("unable to find hostname in " + jsonConfig, hostname,
			parsedConfig.get(JobManagerOptions.ADDRESS.key()));
		Assert.assertEquals("unable to find port in " + jsonConfig, port,
			parsedConfig.get(JobManagerOptions.PORT.key()));

		// test logfile access
		String logs = TestBaseUtils.getFromHTTP(url + "jobmanager/log");
		Assert.assertTrue(logs.contains("Starting YARN ApplicationMaster"));
		Assert.assertTrue(logs.contains("Starting JobManager"));
		Assert.assertTrue(logs.contains("Starting JobManager Web Frontend"));

		// ------------------------ Kill container with TaskManager and check if vcores are set correctly -------

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
		for (int nmId = 0; nmId < NUM_NODEMANAGERS; nmId++) {
			NodeManager nm = yarnCluster.getNodeManager(nmId);
			ConcurrentMap<ContainerId, Container> containers = nm.getNMContext().getContainers();
			for (Map.Entry<ContainerId, Container> entry : containers.entrySet()) {
				String command = StringUtils.join(entry.getValue().getLaunchContext().getCommands(), " ");
				if (command.contains(YarnTaskManager.class.getSimpleName())) {
					taskManagerContainer = entry.getKey();
					nodeManager = nm;
					nmIdent = new NMTokenIdentifier(taskManagerContainer.getApplicationAttemptId(), null, "", 0);
					// allow myself to do stuff with the container
					// remoteUgi.addCredentials(entry.getValue().getCredentials());
					remoteUgi.addTokenIdentifier(nmIdent);
				}
			}
			sleep(500);
		}

		Assert.assertNotNull("Unable to find container with TaskManager", taskManagerContainer);
		Assert.assertNotNull("Illegal state", nodeManager);

		yc.stop();

		List<ContainerId> toStop = new LinkedList<ContainerId>();
		toStop.add(taskManagerContainer);
		StopContainersRequest scr = StopContainersRequest.newInstance(toStop);

		try {
			nodeManager.getNMContext().getContainerManager().stopContainers(scr);
		} catch (Throwable e) {
			LOG.warn("Error stopping container", e);
			Assert.fail("Error stopping container: " + e.getMessage());
		}

		// stateful termination check:
		// wait until we saw a container being killed and AFTERWARDS a new one launched
		boolean ok = false;
		do {
			LOG.debug("Waiting for correct order of events. Output: {}", errContent.toString());

			String o = errContent.toString();
			int killedOff = o.indexOf("Container killed by the ApplicationMaster");
			if (killedOff != -1) {
				o = o.substring(killedOff);
				ok = o.indexOf("Launching TaskManager") > 0;
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
		System.setOut(ORIGINAL_STDOUT);
		System.setErr(ORIGINAL_STDERR);
		oC = outContent.toString();
		String eC = errContent.toString();
		LOG.info("Sending stdout content through logger: \n\n{}\n\n", oC);
		LOG.info("Sending stderr content through logger: \n\n{}\n\n", eC);

		// ------ Check if everything happened correctly
		Assert.assertTrue("Expect to see failed container",
			eC.contains("New messages from the YARN cluster"));

		Assert.assertTrue("Expect to see failed container",
			eC.contains("Container killed by the ApplicationMaster"));

		Assert.assertTrue("Expect to see new container started",
			eC.contains("Launching TaskManager") && eC.contains("on host"));

		// cleanup auth for the subsequent tests.
		remoteUgi.getTokenIdentifiers().remove(nmIdent);

		LOG.info("Finished testTaskManagerFailure()");
	}

	/**
	 * Test deployment to non-existing queue & ensure that the system logs a WARN message
	 * for the user. (Users had unexpected behavior of Flink on YARN because they mistyped the
	 * target queue. With an error message, we can help users identifying the issue)
	 */
	@Test
	public void testNonexistingQueueWARNmessage() throws IOException {
		LOG.info("Starting testNonexistingQueueWARNmessage()");
		addTestAppender(AbstractYarnClusterDescriptor.class, Level.WARN);
		try {
			runWithArgs(new String[]{"-j", flinkUberjar.getAbsolutePath(),
				"-t", flinkLibFolder.getAbsolutePath(),
				"-n", "1",
				"-jm", "768",
				"-tm", "1024",
				"-qu", "doesntExist"}, "to unknown queue: doesntExist", null, RunTypes.YARN_SESSION, 1);
		} catch (Exception e) {
			assertTrue(ExceptionUtils.findThrowableWithMessage(e, "to unknown queue: doesntExist").isPresent());
		}
		checkForLogString("The specified queue 'doesntExist' does not exist. Available queues");
		LOG.info("Finished testNonexistingQueueWARNmessage()");
	}

	/**
	 * Test per-job yarn cluster with the parallelism set at the CliFrontend instead of the YARN client.
	 */
	@Test
	public void perJobYarnClusterWithParallelism() throws IOException {
		LOG.info("Starting perJobYarnClusterWithParallelism()");
		// write log messages to stdout as well, so that the runWithArgs() method
		// is catching the log output
		addTestAppender(JobClient.class, Level.INFO);
		File exampleJarLocation = getTestJarPath("BatchWordCount.jar");
		runWithArgs(new String[]{"run",
				"-p", "2", //test that the job is executed with a DOP of 2
				"-m", "yarn-cluster",
				"-yj", flinkUberjar.getAbsolutePath(),
				"-yt", flinkLibFolder.getAbsolutePath(),
				"-yn", "1",
				"-ys", "2",
				"-yjm", "768",
				"-ytm", "1024", exampleJarLocation.getAbsolutePath()},
				/* test succeeded after this string */
			"Program execution finished",
			/* prohibited strings: (we want to see "DataSink (...) (2/2) switched to FINISHED") */
			new String[]{"DataSink \\(.*\\) \\(1/1\\) switched to FINISHED"},
			RunTypes.CLI_FRONTEND, 0, true);
		LOG.info("Finished perJobYarnClusterWithParallelism()");
	}

	/**
	 * Test a fire-and-forget job submission to a YARN cluster.
	 */
	@Test(timeout = 60000)
	public void testDetachedPerJobYarnCluster() throws Exception {
		LOG.info("Starting testDetachedPerJobYarnCluster()");

		File exampleJarLocation = getTestJarPath("BatchWordCount.jar");

		testDetachedPerJobYarnClusterInternal(exampleJarLocation.getAbsolutePath());

		LOG.info("Finished testDetachedPerJobYarnCluster()");
	}

	/**
	 * Test a fire-and-forget job submission to a YARN cluster.
	 */
	@Test(timeout = 60000)
	public void testDetachedPerJobYarnClusterWithStreamingJob() throws Exception {
		LOG.info("Starting testDetachedPerJobYarnClusterWithStreamingJob()");

		File exampleJarLocation = getTestJarPath("StreamingWordCount.jar");

		testDetachedPerJobYarnClusterInternal(exampleJarLocation.getAbsolutePath());

		LOG.info("Finished testDetachedPerJobYarnClusterWithStreamingJob()");
	}

	private void testDetachedPerJobYarnClusterInternal(String job) throws Exception {
		YarnClient yc = YarnClient.createYarnClient();
		yc.init(YARN_CONFIGURATION);
		yc.start();

		// get temporary folder for writing output of wordcount example
		File tmpOutFolder = null;
		try {
			tmpOutFolder = tmp.newFolder();
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}

		// get temporary file for reading input data for wordcount example
		File tmpInFile;
		try {
			tmpInFile = tmp.newFile();
			FileUtils.writeStringToFile(tmpInFile, WordCountData.TEXT);
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}

		Runner runner = startWithArgs(new String[]{
				"run", "-m", "yarn-cluster",
				"-yj", flinkUberjar.getAbsolutePath(),
				"-yt", flinkLibFolder.getAbsolutePath(),
				"-yn", "1",
				"-yjm", "768",
				// test if the cutoff is passed correctly (only useful when larger than the value
				// of containerized.heap-cutoff-min (default: 600MB)
				"-yD", "yarn.heap-cutoff-ratio=0.7",
				"-yD", "yarn.tags=test-tag",
				"-ytm", "1024",
				"-ys", "2", // test requesting slots from YARN.
				"-p", "2",
				"--detached", job,
				"--input", tmpInFile.getAbsoluteFile().toString(),
				"--output", tmpOutFolder.getAbsoluteFile().toString()},
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
				while (yc.getApplications(EnumSet.of(YarnApplicationState.RUNNING)).size() > 0) {
					sleep(500);
				}
			} else {
				// get appId by finding the latest finished appid
				apps = yc.getApplications();
				Collections.sort(apps, new Comparator<ApplicationReport>() {
					@Override
					public int compare(ApplicationReport o1, ApplicationReport o2) {
						return o1.getApplicationId().compareTo(o2.getApplicationId()) * -1;
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
			LOG.info("The job has finished. TaskManager output files found in {}", tmpOutFolder);

			// read all output files in output folder to one output string
			String content = "";
			for (File f:listOfOutputFiles) {
				if (f.isFile()) {
					content += FileUtils.readFileToString(f) + "\n";
				}
			}
			//String content = FileUtils.readFileToString(taskmanagerOut);
			// check for some of the wordcount outputs.
			Assert.assertTrue("Expected string 'da 5' or '(all,2)' not found in string '" + content + "'", content.contains("da 5") || content.contains("(da,5)") || content.contains("(all,2)"));
			Assert.assertTrue("Expected string 'der 29' or '(mind,1)' not found in string'" + content + "'", content.contains("der 29") || content.contains("(der,29)") || content.contains("(mind,1)"));

			// check if the heap size for the TaskManager was set correctly
			File jobmanagerLog = YarnTestBase.findFile("..", new FilenameFilter() {
				@Override
				public boolean accept(File dir, String name) {
					return name.contains("jobmanager.log") && dir.getAbsolutePath().contains(id.toString());
				}
			});
			Assert.assertNotNull("Unable to locate JobManager log", jobmanagerLog);
			content = FileUtils.readFileToString(jobmanagerLog);
			// TM was started with 1024 but we cut off 70% (NOT THE DEFAULT VALUE)
			String expected = "Starting TaskManagers";
			Assert.assertTrue("Expected string '" + expected + "' not found in JobManager log: '" + jobmanagerLog + "'",
				content.contains(expected));
			expected = " (2/2) (attempt #0) to ";
			Assert.assertTrue("Expected string '" + expected + "' not found in JobManager log." +
					"This string checks that the job has been started with a parallelism of 2. Log contents: '" + jobmanagerLog + "'",
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

			//cleanup the yarn-properties file
			String confDirPath = System.getenv("FLINK_CONF_DIR");
			File configDirectory = new File(confDirPath);
			LOG.info("testDetachedPerJobYarnClusterInternal: Using configuration directory " + configDirectory.getAbsolutePath());

			// load the configuration
			LOG.info("testDetachedPerJobYarnClusterInternal: Trying to load configuration file");
			Configuration configuration = GlobalConfiguration.loadConfiguration(configDirectory.getAbsolutePath());

			try {
				File yarnPropertiesFile = FlinkYarnSessionCli.getYarnPropertiesLocation(configuration.getValue(YarnConfigOptions.PROPERTIES_FILE_LOCATION));
				if (yarnPropertiesFile.exists()) {
					LOG.info("testDetachedPerJobYarnClusterInternal: Cleaning up temporary Yarn address reference: {}", yarnPropertiesFile.getAbsolutePath());
					yarnPropertiesFile.delete();
				}
			} catch (Exception e) {
				LOG.warn("testDetachedPerJobYarnClusterInternal: Exception while deleting the JobManager address file", e);
			}

			try {
				LOG.info("testDetachedPerJobYarnClusterInternal: Closing the yarn client");
				yc.stop();
			} catch (Exception e) {
				LOG.warn("testDetachedPerJobYarnClusterInternal: Exception while close the yarn client", e);
			}
		}
	}

	/**
	 * Ensures that the YARN application tags were set properly.
	 *
	 * <p>Since YARN application tags were only added in Hadoop 2.4, but Flink still supports Hadoop 2.3, reflection is
	 * required to invoke the methods. If the method does not exist, this test passes.
	 */
	private void verifyApplicationTags(final ApplicationReport report) throws InvocationTargetException,
		IllegalAccessException {

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

		Assert.assertEquals(Collections.singleton("test-tag"), applicationTags);
	}

	@After
	public void checkForProhibitedLogContents() {
		ensureNoProhibitedStringInLogFiles(PROHIBITED_STRINGS, WHITELISTED_STRINGS);
	}
}
