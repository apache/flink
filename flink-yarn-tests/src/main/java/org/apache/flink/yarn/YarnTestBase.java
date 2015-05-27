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

import org.apache.commons.io.FileUtils;
import org.apache.flink.client.CliFrontend;
import org.apache.flink.client.FlinkYarnSessionCli;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ConcurrentMap;


/**
 * This base class allows to use the MiniYARNCluster.
 * The cluster is re-used for all tests.
 *
 * This class is located in a different package which is build after flink-dist. This way,
 * we can use the YARN uberjar of flink to start a Flink YARN session.
 *
 * The test is not thread-safe. Parallel execution of tests is not possible!
 */
public abstract class YarnTestBase {
	private static final Logger LOG = LoggerFactory.getLogger(YarnTestBase.class);

	protected final static PrintStream originalStdout = System.out;
	protected final static PrintStream originalStderr = System.err;

	protected static String TEST_CLUSTER_NAME_KEY = "flink-yarn-minicluster-name";

	protected final static int NUM_NODEMANAGERS = 2;

	// The tests are scanning for these strings in the final output.
	protected final static String[] PROHIBITED_STRINGS = {
			"Exception", // we don't want any exceptions to happen
			"Started SelectChannelConnector@0.0.0.0:8081" // Jetty should start on a random port in YARN mode.
	};

	// Temp directory which is deleted after the unit test.
	@ClassRule
	public static TemporaryFolder tmp = new TemporaryFolder();

	protected static MiniYARNCluster yarnCluster = null;

	protected static File flinkUberjar;

	protected static final Configuration yarnConfiguration;

	static {
		yarnConfiguration = new YarnConfiguration();
		yarnConfiguration.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 512);
		yarnConfiguration.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB, 4096); // 4096 is the available memory anyways
		yarnConfiguration.setBoolean(YarnConfiguration.YARN_MINICLUSTER_FIXED_PORTS, true);
		yarnConfiguration.setBoolean(YarnConfiguration.RM_SCHEDULER_INCLUDE_PORT_IN_NODE_NAME, true);
		yarnConfiguration.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 2);
		yarnConfiguration.setInt(YarnConfiguration.RM_MAX_COMPLETED_APPLICATIONS, 2);
		yarnConfiguration.setInt(YarnConfiguration.DEBUG_NM_DELETE_DELAY_SEC, 3600);
		yarnConfiguration.setBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED, false);
		yarnConfiguration.setInt(YarnConfiguration.NM_VCORES, 666); // memory is overwritten in the MiniYARNCluster.
		// so we have to change the number of cores for testing.
		yarnConfiguration.setInt(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS, 20000); // 20 seconds expiry (to ensure we properly heartbeat with YARN).
	}



	/**
	 * Sleep a bit between the tests (we are re-using the YARN cluster for the tests)
	 */
	@After
	public void sleep() {
		try {
			Thread.sleep(500);
		} catch (InterruptedException e) {
			Assert.fail("Should not happen");
		}
	}

	private YarnClient yarnClient = null;
	@Before
	public void checkClusterEmpty() throws IOException, YarnException {
		if(yarnClient == null) {
			yarnClient = YarnClient.createYarnClient();
			yarnClient.init(yarnConfiguration);
			yarnClient.start();
		}

		List<ApplicationReport> apps = yarnClient.getApplications();
		for(ApplicationReport app : apps) {
			if(app.getYarnApplicationState() != YarnApplicationState.FINISHED
					&& app.getYarnApplicationState() != YarnApplicationState.KILLED
					&& app.getYarnApplicationState() != YarnApplicationState.FAILED) {
				Assert.fail("There is at least one application on the cluster is not finished." +
						"App "+app.getApplicationId()+" is in state "+app.getYarnApplicationState());
			}
		}
	}

	/**
	 * Locate a file or directory
	 */
	public static File findFile(String startAt, FilenameFilter fnf) {
		File root = new File(startAt);
		String[] files = root.list();
		if(files == null) {
			return null;
		}
		for(String file : files) {
			File f = new File(startAt + File.separator + file);
			if(f.isDirectory()) {
				File r = findFile(f.getAbsolutePath(), fnf);
				if(r != null) {
					return r;
				}
			} else if (fnf.accept(f.getParentFile(), f.getName())) {
				return f;
			}
		}
		return null;
	}

	/**
	 * Filter to find root dir of the flink-yarn dist.
	 */
	public static class RootDirFilenameFilter implements FilenameFilter {
		@Override
		public boolean accept(File dir, String name) {
			return name.startsWith("flink-dist") && name.endsWith(".jar") && dir.toString().contains("/lib");
		}
	}

	public static class ContainsName implements FilenameFilter {
		private String[] names;
		private String excludeInPath = null;

		/**
		 * @param names which have to be included in the filename.
		 */
		public ContainsName(String[] names) {
			this.names = names;
		}

		public ContainsName(String[] names, String excludeInPath) {
			this.names = names;
			this.excludeInPath = excludeInPath;
		}

		@Override
		public boolean accept(File dir, String name) {
			if(excludeInPath == null) {
				for(String n: names) {
					if(!name.contains(n)) {
						return false;
					}
				}
				return true;
			} else {
				for(String n: names) {
					if(!name.contains(n)) {
						return false;
					}
				}
				return !dir.toString().contains(excludeInPath);
			}
		}
	}

	public static File writeYarnSiteConfigXML(Configuration yarnConf) throws IOException {
		tmp.create();
		File yarnSiteXML = new File(tmp.newFolder().getAbsolutePath() + "/yarn-site.xml");

		FileWriter writer = new FileWriter(yarnSiteXML);
		yarnConf.writeXml(writer);
		writer.flush();
		writer.close();
		return yarnSiteXML;
	}

	/**
	 * This method checks the written TaskManager and JobManager log files
	 * for exceptions.
	 *
	 * WARN: Please make sure the tool doesn't find old logfiles from previous test runs.
	 * So always run "mvn clean" before running the tests here.
	 *
	 */
	public static void ensureNoProhibitedStringInLogFiles(final String[] prohibited) {
		File cwd = new File("target/"+yarnConfiguration.get(TEST_CLUSTER_NAME_KEY));
		Assert.assertTrue("Expecting directory "+cwd.getAbsolutePath()+" to exist", cwd.exists());
		Assert.assertTrue("Expecting directory "+cwd.getAbsolutePath()+" to be a directory", cwd.isDirectory());
		File foundFile = findFile(cwd.getAbsolutePath(), new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
			// scan each file for prohibited strings.
			File f = new File(dir.getAbsolutePath()+ "/" + name);
			try {
				Scanner scanner = new Scanner(f);
				while (scanner.hasNextLine()) {
					final String lineFromFile = scanner.nextLine();
					for (String aProhibited : prohibited) {
						if (lineFromFile.contains(aProhibited)) {
							// logging in FATAL to see the actual message in TRAVIS tests.
							Marker fatal = MarkerFactory.getMarker("FATAL");
							LOG.error(fatal, "Prohibited String '{}' in line '{}'", aProhibited, lineFromFile);
							return true;
						}
					}

				}
			} catch (FileNotFoundException e) {
				LOG.warn("Unable to locate file: "+e.getMessage()+" file: "+f.getAbsolutePath());
			}

			return false;
			}
		});
		if(foundFile != null) {
			Scanner scanner =  null;
			try {
				scanner = new Scanner(foundFile);
			} catch (FileNotFoundException e) {
				Assert.fail("Unable to locate file: "+e.getMessage()+" file: "+foundFile.getAbsolutePath());
			}
			LOG.warn("Found a file with a prohibited string. Printing contents:");
			while (scanner.hasNextLine()) {
				LOG.warn("LINE: "+scanner.nextLine());
			}
			Assert.fail("Found a file "+foundFile+" with a prohibited string: "+Arrays.toString(prohibited));
		}
	}

	public static void sleep(int time) {
		try {
			Thread.sleep(time);
		} catch (InterruptedException e) {
			LOG.warn("Interruped",e);
		}
	}

	public static int getRunningContainers() {
		int count = 0;
		for(int nmId = 0; nmId < NUM_NODEMANAGERS; nmId++) {
			NodeManager nm = yarnCluster.getNodeManager(nmId);
			ConcurrentMap<ContainerId, Container> containers = nm.getNMContext().getContainers();
			count += containers.size();
		}
		return count;
	}

	public static void startYARNWithConfig(Configuration conf) {
		// set the home directory to a tmp directory. Flink on YARN is using the home dir to distribute the file
		File homeDir = null;
		try {
			homeDir = tmp.newFolder();
		} catch (IOException e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
		System.setProperty("user.home", homeDir.getAbsolutePath());
		String uberjarStartLoc = "..";
		LOG.info("Trying to locate uberjar in {}", new File(uberjarStartLoc));
		flinkUberjar = findFile(uberjarStartLoc, new RootDirFilenameFilter());
		Assert.assertNotNull("Flink uberjar not found", flinkUberjar);
		String flinkDistRootDir = flinkUberjar.getParentFile().getParent();

		if (!flinkUberjar.exists()) {
			Assert.fail("Unable to locate yarn-uberjar.jar");
		}

		try {
			LOG.info("Starting up MiniYARNCluster");
			if (yarnCluster == null) {
				yarnCluster = new MiniYARNCluster(conf.get(YarnTestBase.TEST_CLUSTER_NAME_KEY), NUM_NODEMANAGERS, 1, 1);

				yarnCluster.init(conf);
				yarnCluster.start();
			}

			Map<String, String> map = new HashMap<String, String>(System.getenv());
			File flinkConfFilePath = findFile(flinkDistRootDir, new ContainsName(new String[] {"flink-conf.yaml"}));
			Assert.assertNotNull(flinkConfFilePath);
			map.put("FLINK_CONF_DIR", flinkConfFilePath.getParent());
			File yarnConfFile = writeYarnSiteConfigXML(conf);
			map.put("YARN_CONF_DIR", yarnConfFile.getParentFile().getAbsolutePath());
			map.put("IN_TESTS", "yes we are in tests"); // see FlinkYarnClient() for more infos
			TestBaseUtils.setEnv(map);

			Assert.assertTrue(yarnCluster.getServiceState() == Service.STATE.STARTED);
		} catch (Exception ex) {
			ex.printStackTrace();
			LOG.error("setup failure", ex);
			Assert.fail();
		}
	}

	/**
	 * Default @BeforeClass impl. Overwrite this for passing a different configuration
	 */
	@BeforeClass
	public static void setup() {
		startYARNWithConfig(yarnConfiguration);
	}

	// -------------------------- Runner -------------------------- //

	protected static ByteArrayOutputStream outContent;
	protected static ByteArrayOutputStream errContent;
	enum RunTypes {
		YARN_SESSION, CLI_FRONTEND
	}

	/**
	 * This method returns once the "startedAfterString" has been seen.
	 */
	protected Runner startWithArgs(String[] args, String startedAfterString, RunTypes type) {
		LOG.info("Running with args {}", Arrays.toString(args));

		outContent = new ByteArrayOutputStream();
		errContent = new ByteArrayOutputStream();
		System.setOut(new PrintStream(outContent));
		System.setErr(new PrintStream(errContent));


		final int START_TIMEOUT_SECONDS = 60;

		Runner runner = new Runner(args, type);
		runner.setName("Frontend (CLI/YARN Client) runner thread (runWithArgs()).");
		runner.start();

		for(int second = 0; second <  START_TIMEOUT_SECONDS; second++) {
			sleep(1000);
			// check output for correct TaskManager startup.
			if(outContent.toString().contains(startedAfterString)
					|| errContent.toString().contains(startedAfterString) ) {
				LOG.info("Found expected output in redirected streams");
				return runner;
			}
			// check if thread died
			if(!runner.isAlive()) {
				sendOutput();
				Assert.fail("Runner thread died before the test was finished. Return value = "+runner.getReturnValue());
			}
		}

		sendOutput();
		Assert.fail("During the timeout period of " + START_TIMEOUT_SECONDS + " seconds the " +
				"expected string did not show up");
		return null;
	}

	/**
	 * The test has been passed once the "terminateAfterString" has been seen.
	 */
	protected void runWithArgs(String[] args, String terminateAfterString, String[] failOnStrings, RunTypes type, int returnCode) {
		LOG.info("Running with args {}", Arrays.toString(args));

		outContent = new ByteArrayOutputStream();
		errContent = new ByteArrayOutputStream();
		System.setOut(new PrintStream(outContent));
		System.setErr(new PrintStream(errContent));


		final int START_TIMEOUT_SECONDS = 60;

		Runner runner = new Runner(args, type);
		runner.start();

		boolean expectedStringSeen = false;
		for(int second = 0; second <  START_TIMEOUT_SECONDS; second++) {
			sleep(1000);
			String outContentString = outContent.toString();
			String errContentString = errContent.toString();
			if(failOnStrings != null) {
				for (String failOnString : failOnStrings) {
					if (outContentString.contains(failOnString)
							|| errContentString.contains(failOnString)) {
						LOG.warn("Failing test. Output contained illegal string '" + failOnString + "'");
						sendOutput();
						// stopping runner.
						runner.sendStop();
						Assert.fail("Output contained illegal string '" + failOnString + "'");
					}
				}
			}
			// check output for correct TaskManager startup.
			if(outContentString.contains(terminateAfterString)
					|| errContentString.contains(terminateAfterString) ) {
				expectedStringSeen = true;
				LOG.info("Found expected output in redirected streams");
				// send "stop" command to command line interface
				LOG.info("RunWithArgs: request runner to stop");
				runner.sendStop();
				// wait for the thread to stop
				try {
					runner.join(10000);
				} catch (InterruptedException e) {
					LOG.debug("Interrupted while stopping runner", e);
				}
				LOG.warn("RunWithArgs runner stopped.");
				break;
			}
			// check if thread died
			if(!runner.isAlive()) {
				sendOutput();
				Assert.fail("Runner thread died before the test was finished. Return value = " +runner.getReturnValue());
			}
		}
		sendOutput();
		Assert.assertTrue("During the timeout period of " + START_TIMEOUT_SECONDS + " seconds the " +
				"expected string did not show up", expectedStringSeen);

		// check for 0 return code
		Assert.assertTrue("Expecting return value == "+returnCode, runner.getReturnValue() == returnCode);
		LOG.info("Test was successful");
	}

	protected static void sendOutput() {
		System.setOut(originalStdout);
		System.setErr(originalStderr);

		LOG.info("Sending stdout content through logger: \n\n{}\n\n", outContent.toString());
		LOG.info("Sending stderr content through logger: \n\n{}\n\n", errContent.toString());
	}

	public static class Runner extends Thread {
		private final String[] args;
		private int returnValue;
		private RunTypes type;
		private FlinkYarnSessionCli yCli;

		public Runner(String[] args, RunTypes type) {
			this.args = args;
			this.type = type;
		}

		public int getReturnValue() {
			return returnValue;
		}

		@Override
		public void run() {
			switch(type) {
				case YARN_SESSION:
					yCli = new FlinkYarnSessionCli("", "");
					returnValue = yCli.run(args);
					break;
				case CLI_FRONTEND:
					try {
						CliFrontend cli = new CliFrontend();
						returnValue = cli.parseParameters(args);
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
					break;
				default:
					throw new RuntimeException("Unknown type " + type);
			}

			if(returnValue != 0) {
				Assert.fail("The YARN session returned with non-null value="+returnValue);
			}
		}

		public void sendStop() {
			if(yCli != null) {
				yCli.stop();
			}
		}
	}

	// -------------------------- Tear down -------------------------- //

	@AfterClass
	public static void tearDown() {
		//shutdown YARN cluster
		if (yarnCluster != null) {
			LOG.info("Shutting down MiniYarn cluster");
			yarnCluster.stop();
			yarnCluster = null;
		}
		// When we are on travis, we copy the tmp files of JUnit (containing the MiniYARNCluster log files)
		// to <flinkRoot>/target/flink-yarn-tests-*.
		// The files from there are picked up by the ./tools/travis_watchdog.sh script
		// to upload them to Amazon S3.
		if(isOnTravis()) {
			File target = new File("../target/"+yarnConfiguration.get(TEST_CLUSTER_NAME_KEY));
			if(!target.mkdirs()) {
				LOG.warn("Error creating dirs to {}", target);
			}
			File src = tmp.getRoot();
			LOG.info("copying the final files from {} to {}", src.getAbsolutePath(), target.getAbsolutePath());
			try {
				FileUtils.copyDirectoryToDirectory(src, target);
			} catch (IOException e) {
				LOG.warn("Error copying the final files from {} to {}: msg: {}", src.getAbsolutePath(), target.getAbsolutePath(), e.getMessage(), e);
			}
		}
	}

	public static boolean isOnTravis() {
		return System.getenv("TRAVIS") != null && System.getenv("TRAVIS").equals("true");
	}

}
