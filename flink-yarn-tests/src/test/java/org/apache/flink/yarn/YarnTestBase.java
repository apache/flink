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

import org.apache.flink.client.CliFrontend;
import org.apache.flink.client.cli.CommandLineOptions;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLogger;
import org.apache.flink.yarn.cli.FlinkYarnSessionCli;

import akka.actor.Identify;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileUtil;
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
import org.apache.log4j.spi.LoggingEvent;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import scala.concurrent.Await;
import scala.concurrent.duration.FiniteDuration;

/**
 * This base class allows to use the MiniYARNCluster.
 * The cluster is re-used for all tests.
 *
 * <p>This class is located in a different package which is build after flink-dist. This way,
 * we can use the YARN uberjar of flink to start a Flink YARN session.
 *
 * <p>The test is not thread-safe. Parallel execution of tests is not possible!
 */
public abstract class YarnTestBase extends TestLogger {
	private static final Logger LOG = LoggerFactory.getLogger(YarnTestBase.class);

	protected static final PrintStream ORIGINAL_STDOUT = System.out;
	protected static final PrintStream ORIGINAL_STDERR = System.err;

	protected static final String TEST_CLUSTER_NAME_KEY = "flink-yarn-minicluster-name";

	protected static final int NUM_NODEMANAGERS = 2;

	/** The tests are scanning for these strings in the final output. */
	protected static final String[] PROHIBITED_STRINGS = {
			"Exception", // we don't want any exceptions to happen
			"Started SelectChannelConnector@0.0.0.0:8081" // Jetty should start on a random port in YARN mode.
	};

	/** These strings are white-listed, overriding teh prohibited strings. */
	protected static final String[] WHITELISTED_STRINGS = {
			"akka.remote.RemoteTransportExceptionNoStackTrace",
			// workaround for annoying InterruptedException logging:
			// https://issues.apache.org/jira/browse/YARN-1022
			"java.lang.InterruptedException",
			// very specific on purpose
			"Remote connection to [null] failed with java.net.ConnectException: Connection refused",
			"java.io.IOException: Connection reset by peer"
	};

	// Temp directory which is deleted after the unit test.
	@ClassRule
	public static TemporaryFolder tmp = new TemporaryFolder();

	protected static MiniYARNCluster yarnCluster = null;

	/**
	 * Uberjar (fat jar) file of Flink.
	 */
	protected static File flinkUberjar;

	protected static final Configuration YARN_CONFIGURATION;

	/**
	 * lib/ folder of the flink distribution.
	 */
	protected static File flinkLibFolder;

	/**
	 * Temporary folder where Flink configurations will be kept for secure run.
	 */
	protected static File tempConfPathForSecureRun = null;

	static {
		YARN_CONFIGURATION = new YarnConfiguration();
		YARN_CONFIGURATION.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 512);
		YARN_CONFIGURATION.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB, 4096); // 4096 is the available memory anyways
		YARN_CONFIGURATION.setBoolean(YarnConfiguration.YARN_MINICLUSTER_FIXED_PORTS, true);
		YARN_CONFIGURATION.setBoolean(YarnConfiguration.RM_SCHEDULER_INCLUDE_PORT_IN_NODE_NAME, true);
		YARN_CONFIGURATION.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 2);
		YARN_CONFIGURATION.setInt(YarnConfiguration.RM_MAX_COMPLETED_APPLICATIONS, 2);
		YARN_CONFIGURATION.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES, 4);
		YARN_CONFIGURATION.setInt(YarnConfiguration.DEBUG_NM_DELETE_DELAY_SEC, 3600);
		YARN_CONFIGURATION.setBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED, false);
		YARN_CONFIGURATION.setInt(YarnConfiguration.NM_VCORES, 666); // memory is overwritten in the MiniYARNCluster.
		// so we have to change the number of cores for testing.
		YARN_CONFIGURATION.setInt(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS, 20000); // 20 seconds expiry (to ensure we properly heartbeat with YARN).
	}

	public static void populateYarnSecureConfigurations(Configuration conf, String principal, String keytab) {

		conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
		conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, "true");

		conf.set(YarnConfiguration.RM_KEYTAB, keytab);
		conf.set(YarnConfiguration.RM_PRINCIPAL, principal);
		conf.set(YarnConfiguration.NM_KEYTAB, keytab);
		conf.set(YarnConfiguration.NM_PRINCIPAL, principal);

		conf.set(YarnConfiguration.RM_WEBAPP_SPNEGO_USER_NAME_KEY, principal);
		conf.set(YarnConfiguration.RM_WEBAPP_SPNEGO_KEYTAB_FILE_KEY, keytab);
		conf.set(YarnConfiguration.NM_WEBAPP_SPNEGO_USER_NAME_KEY, principal);
		conf.set(YarnConfiguration.NM_WEBAPP_SPNEGO_KEYTAB_FILE_KEY, keytab);

		conf.set("hadoop.security.auth_to_local", "RULE:[1:$1] RULE:[2:$1]");
	}

	/**
	 * Sleep a bit between the tests (we are re-using the YARN cluster for the tests).
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
	protected org.apache.flink.configuration.Configuration flinkConfiguration;

	@Before
	public void checkClusterEmpty() throws IOException, YarnException {
		if (yarnClient == null) {
			yarnClient = YarnClient.createYarnClient();
			yarnClient.init(YARN_CONFIGURATION);
			yarnClient.start();
		}

		List<ApplicationReport> apps = yarnClient.getApplications();
		for (ApplicationReport app : apps) {
			if (app.getYarnApplicationState() != YarnApplicationState.FINISHED
					&& app.getYarnApplicationState() != YarnApplicationState.KILLED
					&& app.getYarnApplicationState() != YarnApplicationState.FAILED) {
				Assert.fail("There is at least one application on the cluster is not finished." +
						"App " + app.getApplicationId() + " is in state " + app.getYarnApplicationState());
			}
		}

		flinkConfiguration = new org.apache.flink.configuration.Configuration();
	}

	/**
	 * Locate a file or directory.
	 */
	public static File findFile(String startAt, FilenameFilter fnf) {
		File root = new File(startAt);
		String[] files = root.list();
		if (files == null) {
			return null;
		}
		for (String file : files) {
			File f = new File(startAt + File.separator + file);
			if (f.isDirectory()) {
				File r = findFile(f.getAbsolutePath(), fnf);
				if (r != null) {
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

	/**
	 * A simple {@link FilenameFilter} that only accepts files if their name contains every string in the array passed
	 * to the constructor.
	 */
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
			if (excludeInPath == null) {
				for (String n: names) {
					if (!name.contains(n)) {
						return false;
					}
				}
				return true;
			} else {
				for (String n: names) {
					if (!name.contains(n)) {
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

		try (FileWriter writer = new FileWriter(yarnSiteXML)) {
			yarnConf.writeXml(writer);
			writer.flush();
		}
		return yarnSiteXML;
	}

	/**
	 * This method checks the written TaskManager and JobManager log files
	 * for exceptions.
	 *
	 * <p>WARN: Please make sure the tool doesn't find old logfiles from previous test runs.
	 * So always run "mvn clean" before running the tests here.
	 *
	 */
	public static void ensureNoProhibitedStringInLogFiles(final String[] prohibited, final String[] whitelisted) {
		File cwd = new File("target/" + YARN_CONFIGURATION.get(TEST_CLUSTER_NAME_KEY));
		Assert.assertTrue("Expecting directory " + cwd.getAbsolutePath() + " to exist", cwd.exists());
		Assert.assertTrue("Expecting directory " + cwd.getAbsolutePath() + " to be a directory", cwd.isDirectory());

		File foundFile = findFile(cwd.getAbsolutePath(), new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
			// scan each file for prohibited strings.
			File f = new File(dir.getAbsolutePath() + "/" + name);
			try {
				Scanner scanner = new Scanner(f);
				while (scanner.hasNextLine()) {
					final String lineFromFile = scanner.nextLine();
					for (String aProhibited : prohibited) {
						if (lineFromFile.contains(aProhibited)) {

							boolean whitelistedFound = false;
							for (String white : whitelisted) {
								if (lineFromFile.contains(white)) {
									whitelistedFound = true;
									break;
								}
							}

							if (!whitelistedFound) {
								// logging in FATAL to see the actual message in TRAVIS tests.
								Marker fatal = MarkerFactory.getMarker("FATAL");
								LOG.error(fatal, "Prohibited String '{}' in line '{}'", aProhibited, lineFromFile);
								return true;
							}
						}
					}

				}
			} catch (FileNotFoundException e) {
				LOG.warn("Unable to locate file: " + e.getMessage() + " file: " + f.getAbsolutePath());
			}

			return false;
			}
		});
		if (foundFile != null) {
			Scanner scanner =  null;
			try {
				scanner = new Scanner(foundFile);
			} catch (FileNotFoundException e) {
				Assert.fail("Unable to locate file: " + e.getMessage() + " file: " + foundFile.getAbsolutePath());
			}
			LOG.warn("Found a file with a prohibited string. Printing contents:");
			while (scanner.hasNextLine()) {
				LOG.warn("LINE: " + scanner.nextLine());
			}
			Assert.fail("Found a file " + foundFile + " with a prohibited string: " + Arrays.toString(prohibited));
		}
	}

	public static void sleep(int time) {
		try {
			Thread.sleep(time);
		} catch (InterruptedException e) {
			LOG.warn("Interruped", e);
		}
	}

	public static int getRunningContainers() {
		int count = 0;
		for (int nmId = 0; nmId < NUM_NODEMANAGERS; nmId++) {
			NodeManager nm = yarnCluster.getNodeManager(nmId);
			ConcurrentMap<ContainerId, Container> containers = nm.getNMContext().getContainers();
			count += containers.size();
		}
		return count;
	}

	public static void startYARNSecureMode(Configuration conf, String principal, String keytab) {
		start(conf, principal, keytab);
	}

	public static void startYARNWithConfig(Configuration conf) {
		start(conf, null, null);
	}

	private static void start(Configuration conf, String principal, String keytab) {
		// set the home directory to a temp directory. Flink on YARN is using the home dir to distribute the file
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
		flinkLibFolder = flinkUberjar.getParentFile(); // the uberjar is located in lib/
		Assert.assertNotNull("Flink flinkLibFolder not found", flinkLibFolder);
		Assert.assertTrue("lib folder not found", flinkLibFolder.exists());
		Assert.assertTrue("lib folder not found", flinkLibFolder.isDirectory());

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

			File flinkConfDirPath = findFile(flinkDistRootDir, new ContainsName(new String[]{"flink-conf.yaml"}));
			Assert.assertNotNull(flinkConfDirPath);

			if (!StringUtils.isBlank(principal) && !StringUtils.isBlank(keytab)) {
				//copy conf dir to test temporary workspace location
				tempConfPathForSecureRun = tmp.newFolder("conf");

				String confDirPath = flinkConfDirPath.getParentFile().getAbsolutePath();
				FileUtils.copyDirectory(new File(confDirPath), tempConfPathForSecureRun);

				try (FileWriter fw = new FileWriter(new File(tempConfPathForSecureRun, "flink-conf.yaml"), true);
					BufferedWriter bw = new BufferedWriter(fw);
					PrintWriter out = new PrintWriter(bw)) {

					LOG.info("writing keytab: " + keytab + " and principal: " + principal + " to config file");
					out.println("");
					out.println("#Security Configurations Auto Populated ");
					out.println(SecurityOptions.KERBEROS_LOGIN_KEYTAB.key() + ": " + keytab);
					out.println(SecurityOptions.KERBEROS_LOGIN_PRINCIPAL.key() + ": " + principal);
					out.println("");
				} catch (IOException e) {
					throw new RuntimeException("Exception occured while trying to append the security configurations.", e);
				}

				String configDir = tempConfPathForSecureRun.getAbsolutePath();

				LOG.info("Temporary Flink configuration directory to be used for secure test: {}", configDir);

				Assert.assertNotNull(configDir);

				map.put(ConfigConstants.ENV_FLINK_CONF_DIR, configDir);

			} else {
				map.put(ConfigConstants.ENV_FLINK_CONF_DIR, flinkConfDirPath.getParent());
			}

			File yarnConfFile = writeYarnSiteConfigXML(conf);
			map.put("YARN_CONF_DIR", yarnConfFile.getParentFile().getAbsolutePath());
			map.put("IN_TESTS", "yes we are in tests"); // see YarnClusterDescriptor() for more infos
			TestBaseUtils.setEnv(map);

			Assert.assertTrue(yarnCluster.getServiceState() == Service.STATE.STARTED);

			// wait for the nodeManagers to connect
			while (!yarnCluster.waitForNodeManagersToConnect(500)) {
				LOG.info("Waiting for Nodemanagers to connect");
			}
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
		startYARNWithConfig(YARN_CONFIGURATION);
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

		final int startTimeoutSeconds = 60;

		Runner runner = new Runner(
			args,
			flinkConfiguration,
			CliFrontend.getConfigurationDirectoryFromEnv(),
			type,
			0);
		runner.setName("Frontend (CLI/YARN Client) runner thread (startWithArgs()).");
		runner.start();

		for (int second = 0; second <  startTimeoutSeconds; second++) {
			sleep(1000);
			// check output for correct TaskManager startup.
			if (outContent.toString().contains(startedAfterString)
					|| errContent.toString().contains(startedAfterString)) {
				LOG.info("Found expected output in redirected streams");
				return runner;
			}
			// check if thread died
			if (!runner.isAlive()) {
				sendOutput();
				if (runner.getRunnerError() != null) {
					throw new RuntimeException("Runner failed with exception.", runner.getRunnerError());
				}
				Assert.fail("Runner thread died before the test was finished.");
			}
		}

		sendOutput();
		Assert.fail("During the timeout period of " + startTimeoutSeconds + " seconds the " +
				"expected string did not show up");
		return null;
	}

	protected void runWithArgs(String[] args, String terminateAfterString, String[] failOnStrings, RunTypes type, int returnCode) {
		runWithArgs(args, terminateAfterString, failOnStrings, type, returnCode, false);
	}

	/**
	 * The test has been passed once the "terminateAfterString" has been seen.
	 * @param args Command line arguments for the runner
	 * @param terminateAfterString the runner is searching the stdout and stderr for this string. as soon as it appears, the test has passed
	 * @param failOnPatterns The runner is searching stdout and stderr for the pattern (regexp) specified here. If one appears, the test has failed
	 * @param type Set the type of the runner
	 * @param expectedReturnValue Expected return code from the runner.
	 * @param checkLogForTerminateString  If true, the runner checks also the log4j logger for the terminate string
	 */
	protected void runWithArgs(String[] args, String terminateAfterString, String[] failOnPatterns, RunTypes type, int expectedReturnValue, boolean checkLogForTerminateString) {
		LOG.info("Running with args {}", Arrays.toString(args));

		outContent = new ByteArrayOutputStream();
		errContent = new ByteArrayOutputStream();
		System.setOut(new PrintStream(outContent));
		System.setErr(new PrintStream(errContent));

		// we wait for at most three minutes
		final int startTimeoutSeconds = 180;
		final long deadline = System.currentTimeMillis() + (startTimeoutSeconds * 1000);

		Runner runner = new Runner(
			args,
			flinkConfiguration,
			CliFrontend.getConfigurationDirectoryFromEnv(),
			type,
			expectedReturnValue);
		runner.start();

		boolean expectedStringSeen = false;
		boolean testPassedFromLog4j = false;
		do {
			sleep(1000);
			String outContentString = outContent.toString();
			String errContentString = errContent.toString();
			if (failOnPatterns != null) {
				for (String failOnString : failOnPatterns) {
					Pattern pattern = Pattern.compile(failOnString);
					if (pattern.matcher(outContentString).find() || pattern.matcher(errContentString).find()) {
						LOG.warn("Failing test. Output contained illegal string '" + failOnString + "'");
						sendOutput();
						// stopping runner.
						runner.sendStop();
						Assert.fail("Output contained illegal string '" + failOnString + "'");
					}
				}
			}
			// check output for the expected terminateAfterString.
			if (checkLogForTerminateString) {
				LoggingEvent matchedEvent = UtilsTest.getEventContainingString(terminateAfterString);
				if (matchedEvent != null) {
					testPassedFromLog4j = true;
					LOG.info("Found expected output in logging event {}", matchedEvent);
				}

			}

			if (outContentString.contains(terminateAfterString) || errContentString.contains(terminateAfterString) || testPassedFromLog4j) {
				expectedStringSeen = true;
				LOG.info("Found expected output in redirected streams");
				// send "stop" command to command line interface
				LOG.info("RunWithArgs: request runner to stop");
				runner.sendStop();
				// wait for the thread to stop
				try {
					runner.join(30000);
				}
				catch (InterruptedException e) {
					LOG.warn("Interrupted while stopping runner", e);
				}
				LOG.warn("RunWithArgs runner stopped.");
			}
			else {
				// check if thread died
				if (!runner.isAlive()) {
					// leave loop: the runner died, so we can not expect new strings to show up.
					break;
				}
			}
		}
		while (runner.getRunnerError() == null && !expectedStringSeen && System.currentTimeMillis() < deadline);

		sendOutput();

		if (runner.getRunnerError() != null) {
			// this lets the test fail.
			throw new RuntimeException("Runner failed", runner.getRunnerError());
		}
		Assert.assertTrue("During the timeout period of " + startTimeoutSeconds + " seconds the " +
				"expected string did not show up", expectedStringSeen);

		LOG.info("Test was successful");
	}

	protected static void sendOutput() {
		System.setOut(ORIGINAL_STDOUT);
		System.setErr(ORIGINAL_STDERR);

		LOG.info("Sending stdout content through logger: \n\n{}\n\n", outContent.toString());
		LOG.info("Sending stderr content through logger: \n\n{}\n\n", errContent.toString());
	}

	/**
	 * Utility class to run yarn jobs.
	 */
	protected static class Runner extends Thread {
		private final String[] args;
		private final org.apache.flink.configuration.Configuration configuration;
		private final String configurationDirectory;
		private final int expectedReturnValue;

		private RunTypes type;
		private FlinkYarnSessionCli yCli;
		private Throwable runnerError;

		public Runner(
				String[] args,
				org.apache.flink.configuration.Configuration configuration,
				String configurationDirectory,
				RunTypes type,
				int expectedReturnValue) {

			this.args = args;
			this.configuration = Preconditions.checkNotNull(configuration);
			this.configurationDirectory = Preconditions.checkNotNull(configurationDirectory);
			this.type = type;
			this.expectedReturnValue = expectedReturnValue;
		}

		@Override
		public void run() {
			try {
				int returnValue;
				switch (type) {
					case YARN_SESSION:
						yCli = new FlinkYarnSessionCli(
							"",
							"",
							false);
						returnValue = yCli.run(args, configuration, configurationDirectory);
						break;
					case CLI_FRONTEND:
						TestingCLI cli;
						try {
							cli = new TestingCLI();
							returnValue = cli.parseParameters(args);
						} catch (Exception e) {
							throw new RuntimeException("Failed to execute the following args with CliFrontend: "
								+ Arrays.toString(args), e);
						}

						final ClusterClient client = cli.getClusterClient();
						try {
							// check if the JobManager is still alive after running the job
							final FiniteDuration finiteDuration = new FiniteDuration(10, TimeUnit.SECONDS);
							ActorGateway jobManagerGateway = client.getJobManagerGateway();
							Await.ready(jobManagerGateway.ask(new Identify(true), finiteDuration), finiteDuration);
						} catch (Exception e) {
							throw new RuntimeException("It seems like the JobManager died although it should still be alive");
						}
						// verify we would have shut down anyways and then shutdown
						Mockito.verify(cli.getSpiedClusterClient()).shutdown();
						client.shutdown();

						break;
					default:
						throw new RuntimeException("Unknown type " + type);
				}

				if (returnValue != this.expectedReturnValue) {
					Assert.fail("The YARN session returned with unexpected value=" + returnValue + " expected=" + expectedReturnValue);
				}
			} catch (Throwable t) {
				LOG.info("Runner stopped with exception", t);
				// save error.
				this.runnerError = t;
			}
		}

		/** Stops the Yarn session. */
		public void sendStop() {
			if (yCli != null) {
				yCli.stop();
			}
		}

		public Throwable getRunnerError() {
			return runnerError;
		}
	}

	// -------------------------- Tear down -------------------------- //

	@AfterClass
	public static void teardown() throws Exception {

		LOG.info("Stopping MiniYarn Cluster");
		yarnCluster.stop();

		// Unset FLINK_CONF_DIR, as it might change the behavior of other tests
		Map<String, String> map = new HashMap<>(System.getenv());
		map.remove(ConfigConstants.ENV_FLINK_CONF_DIR);
		map.remove("YARN_CONF_DIR");
		map.remove("IN_TESTS");
		TestBaseUtils.setEnv(map);

		if (tempConfPathForSecureRun != null) {
			FileUtil.fullyDelete(tempConfPathForSecureRun);
			tempConfPathForSecureRun = null;
		}

		// When we are on travis, we copy the temp files of JUnit (containing the MiniYARNCluster log files)
		// to <flinkRoot>/target/flink-yarn-tests-*.
		// The files from there are picked up by the ./tools/travis_watchdog.sh script
		// to upload them to Amazon S3.
		if (isOnTravis()) {
			File target = new File("../target" + YARN_CONFIGURATION.get(TEST_CLUSTER_NAME_KEY));
			if (!target.mkdirs()) {
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

	private static class TestingCLI extends CliFrontend {

		private ClusterClient originalClusterClient;
		private ClusterClient spiedClusterClient;

		public TestingCLI() throws Exception {}

		@Override
		protected ClusterClient createClient(CommandLineOptions options, PackagedProgram program) throws Exception {
			// mock the returned ClusterClient to disable shutdown and verify shutdown behavior later on
			originalClusterClient = super.createClient(options, program);
			spiedClusterClient = Mockito.spy(originalClusterClient);
			Mockito.doNothing().when(spiedClusterClient).shutdown();
			return spiedClusterClient;
		}

		public ClusterClient getClusterClient() {
			return originalClusterClient;
		}

		public ClusterClient getSpiedClusterClient() {
			return spiedClusterClient;
		}

	}
}
