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
import org.apache.flink.client.cli.CliFrontend;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.RunnableWithException;
import org.apache.flink.yarn.cli.FlinkYarnSessionCli;
import org.apache.flink.yarn.util.TestUtils;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.records.ApplicationId;
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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkState;
import static org.junit.Assert.assertEquals;

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
	private static final InputStream ORIGINAL_STDIN = System.in;

	protected static final String TEST_CLUSTER_NAME_KEY = "flink-yarn-minicluster-name";

	protected static final int NUM_NODEMANAGERS = 2;

	/** The tests are scanning for these strings in the final output. */
	protected static final String[] PROHIBITED_STRINGS = {
			"Exception", // we don't want any exceptions to happen
			"Started SelectChannelConnector@0.0.0.0:8081" // Jetty should start on a random port in YARN mode.
	};

	/** These strings are white-listed, overriding the prohibited strings. */
	protected static final Pattern[] WHITELISTED_STRINGS = {
		Pattern.compile("akka\\.remote\\.RemoteTransportExceptionNoStackTrace"),
		// workaround for annoying InterruptedException logging:
		// https://issues.apache.org/jira/browse/YARN-1022
		Pattern.compile("java\\.lang\\.InterruptedException"),
		// very specific on purpose; whitelist meaningless exceptions that occur during akka shutdown:
		Pattern.compile("Remote connection to \\[null\\] failed with java.net.ConnectException: Connection refused"),
		Pattern.compile("Remote connection to \\[null\\] failed with java.nio.channels.NotYetConnectedException"),
		Pattern.compile("java\\.io\\.IOException: Connection reset by peer"),
		Pattern.compile("Association with remote system \\[akka.tcp://flink@[^]]+\\] has failed, address is now gated for \\[50\\] ms. Reason: \\[Association failed with \\[akka.tcp://flink@[^]]+\\]\\] Caused by: \\[java.net.ConnectException: Connection refused: [^]]+\\]"),

		// filter out expected ResourceManagerException caused by intended shutdown request
		Pattern.compile(YarnResourceManagerDriver.ERROR_MESSAGE_ON_SHUTDOWN_REQUEST),

		// this can happen in Akka 2.4 on shutdown.
		Pattern.compile("java\\.util\\.concurrent\\.RejectedExecutionException: Worker has already been shutdown"),

		Pattern.compile("org\\.apache\\.flink.util\\.FlinkException: Stopping JobMaster"),
		Pattern.compile("org\\.apache\\.flink.util\\.FlinkException: JobManager is shutting down\\."),
		Pattern.compile("lost the leadership."),

		Pattern.compile("akka.remote.transport.netty.NettyTransport.*Remote connection to \\[[^]]+\\] failed with java.io.IOException: Broken pipe")
	};

	// Temp directory which is deleted after the unit test.
	@ClassRule
	public static TemporaryFolder tmp = new TemporaryFolder();

	// Temp directory for mini hdfs
	@ClassRule
	public static TemporaryFolder tmpHDFS = new TemporaryFolder();

	protected static MiniYARNCluster yarnCluster = null;

	protected static MiniDFSCluster miniDFSCluster = null;

	/**
	 * Uberjar (fat jar) file of Flink.
	 */
	protected static File flinkUberjar;

	protected static final YarnConfiguration YARN_CONFIGURATION;

	/**
	 * lib/ folder of the flink distribution.
	 */
	protected static File flinkLibFolder;

	/**
	 * Temporary folder where Flink configurations will be kept for secure run.
	 */
	protected static File tempConfPathForSecureRun = null;

	protected static File yarnSiteXML = null;
	protected static File hdfsSiteXML = null;

	private YarnClient yarnClient = null;

	private static org.apache.flink.configuration.Configuration globalConfiguration;

	protected org.apache.flink.configuration.Configuration flinkConfiguration;

	static {
		YARN_CONFIGURATION = new YarnConfiguration();
		YARN_CONFIGURATION.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 32);
		YARN_CONFIGURATION.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB, 4096); // 4096 is the available memory anyways
		YARN_CONFIGURATION.setBoolean(YarnConfiguration.RM_SCHEDULER_INCLUDE_PORT_IN_NODE_NAME, true);
		YARN_CONFIGURATION.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 2);
		YARN_CONFIGURATION.setInt(YarnConfiguration.RM_MAX_COMPLETED_APPLICATIONS, 2);
		YARN_CONFIGURATION.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES, 4);
		YARN_CONFIGURATION.setInt(YarnConfiguration.DEBUG_NM_DELETE_DELAY_SEC, 3600);
		YARN_CONFIGURATION.setBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED, false);
		YARN_CONFIGURATION.setInt(YarnConfiguration.NM_VCORES, 666); // memory is overwritten in the MiniYARNCluster.
		// so we have to change the number of cores for testing.
		YARN_CONFIGURATION.setInt(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS, 20000); // 20 seconds expiry (to ensure we properly heartbeat with YARN).
		YARN_CONFIGURATION.setFloat(YarnConfiguration.NM_MAX_PER_DISK_UTILIZATION_PERCENTAGE, 99.0F);

		YARN_CONFIGURATION.set(YarnConfiguration.YARN_APPLICATION_CLASSPATH, getYarnClasspath());
	}

	/**
	 * Searches for the yarn.classpath file generated by the "dependency:build-classpath" maven plugin in
	 * "flink-yarn-tests".
	 * @return a classpath suitable for running all YARN-launched JVMs
	 */
	private static String getYarnClasspath() {
		final String start = "../flink-yarn-tests";
		try {
			File classPathFile = TestUtils.findFile(start, (dir, name) -> name.equals("yarn.classpath"));
			return FileUtils.readFileToString(classPathFile); // potential NPE is supposed to be fatal
		} catch (Throwable t) {
			LOG.error("Error while getting YARN classpath in {}", new File(start).getAbsoluteFile(), t);
			throw new RuntimeException("Error while getting YARN classpath", t);
		}
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

	@Before
	public void setupYarnClient() {
		if (yarnClient == null) {
			yarnClient = YarnClient.createYarnClient();
			yarnClient.init(getYarnConfiguration());
			yarnClient.start();
		}

		flinkConfiguration = new org.apache.flink.configuration.Configuration(globalConfiguration);
	}

	/**
	 * Sleep a bit between the tests (we are re-using the YARN cluster for the tests).
	 */
	@After
	public void shutdownYarnClient() {
		yarnClient.stop();
	}

	protected void runTest(RunnableWithException test) throws Exception {
		// wrapping the cleanup logic in an AutoClosable automatically suppresses additional exceptions
		try (final CleanupYarnApplication ignored = new CleanupYarnApplication()) {
			test.run();
		}
	}

	private class CleanupYarnApplication implements AutoCloseable {
		@Override
		public void close() throws Exception {
			Deadline deadline = Deadline.now().plus(Duration.ofSeconds(10));

			boolean isAnyJobRunning = yarnClient.getApplications().stream()
				.anyMatch(YarnTestBase::isApplicationRunning);

			while (deadline.hasTimeLeft() && isAnyJobRunning) {
				try {
					Thread.sleep(500);
				} catch (InterruptedException e) {
					Assert.fail("Should not happen");
				}
				isAnyJobRunning = yarnClient.getApplications().stream()
					.anyMatch(YarnTestBase::isApplicationRunning);
			}

			if (isAnyJobRunning) {
				final List<String> runningApps = yarnClient.getApplications().stream()
					.filter(YarnTestBase::isApplicationRunning)
					.map(app -> "App " + app.getApplicationId() + " is in state " + app.getYarnApplicationState() + '.')
					.collect(Collectors.toList());
				if (!runningApps.isEmpty()) {
					Assert.fail("There is at least one application on the cluster that is not finished." + runningApps);
				}
			}
		}
	}

	private static boolean isApplicationRunning(ApplicationReport app) {
		final YarnApplicationState yarnApplicationState = app.getYarnApplicationState();
		return yarnApplicationState != YarnApplicationState.FINISHED
			&& app.getYarnApplicationState() != YarnApplicationState.KILLED
			&& app.getYarnApplicationState() != YarnApplicationState.FAILED;
	}

	@Nullable
	protected YarnClient getYarnClient() {
		return yarnClient;
	}

	protected static YarnConfiguration getYarnConfiguration() {
		return YARN_CONFIGURATION;
	}

	@Nonnull
	YarnClusterDescriptor createYarnClusterDescriptor(org.apache.flink.configuration.Configuration flinkConfiguration) {
		final YarnClusterDescriptor yarnClusterDescriptor = createYarnClusterDescriptorWithoutLibDir(flinkConfiguration);
		yarnClusterDescriptor.addShipFiles(Collections.singletonList(flinkLibFolder));
		return yarnClusterDescriptor;
	}

	YarnClusterDescriptor createYarnClusterDescriptorWithoutLibDir(org.apache.flink.configuration.Configuration flinkConfiguration) {
		final YarnClusterDescriptor yarnClusterDescriptor = YarnTestUtils.createClusterDescriptorWithLogging(
				tempConfPathForSecureRun.getAbsolutePath(),
				flinkConfiguration,
				YARN_CONFIGURATION,
				yarnClient,
				true);
		yarnClusterDescriptor.setLocalJarPath(new Path(flinkUberjar.toURI()));
		return yarnClusterDescriptor;
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

	// write yarn-site.xml to target/test-classes so that flink pick can pick up this when
	// initializing YarnClient properly from classpath
	public static void writeYarnSiteConfigXML(Configuration yarnConf, File targetFolder) throws IOException {
		yarnSiteXML = new File(targetFolder, "/yarn-site.xml");
		try (FileWriter writer = new FileWriter(yarnSiteXML)) {
			yarnConf.writeXml(writer);
			writer.flush();
		}
	}

	private static void writeHDFSSiteConfigXML(Configuration coreSite, File targetFolder) throws IOException {
		hdfsSiteXML = new File(targetFolder, "/hdfs-site.xml");
		try (FileWriter writer = new FileWriter(hdfsSiteXML)) {
			coreSite.writeXml(writer);
			writer.flush();
		}
	}

	/**
	 * This method checks the written TaskManager and JobManager log files
	 * for exceptions.
	 *
	 * <p>WARN: Please make sure the tool doesn't find old logfiles from previous test runs.
	 * So always run "mvn clean" before running the tests here.
	 *
	 */
	public static void ensureNoProhibitedStringInLogFiles(final String[] prohibited, final Pattern[] whitelisted) {
		File cwd = new File("target/" + YARN_CONFIGURATION.get(TEST_CLUSTER_NAME_KEY));
		Assert.assertTrue("Expecting directory " + cwd.getAbsolutePath() + " to exist", cwd.exists());
		Assert.assertTrue("Expecting directory " + cwd.getAbsolutePath() + " to be a directory", cwd.isDirectory());

		List<String> prohibitedExcerpts = new ArrayList<>();
		File foundFile = TestUtils.findFile(cwd.getAbsolutePath(), new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
			// scan each file for prohibited strings.
			File logFile = new File(dir.getAbsolutePath() + "/" + name);
			try {
				BufferingScanner scanner = new BufferingScanner(new Scanner(logFile), 10);
				while (scanner.hasNextLine()) {
					final String lineFromFile = scanner.nextLine();
					for (String aProhibited : prohibited) {
						if (lineFromFile.contains(aProhibited)) {

							boolean whitelistedFound = false;
							for (Pattern whitelistPattern : whitelisted) {
								Matcher whitelistMatch = whitelistPattern.matcher(lineFromFile);
								if (whitelistMatch.find()) {
									whitelistedFound = true;
									break;
								}
							}

							if (!whitelistedFound) {
								// logging in FATAL to see the actual message in CI tests.
								Marker fatal = MarkerFactory.getMarker("FATAL");
								LOG.error(fatal, "Prohibited String '{}' in '{}:{}'", aProhibited, logFile.getAbsolutePath(), lineFromFile);

								StringBuilder logExcerpt = new StringBuilder();

								logExcerpt.append(System.lineSeparator());

								// include some previous lines in case of irregular formatting
								for (String previousLine : scanner.getPreviousLines()) {
									logExcerpt.append(previousLine);
									logExcerpt.append(System.lineSeparator());
								}

								logExcerpt.append(lineFromFile);
								logExcerpt.append(System.lineSeparator());
								// extract potential stack trace from log
								while (scanner.hasNextLine()) {
									String line = scanner.nextLine();
									logExcerpt.append(line);
									logExcerpt.append(System.lineSeparator());
									if (line.isEmpty() || (!Character.isWhitespace(line.charAt(0)) && !line.startsWith("Caused by"))) {
										// the cause has been printed, now add a few more lines in case of irregular formatting
										for (int x = 0; x < 10 && scanner.hasNextLine(); x++) {
											logExcerpt.append(scanner.nextLine());
											logExcerpt.append(System.lineSeparator());
										}
										break;
									}
								}
								prohibitedExcerpts.add(logExcerpt.toString());

								return true;
							}
						}
					}

				}
			} catch (FileNotFoundException e) {
				LOG.warn("Unable to locate file: " + e.getMessage() + " file: " + logFile.getAbsolutePath());
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
			Assert.fail(
				"Found a file " + foundFile + " with a prohibited string (one of " + Arrays.toString(prohibited) + "). " +
				"Excerpts:" + System.lineSeparator() + prohibitedExcerpts);
		}
	}

	public static boolean verifyStringsInNamedLogFiles(
			final String[] mustHave, final String fileName) {
		List<String> mustHaveList = Arrays.asList(mustHave);
		File cwd = new File("target/" + YARN_CONFIGURATION.get(TEST_CLUSTER_NAME_KEY));
		if (!cwd.exists() || !cwd.isDirectory()) {
			return false;
		}

		File foundFile = TestUtils.findFile(cwd.getAbsolutePath(), new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				if (fileName != null && !name.equals(fileName)) {
					return false;
				}
				File f = new File(dir.getAbsolutePath() + "/" + name);
				LOG.info("Searching in {}", f.getAbsolutePath());
				try (Scanner scanner = new Scanner(f)) {
					Set<String> foundSet = new HashSet<>(mustHave.length);
					while (scanner.hasNextLine()) {
						final String lineFromFile = scanner.nextLine();
						for (String str : mustHave) {
							if (lineFromFile.contains(str)) {
								foundSet.add(str);
							}
						}
						if (foundSet.containsAll(mustHaveList)) {
							return true;
						}
					}
				} catch (FileNotFoundException e) {
					LOG.warn("Unable to locate file: " + e.getMessage() + " file: " + f.getAbsolutePath());
				}
				return false;
			}
		});

		if (foundFile != null) {
			LOG.info("Found string {} in {}.", Arrays.toString(mustHave), foundFile.getAbsolutePath());
			return true;
		} else {
			return false;
		}
	}

	public static boolean verifyTokenKindInContainerCredentials(final Collection<String> tokens, final String containerId)
		throws IOException {
		File cwd = new File("target/" + YARN_CONFIGURATION.get(TEST_CLUSTER_NAME_KEY));
		if (!cwd.exists() || !cwd.isDirectory()) {
			return false;
		}

		File containerTokens = TestUtils.findFile(cwd.getAbsolutePath(), new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return name.equals(containerId + ".tokens");
			}
		});

		if (containerTokens != null) {
			LOG.info("Verifying tokens in {}", containerTokens.getAbsolutePath());

			Credentials tmCredentials = Credentials.readTokenStorageFile(containerTokens, new Configuration());

			Collection<Token<? extends TokenIdentifier>> userTokens = tmCredentials.getAllTokens();
			Set<String> tokenKinds = new HashSet<>(4);
			for (Token<? extends TokenIdentifier> token : userTokens) {
				tokenKinds.add(token.getKind().toString());
			}

			return tokenKinds.containsAll(tokens);
		} else {
			LOG.warn("Unable to find credential file for container {}", containerId);
			return false;
		}
	}

	public static String getContainerIdByLogName(String logName) {
		File cwd = new File("target/" + YARN_CONFIGURATION.get(TEST_CLUSTER_NAME_KEY));
		File containerLog = TestUtils.findFile(cwd.getAbsolutePath(), new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return name.equals(logName);
			}
		});
		if (containerLog != null) {
			return containerLog.getParentFile().getName();
		} else {
			throw new IllegalStateException("No container has log named " + logName);
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

	protected ApplicationReport getOnlyApplicationReport() throws IOException, YarnException {
		final YarnClient yarnClient = getYarnClient();
		checkState(yarnClient != null);

		final List<ApplicationReport> apps = yarnClient.getApplications(EnumSet.of(YarnApplicationState.RUNNING));
		assertEquals(1, apps.size()); // Only one running
		return apps.get(0);
	}

	public static void startYARNSecureMode(YarnConfiguration conf, String principal, String keytab) {
		start(conf, principal, keytab, false);
	}

	public static void startYARNWithConfig(YarnConfiguration conf) {
		startYARNWithConfig(conf,	false);
	}

	public static void startYARNWithConfig(YarnConfiguration conf, boolean withDFS) {
		start(conf, null, null, withDFS);
	}

	private static void start(YarnConfiguration conf, String principal, String keytab, boolean withDFS) {
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
		LOG.info("Trying to locate uberjar in {}", new File(uberjarStartLoc).getAbsolutePath());
		flinkUberjar = TestUtils.findFile(uberjarStartLoc, new TestUtils.RootDirFilenameFilter());
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
				final String testName = conf.get(YarnTestBase.TEST_CLUSTER_NAME_KEY);
				yarnCluster = new MiniYARNCluster(
					testName == null ? "YarnTest_" + UUID.randomUUID() : testName,
					NUM_NODEMANAGERS,
					1,
					1);

				yarnCluster.init(conf);
				yarnCluster.start();
			}

			Map<String, String> map = new HashMap<String, String>(System.getenv());

			File flinkConfDirPath = TestUtils.findFile(flinkDistRootDir, new ContainsName(new String[]{"flink-conf.yaml"}));
			Assert.assertNotNull(flinkConfDirPath);

			final String confDirPath = flinkConfDirPath.getParentFile().getAbsolutePath();
			globalConfiguration = GlobalConfiguration.loadConfiguration(confDirPath);

			//copy conf dir to test temporary workspace location
			tempConfPathForSecureRun = tmp.newFolder("conf");

			FileUtils.copyDirectory(new File(confDirPath), tempConfPathForSecureRun);

			BootstrapTools.writeConfiguration(
				globalConfiguration,
				new File(tempConfPathForSecureRun, "flink-conf.yaml"));

			String configDir = tempConfPathForSecureRun.getAbsolutePath();

			LOG.info("Temporary Flink configuration directory to be used for secure test: {}", configDir);

			Assert.assertNotNull(configDir);

			map.put(ConfigConstants.ENV_FLINK_CONF_DIR, configDir);

			File targetTestClassesFolder = new File("target/test-classes");
			writeYarnSiteConfigXML(conf, targetTestClassesFolder);

			if (withDFS) {
				LOG.info("Starting up MiniDFSCluster");
				setMiniDFSCluster(targetTestClassesFolder);
			}

			map.put("IN_TESTS", "yes we are in tests"); // see YarnClusterDescriptor() for more infos
			map.put("YARN_CONF_DIR", targetTestClassesFolder.getAbsolutePath());
			map.put("MAX_LOG_FILE_NUMBER", "10");
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

	private static void setMiniDFSCluster(File targetTestClassesFolder) throws Exception {
		if (miniDFSCluster == null) {
			Configuration hdfsConfiguration = new Configuration();
			hdfsConfiguration.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, tmpHDFS.getRoot().getAbsolutePath());
			miniDFSCluster = new MiniDFSCluster
				.Builder(hdfsConfiguration)
				.numDataNodes(2)
				.build();
			miniDFSCluster.waitClusterUp();

			hdfsConfiguration = miniDFSCluster.getConfiguration(0);
			writeHDFSSiteConfigXML(hdfsConfiguration, targetTestClassesFolder);
			YARN_CONFIGURATION.addResource(hdfsConfiguration);
		}
	}

	/**
	 * Default @BeforeClass impl. Overwrite this for passing a different configuration
	 */
	@BeforeClass
	public static void setup() throws Exception {
		startYARNWithConfig(YARN_CONFIGURATION, false);
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
	protected Runner startWithArgs(String[] args, String startedAfterString, RunTypes type) throws IOException {
		LOG.info("Running with args {}", Arrays.toString(args));

		outContent = new ByteArrayOutputStream();
		errContent = new ByteArrayOutputStream();
		PipedOutputStream out = new PipedOutputStream();
		PipedInputStream in = new PipedInputStream(out);
		PrintStream stdinPrintStream = new PrintStream(out);

		System.setOut(new PrintStream(outContent));
		System.setErr(new PrintStream(errContent));
		System.setIn(in);

		final int startTimeoutSeconds = 60;

		Runner runner = new Runner(
			args,
			flinkConfiguration,
			CliFrontend.getConfigurationDirectoryFromEnv(),
			type,
			0,
			stdinPrintStream);
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
				resetStreamsAndSendOutput();
				if (runner.getRunnerError() != null) {
					throw new RuntimeException("Runner failed with exception.", runner.getRunnerError());
				}
				Assert.fail("Runner thread died before the test was finished.");
			}
		}

		resetStreamsAndSendOutput();
		Assert.fail("During the timeout period of " + startTimeoutSeconds + " seconds the " +
				"expected string did not show up");
		return null;
	}

	protected void runWithArgs(String[] args, String terminateAfterString, String[] failOnStrings, RunTypes type, int returnCode) throws IOException {
		runWithArgs(args, terminateAfterString, failOnStrings, type, returnCode, Collections::emptyList);
	}

	/**
	 * The test has been passed once the "terminateAfterString" has been seen.
	 * @param args Command line arguments for the runner
	 * @param terminateAfterString the runner is searching the stdout and stderr for this string. as soon as it appears, the test has passed
	 * @param failOnPatterns The runner is searching stdout and stderr for the pattern (regexp) specified here. If one appears, the test has failed
	 * @param type Set the type of the runner
	 * @param expectedReturnValue Expected return code from the runner.
	 * @param logMessageSupplier Supplier for log messages
	 */
	protected void runWithArgs(String[] args, String terminateAfterString, String[] failOnPatterns, RunTypes type, int expectedReturnValue, Supplier<Collection<String>> logMessageSupplier) throws IOException {
		LOG.info("Running with args {}", Arrays.toString(args));

		outContent = new ByteArrayOutputStream();
		errContent = new ByteArrayOutputStream();
		PipedOutputStream out = new PipedOutputStream();
		PipedInputStream in = new PipedInputStream(out);
		PrintStream stdinPrintStream = new PrintStream(out);
		System.setOut(new PrintStream(outContent));
		System.setErr(new PrintStream(errContent));
		System.setIn(in);

		// we wait for at most three minutes
		final int startTimeoutSeconds = 180;
		final long deadline = System.currentTimeMillis() + (startTimeoutSeconds * 1000);

		Runner runner = new Runner(
			args,
			flinkConfiguration,
			CliFrontend.getConfigurationDirectoryFromEnv(),
			type,
			expectedReturnValue,
			stdinPrintStream);
		runner.start();

		boolean expectedStringSeen = false;
		boolean testPassedFromLog4j = false;
		long shutdownTimeout = 30000L;
		do {
			sleep(1000);
			String outContentString = outContent.toString();
			String errContentString = errContent.toString();
			if (failOnPatterns != null) {
				for (String failOnString : failOnPatterns) {
					Pattern pattern = Pattern.compile(failOnString);
					if (pattern.matcher(outContentString).find() || pattern.matcher(errContentString).find()) {
						LOG.warn("Failing test. Output contained illegal string '" + failOnString + "'");
						resetStreamsAndSendOutput();
						// stopping runner.
						runner.sendStop();
						// wait for the thread to stop
						try {
							runner.join(shutdownTimeout);
						} catch (InterruptedException e) {
							LOG.warn("Interrupted while stopping runner", e);
						}
						Assert.fail("Output contained illegal string '" + failOnString + "'");
					}
				}
			}

			for (String logMessage : logMessageSupplier.get()) {
				if (logMessage.contains(terminateAfterString)) {
					testPassedFromLog4j = true;
					LOG.info("Found expected output in logging event {}", logMessage);
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
					runner.join(shutdownTimeout);
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

		resetStreamsAndSendOutput();

		if (runner.getRunnerError() != null) {
			// this lets the test fail.
			throw new RuntimeException("Runner failed", runner.getRunnerError());
		}
		Assert.assertTrue("During the timeout period of " + startTimeoutSeconds + " seconds the " +
				"expected string \"" + terminateAfterString + "\" did not show up.", expectedStringSeen);

		LOG.info("Test was successful");
	}

	protected static void resetStreamsAndSendOutput() {
		System.setOut(ORIGINAL_STDOUT);
		System.setErr(ORIGINAL_STDERR);
		System.setIn(ORIGINAL_STDIN);

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

		private final PrintStream stdinPrintStream;

		private RunTypes type;
		private FlinkYarnSessionCli yCli;
		private Throwable runnerError;

		public Runner(
				String[] args,
				org.apache.flink.configuration.Configuration configuration,
				String configurationDirectory,
				RunTypes type,
				int expectedReturnValue,
				PrintStream stdinPrintStream) {

			this.args = args;
			this.configuration = Preconditions.checkNotNull(configuration);
			this.configurationDirectory = Preconditions.checkNotNull(configurationDirectory);
			this.type = type;
			this.expectedReturnValue = expectedReturnValue;
			this.stdinPrintStream = Preconditions.checkNotNull(stdinPrintStream);
		}

		@Override
		public void run() {
			try {
				int returnValue;
				switch (type) {
					case YARN_SESSION:
						yCli = new FlinkYarnSessionCli(
							configuration,
							configurationDirectory,
							"",
							"",
							true);
						returnValue = yCli.run(args);
						break;
					case CLI_FRONTEND:
						try {
							CliFrontend cli = new CliFrontend(
								configuration,
								CliFrontend.loadCustomCommandLines(configuration, configurationDirectory));
							returnValue = cli.parseAndRun(args);
						} catch (Exception e) {
							throw new RuntimeException("Failed to execute the following args with CliFrontend: "
								+ Arrays.toString(args), e);
						}
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
			stdinPrintStream.println("stop");
		}

		public Throwable getRunnerError() {
			return runnerError;
		}
	}

	// -------------------------- Tear down -------------------------- //

	@AfterClass
	public static void teardown() throws Exception {

		if (yarnCluster != null) {
			LOG.info("Stopping MiniYarn Cluster");
			yarnCluster.stop();
			yarnCluster = null;
		}

		if (miniDFSCluster != null) {
			LOG.info("Stopping MiniDFS Cluster");
			miniDFSCluster.shutdown();
			miniDFSCluster = null;
		}

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

		if (yarnSiteXML != null) {
			yarnSiteXML.delete();
		}

		if (hdfsSiteXML != null) {
			hdfsSiteXML.delete();
		}

		// When we are on CI, we copy the temp files of JUnit (containing the MiniYARNCluster log files)
		// to <flinkRoot>/target/flink-yarn-tests-*.
		// The files from there are picked up by the tools/ci/* scripts to upload them.
		if (isOnCI()) {
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

	public static boolean isOnCI() {
		return System.getenv("IS_CI") != null && System.getenv("IS_CI").equals("true");
	}

	protected void waitApplicationFinishedElseKillIt(
		ApplicationId applicationId,
		Duration timeout,
		YarnClusterDescriptor yarnClusterDescriptor,
		int sleepIntervalInMS) throws Exception {
		Deadline deadline = Deadline.now().plus(timeout);
		YarnApplicationState state = getYarnClient().getApplicationReport(applicationId).getYarnApplicationState();

		while (state != YarnApplicationState.FINISHED) {
			if (state == YarnApplicationState.FAILED || state == YarnApplicationState.KILLED) {
				Assert.fail("Application became FAILED or KILLED while expecting FINISHED");
			}

			if (deadline.isOverdue()) {
				yarnClusterDescriptor.killCluster(applicationId);
				Assert.fail("Application didn't finish before timeout");
			}

			sleep(sleepIntervalInMS);
			state = getYarnClient().getApplicationReport(applicationId).getYarnApplicationState();
		}
	}

	/**
	 * Wrapper around a {@link Scanner} that buffers the last N lines read.
	 */
	private static class BufferingScanner {

		private final Scanner scanner;
		private final int numLinesBuffered;
		private final List<String> bufferedLines;

		BufferingScanner(Scanner scanner, int numLinesBuffered) {
			this.scanner = scanner;
			this.numLinesBuffered = numLinesBuffered;
			this.bufferedLines = new ArrayList<>(numLinesBuffered);
		}

		public boolean hasNextLine() {
			return scanner.hasNextLine();
		}

		public String nextLine() {
			if (bufferedLines.size() == numLinesBuffered) {
				bufferedLines.remove(0);
			}
			String line = scanner.nextLine();
			bufferedLines.add(line);
			return line;
		}

		public List<String> getPreviousLines() {
			return new ArrayList<>(bufferedLines);
		}
	}
}
