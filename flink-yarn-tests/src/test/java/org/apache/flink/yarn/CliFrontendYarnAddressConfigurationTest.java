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

import org.apache.commons.cli.CommandLine;
import org.apache.flink.client.CliFrontend;
import org.apache.flink.client.cli.CliFrontendParser;
import org.apache.flink.client.cli.CommandLineOptions;
import org.apache.flink.client.cli.CustomCommandLine;
import org.apache.flink.client.cli.RunOptions;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.yarn.cli.FlinkYarnSessionCli;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.impl.YarnClientImpl;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Tests that verify that the CLI client picks up the correct address for the JobManager
 * from configuration and configs.
 */
public class CliFrontendYarnAddressConfigurationTest {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	private final static PrintStream OUT = System.out;
	private final static PrintStream ERR = System.err;

	@BeforeClass
	public static void disableStdOutErr() {
		class NullPrint extends OutputStream {
			@Override
			public void write(int b) {}
		}

		PrintStream nullPrinter = new PrintStream(new NullPrint());
		System.setOut(nullPrinter);
		System.setErr(nullPrinter);

		// Unset FLINK_CONF_DIR, as this is a precondition for this test to work properly
		Map<String, String> map = new HashMap<>(System.getenv());
		map.remove(ConfigConstants.ENV_FLINK_CONF_DIR);
		TestBaseUtils.setEnv(map);
	}

	@AfterClass
	public static void restoreAfterwards() {
		System.setOut(OUT);
		System.setErr(ERR);
	}

	private static final String TEST_YARN_JOB_MANAGER_ADDRESS = "22.33.44.55";
	private static final int TEST_YARN_JOB_MANAGER_PORT = 6655;
	private static final ApplicationId TEST_YARN_APPLICATION_ID =
		ApplicationId.newInstance(System.currentTimeMillis(), 42);

	private static final String validPropertiesFile = "applicationID=" + TEST_YARN_APPLICATION_ID;


	private static final String TEST_JOB_MANAGER_ADDRESS = "192.168.1.33";
	private static final int TEST_JOB_MANAGER_PORT = 55443;

	private static final String flinkConf =
		"jobmanager.rpc.address: " + TEST_JOB_MANAGER_ADDRESS + "\n" +
		"jobmanager.rpc.port: " + TEST_JOB_MANAGER_PORT;


	private static final String invalidPropertiesFile =
		"jasfobManager=" + TEST_YARN_JOB_MANAGER_ADDRESS + ":asf" + TEST_YARN_JOB_MANAGER_PORT;


	/**
	 * Test that the CliFrontend is able to pick up the .yarn-properties file from a specified location.
	 */
	@Test
	public void testResumeFromYarnPropertiesFile() throws Exception {

		File directoryPath = writeYarnPropertiesFile(validPropertiesFile);

		// start CLI Frontend
		TestCLI frontend = new CustomYarnTestCLI(directoryPath.getAbsolutePath());

		RunOptions options = CliFrontendParser.parseRunCommand(new String[] {});

		frontend.retrieveClient(options);
		checkJobManagerAddress(
			frontend.getConfiguration(),
			TEST_YARN_JOB_MANAGER_ADDRESS,
			TEST_YARN_JOB_MANAGER_PORT);

	}

	@Test(expected = IllegalConfigurationException.class)
	public void testResumeFromYarnPropertiesFileWithFinishedApplication() throws Exception {

		File directoryPath = writeYarnPropertiesFile(validPropertiesFile);

		// start CLI Frontend
		TestCLI frontend = new CustomYarnTestCLI(directoryPath.getAbsolutePath(), FinalApplicationStatus.SUCCEEDED);

		RunOptions options = CliFrontendParser.parseRunCommand(new String[] {});

		frontend.retrieveClient(options);
		checkJobManagerAddress(
			frontend.getConfiguration(),
			TEST_YARN_JOB_MANAGER_ADDRESS,
			TEST_YARN_JOB_MANAGER_PORT);
	}

	@Test(expected = IllegalConfigurationException.class)
	public void testInvalidYarnPropertiesFile() throws Exception {

		File directoryPath = writeYarnPropertiesFile(invalidPropertiesFile);

		TestCLI frontend = new CustomYarnTestCLI(directoryPath.getAbsolutePath());

		RunOptions options = CliFrontendParser.parseRunCommand(new String[] {});

		frontend.retrieveClient(options);
		Configuration config = frontend.getConfiguration();

		checkJobManagerAddress(
			config,
			TEST_JOB_MANAGER_ADDRESS,
			TEST_JOB_MANAGER_PORT);
	}


	@Test
	public void testResumeFromYarnID() throws Exception {
		File directoryPath = writeYarnPropertiesFile(validPropertiesFile);

		// start CLI Frontend
		TestCLI frontend = new CustomYarnTestCLI(directoryPath.getAbsolutePath());

		RunOptions options =
			CliFrontendParser.parseRunCommand(new String[] {"-yid", TEST_YARN_APPLICATION_ID.toString()});

		frontend.retrieveClient(options);

		checkJobManagerAddress(
			frontend.getConfiguration(),
			TEST_YARN_JOB_MANAGER_ADDRESS,
			TEST_YARN_JOB_MANAGER_PORT);
	}

	@Test
	public void testResumeFromYarnIDZookeeperNamespace() throws Exception {
		File directoryPath = writeYarnPropertiesFile(validPropertiesFile);
		// start CLI Frontend
		TestCLI frontend = new CustomYarnTestCLI(directoryPath.getAbsolutePath());

		RunOptions options =
				CliFrontendParser.parseRunCommand(new String[] {"-yid", TEST_YARN_APPLICATION_ID.toString()});

		frontend.retrieveClient(options);
		String zkNs = frontend.getConfiguration().getString(ConfigConstants.ZOOKEEPER_NAMESPACE_KEY, "error");
		Assert.assertTrue(zkNs.matches("application_\\d+_0042"));
	}

	@Test
	public void testResumeFromYarnIDZookeeperNamespaceOverride() throws Exception {
		File directoryPath = writeYarnPropertiesFile(validPropertiesFile);
		// start CLI Frontend
		TestCLI frontend = new CustomYarnTestCLI(directoryPath.getAbsolutePath());
		String overrideZkNamespace = "my_cluster";
		RunOptions options =
				CliFrontendParser.parseRunCommand(new String[] {"-yid", TEST_YARN_APPLICATION_ID.toString(), "-yz", overrideZkNamespace});

		frontend.retrieveClient(options);
		String zkNs = frontend.getConfiguration().getString(ConfigConstants.ZOOKEEPER_NAMESPACE_KEY, "error");
		Assert.assertEquals(overrideZkNamespace, zkNs);
	}

	@Test(expected = IllegalConfigurationException.class)
	public void testResumeFromInvalidYarnID() throws Exception {
		File directoryPath = writeYarnPropertiesFile(validPropertiesFile);

		// start CLI Frontend
		TestCLI frontend = new CustomYarnTestCLI(directoryPath.getAbsolutePath(), FinalApplicationStatus.SUCCEEDED);

		RunOptions options =
			CliFrontendParser.parseRunCommand(new String[] {"-yid", ApplicationId.newInstance(0, 666).toString()});

		frontend.retrieveClient(options);
		checkJobManagerAddress(
			frontend.getConfiguration(),
			TEST_YARN_JOB_MANAGER_ADDRESS,
			TEST_YARN_JOB_MANAGER_PORT);
	}

	@Test(expected = IllegalConfigurationException.class)
	public void testResumeFromYarnIDWithFinishedApplication() throws Exception {
		File directoryPath = writeYarnPropertiesFile(validPropertiesFile);

		// start CLI Frontend
		TestCLI frontend = new CustomYarnTestCLI(directoryPath.getAbsolutePath(), FinalApplicationStatus.SUCCEEDED);

		RunOptions options =
			CliFrontendParser.parseRunCommand(new String[] {"-yid", TEST_YARN_APPLICATION_ID.toString()});

		frontend.retrieveClient(options);

		checkJobManagerAddress(
			frontend.getConfiguration(),
			TEST_YARN_JOB_MANAGER_ADDRESS,
			TEST_YARN_JOB_MANAGER_PORT);
	}


	@Test
	public void testYarnIDOverridesPropertiesFile() throws Exception {
		File directoryPath = writeYarnPropertiesFile(invalidPropertiesFile);

		// start CLI Frontend
		TestCLI frontend = new CustomYarnTestCLI(directoryPath.getAbsolutePath());

		RunOptions options =
			CliFrontendParser.parseRunCommand(new String[] {"-yid", TEST_YARN_APPLICATION_ID.toString()});

		frontend.retrieveClient(options);

		checkJobManagerAddress(
			frontend.getConfiguration(),
			TEST_YARN_JOB_MANAGER_ADDRESS,
			TEST_YARN_JOB_MANAGER_PORT);
	}


	@Test
	public void testManualOptionsOverridesYarn() throws Exception {

		File emptyFolder = temporaryFolder.newFolder();
		File testConfFile = new File(emptyFolder.getAbsolutePath(), "flink-conf.yaml");
		Files.createFile(testConfFile.toPath());

		TestCLI frontend = new TestCLI(emptyFolder.getAbsolutePath());

		RunOptions options = CliFrontendParser.parseRunCommand(new String[] {"-m", "10.221.130.22:7788"});

		frontend.retrieveClient(options);

		Configuration config = frontend.getConfiguration();

		InetSocketAddress expectedAddress = InetSocketAddress.createUnresolved("10.221.130.22", 7788);

		checkJobManagerAddress(config, expectedAddress.getHostName(), expectedAddress.getPort());

	}

	///////////
	// Utils //
	///////////

	private File writeYarnPropertiesFile(String contents) throws IOException {
		File tmpFolder = temporaryFolder.newFolder();
		String currentUser = System.getProperty("user.name");

		// copy .yarn-properties-<username>
		File testPropertiesFile = new File(tmpFolder, ".yarn-properties-"+currentUser);
		Files.write(testPropertiesFile.toPath(), contents.getBytes(), StandardOpenOption.CREATE);

		// copy reference flink-conf.yaml to temporary test directory and append custom configuration path.
		String confFile = flinkConf + "\nyarn.properties-file.location: " + tmpFolder;
		File testConfFile = new File(tmpFolder.getAbsolutePath(), "flink-conf.yaml");
		Files.write(testConfFile.toPath(), confFile.getBytes(), StandardOpenOption.CREATE);

		return tmpFolder.getAbsoluteFile();
	}

	private static class TestCLI extends CliFrontend {
		TestCLI(String configDir) throws Exception {
			super(configDir);
		}

		@Override
		// make method public
		public ClusterClient createClient(CommandLineOptions options, String programName) throws Exception {
			return super.createClient(options, programName);
		}

		@Override
		// make method public
		public ClusterClient retrieveClient(CommandLineOptions options) {
			return super.retrieveClient(options);
		}
	}


	/**
	 * Injects an extended FlinkYarnSessionCli that deals with mocking Yarn communication
	 */
	private static class CustomYarnTestCLI extends TestCLI {

		// the default application status for yarn applications to be retrieved
		private final FinalApplicationStatus finalApplicationStatus;

		CustomYarnTestCLI(String configDir) throws Exception {
			this(configDir, FinalApplicationStatus.UNDEFINED);
		}

		CustomYarnTestCLI(String configDir, FinalApplicationStatus finalApplicationStatus) throws Exception {
			super(configDir);
			this.finalApplicationStatus = finalApplicationStatus;
		}

		@Override
		public CustomCommandLine getActiveCustomCommandLine(CommandLine commandLine) {
			// inject the testing FlinkYarnSessionCli
			return new TestingYarnSessionCli();
		}

		/**
		 * Testing FlinkYarnSessionCli which returns a modified cluster descriptor for testing.
		 */
		private class TestingYarnSessionCli extends FlinkYarnSessionCli {
			TestingYarnSessionCli() {
				super("y", "yarn");
			}

			@Override
			// override cluster descriptor to replace the YarnClient
			protected AbstractYarnClusterDescriptor getClusterDescriptor() {
				return new TestingYarnClusterDescriptor();
			}

			/**
			 * Replace the YarnClient for this test.
			 */
			private class TestingYarnClusterDescriptor extends YarnClusterDescriptor {

				@Override
				protected YarnClient getYarnClient() {
					return new TestYarnClient();
				}

				@Override
				protected YarnClusterClient createYarnClusterClient(
						AbstractYarnClusterDescriptor descriptor,
						YarnClient yarnClient,
						ApplicationReport report,
						Configuration flinkConfiguration,
						Path sessionFilesDir,
						boolean perJobCluster) throws IOException, YarnException {

					return Mockito.mock(YarnClusterClient.class);
				}


				private class TestYarnClient extends YarnClientImpl {

					private final List<ApplicationReport> reports = new LinkedList<>();

					TestYarnClient() {
						{   // a report that of our Yarn application we want to resume from
							ApplicationReport report = Mockito.mock(ApplicationReport.class);
							Mockito.when(report.getHost()).thenReturn(TEST_YARN_JOB_MANAGER_ADDRESS);
							Mockito.when(report.getRpcPort()).thenReturn(TEST_YARN_JOB_MANAGER_PORT);
							Mockito.when(report.getApplicationId()).thenReturn(TEST_YARN_APPLICATION_ID);
							Mockito.when(report.getFinalApplicationStatus()).thenReturn(finalApplicationStatus);
							this.reports.add(report);
						}
						{   // a second report, just for noise
							ApplicationReport report = Mockito.mock(ApplicationReport.class);
							Mockito.when(report.getHost()).thenReturn("1.2.3.4");
							Mockito.when(report.getRpcPort()).thenReturn(-123);
							Mockito.when(report.getApplicationId()).thenReturn(ApplicationId.newInstance(0, 0));
							Mockito.when(report.getFinalApplicationStatus()).thenReturn(finalApplicationStatus);
							this.reports.add(report);
						}
					}

					@Override
					public List<ApplicationReport> getApplications() throws YarnException, IOException {
						return reports;
					}

					@Override
					public ApplicationReport getApplicationReport(ApplicationId appId) throws YarnException, IOException {
						for (ApplicationReport report : reports) {
							if (report.getApplicationId().equals(appId)) {
								return report;
							}
						}
						throw new YarnException();
					}
				}
			}
		}
	}


	private static void checkJobManagerAddress(Configuration config, String expectedAddress, int expectedPort) {
		String jobManagerAddress = config.getString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, null);
		int jobManagerPort = config.getInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, -1);

		assertEquals(expectedAddress, jobManagerAddress);
		assertEquals(expectedPort, jobManagerPort);
	}

}
