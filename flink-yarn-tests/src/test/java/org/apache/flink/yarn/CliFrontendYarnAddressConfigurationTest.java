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

import org.apache.flink.client.CliFrontend;
import org.apache.flink.client.cli.CommandLineOptions;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.OutputStream;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
	}

	@AfterClass
	public static void restoreAfterwards() {
		System.setOut(OUT);
		System.setErr(ERR);
	}

	@Before
	public void clearConfig() throws NoSuchFieldException, IllegalAccessException {
		// reset GlobalConfiguration between tests
		Field instance = GlobalConfiguration.class.getDeclaredField("SINGLETON");
		instance.setAccessible(true);
		instance.set(null, null);
	}

	private static final String TEST_YARN_JOB_MANAGER_ADDRESS = "22.33.44.55";
	private static final int TEST_YARN_JOB_MANAGER_PORT = 6655;

	private static final String propertiesFile =
		"jobManager=" + TEST_YARN_JOB_MANAGER_ADDRESS + ":" + TEST_YARN_JOB_MANAGER_PORT;


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
	public void testYarnConfig() {
		try {
			File tmpFolder = temporaryFolder.newFolder();
			String currentUser = System.getProperty("user.name");

			// copy .yarn-properties-<username>
			File testPropertiesFile = new File(tmpFolder, ".yarn-properties-"+currentUser);
			Files.write(testPropertiesFile.toPath(), propertiesFile.getBytes(), StandardOpenOption.CREATE);

			// copy reference flink-conf.yaml to temporary test directory and append custom configuration path.
			String confFile = flinkConf + "\nyarn.properties-file.location: " + tmpFolder;
			File testConfFile = new File(tmpFolder.getAbsolutePath(), "flink-conf.yaml");
			Files.write(testConfFile.toPath(), confFile.getBytes(), StandardOpenOption.CREATE);

			// start CLI Frontend
			TestCLI frontend = new TestCLI(tmpFolder.getAbsolutePath());

			CommandLineOptions options = mock(CommandLineOptions.class);

			frontend.getClient(options, "Program name");

			frontend.updateConfig(options);
			Configuration config = frontend.getConfiguration();

 			checkJobManagerAddress(
					config,
					TEST_YARN_JOB_MANAGER_ADDRESS,
					TEST_YARN_JOB_MANAGER_PORT);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	public static class TestCLI extends CliFrontend {
		TestCLI(String configDir) throws Exception {
			super(configDir);
		}

		@Override
		public ClusterClient getClient(CommandLineOptions options, String programName) throws Exception {
			return super.getClient(options, programName);
		}

		@Override
		public void updateConfig(CommandLineOptions options) {
			super.updateConfig(options);
		}
	}

	@Test
	public void testInvalidYarnConfig() {
		try {
			File tmpFolder = temporaryFolder.newFolder();

			// copy invalid .yarn-properties-<username>
			File testPropertiesFile = new File(tmpFolder, ".yarn-properties");
			Files.write(testPropertiesFile.toPath(), invalidPropertiesFile.getBytes(), StandardOpenOption.CREATE);

			// copy reference flink-conf.yaml to temporary test directory and append custom configuration path.
			String confFile = flinkConf + "\nyarn.properties-file.location: " + tmpFolder;
			File testConfFile = new File(tmpFolder.getAbsolutePath(), "flink-conf.yaml");
			Files.write(testConfFile.toPath(), confFile.getBytes(), StandardOpenOption.CREATE);

			TestCLI cli = new TestCLI(tmpFolder.getAbsolutePath());

			CommandLineOptions options = mock(CommandLineOptions.class);

			cli.updateConfig(options);

			Configuration config = cli.getConfiguration();

			checkJobManagerAddress(
				config,
				TEST_JOB_MANAGER_ADDRESS,
				TEST_JOB_MANAGER_PORT);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}


	@Test
	public void testManualOptionsOverridesYarn() {
		try {
			File emptyFolder = temporaryFolder.newFolder();
			TestCLI frontend = new TestCLI(emptyFolder.getAbsolutePath());

			CommandLineOptions options = mock(CommandLineOptions.class);
			when(options.getJobManagerAddress()).thenReturn("10.221.130.22:7788");

			frontend.updateConfig(options);

			Configuration config = frontend.getConfiguration();

			InetSocketAddress expectedAddress = new InetSocketAddress("10.221.130.22", 7788);

			checkJobManagerAddress(config, expectedAddress.getHostName(), expectedAddress.getPort());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}


	private static void checkJobManagerAddress(Configuration config, String expectedAddress, int expectedPort) {
		String jobManagerAddress = config.getString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, null);
		int jobManagerPort = config.getInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, -1);

		assertEquals(expectedAddress, jobManagerAddress);
		assertEquals(expectedPort, jobManagerPort);
	}

}
