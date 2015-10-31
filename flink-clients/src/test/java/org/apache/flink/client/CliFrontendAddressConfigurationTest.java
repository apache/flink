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

package org.apache.flink.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import static org.mockito.Mockito.*;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import org.apache.flink.client.cli.CommandLineOptions;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.net.InetSocketAddress;

/**
 * Tests that verify that the CLI client picks up the correct address for the JobManager
 * from configuration and configs.
 */
public class CliFrontendAddressConfigurationTest {

	@Rule
	public TemporaryFolder folder = new TemporaryFolder();
	
	@BeforeClass
	public static void init() {
		CliFrontendTestUtils.pipeSystemOutToNull();
	}
	
	@Before
	public void clearConfig() {
		CliFrontendTestUtils.clearGlobalConfiguration();
	}

	@Test
	public void testInvalidConfigAndNoOption() {
		try {
			CliFrontend frontend = new CliFrontend(CliFrontendTestUtils.getInvalidConfigDir());
			CommandLineOptions options = mock(CommandLineOptions.class);

			frontend.updateConfig(options);
			Configuration config = frontend.getConfiguration();

			checkJobManagerAddress(config, null, -1);

		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testInvalidConfigAndOption() {
		try {
			CliFrontend frontend = new CliFrontend(CliFrontendTestUtils.getInvalidConfigDir());

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
	
	@Test
	public void testValidConfig() {
		try {
			CliFrontend frontend = new CliFrontend(CliFrontendTestUtils.getConfigDir());

			CommandLineOptions options = mock(CommandLineOptions.class);

			frontend.updateConfig(options);
			Configuration config = frontend.getConfiguration();

			checkJobManagerAddress(
					config,
					CliFrontendTestUtils.TEST_JOB_MANAGER_ADDRESS,
					CliFrontendTestUtils.TEST_JOB_MANAGER_PORT);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	/**
	 * Test that the CliFrontent is able to pick up the .yarn-properties file from a specified location.
	 */
	@Test
	public void testYarnConfig() {
		try {
			File tmpFolder = folder.newFolder();
			String currentUser = System.getProperty("user.name");

			// copy reference flink-conf.yaml to temporary test directory and append custom configuration path.
			File confFile = new File(CliFrontendRunTest.class.getResource("/testconfigwithyarn/flink-conf.yaml").getFile());
			File testConfFile = new File(tmpFolder, "flink-conf.yaml");
			org.apache.commons.io.FileUtils.copyFile(confFile, testConfFile);
			String toAppend = "\nyarn.properties-file.location: " + tmpFolder;
			// append to flink-conf.yaml
			Files.write(testConfFile.toPath(), toAppend.getBytes(), StandardOpenOption.APPEND);
			// copy .yarn-properties-<username>
			File propertiesFile = new File(CliFrontendRunTest.class.getResource("/testconfigwithyarn/.yarn-properties").getFile());
			File testPropertiesFile = new File(tmpFolder, ".yarn-properties-"+currentUser);
			org.apache.commons.io.FileUtils.copyFile(propertiesFile, testPropertiesFile);

			// start CLI Frontend
			CliFrontend frontend = new CliFrontend(tmpFolder.getAbsolutePath());

			CommandLineOptions options = mock(CommandLineOptions.class);

			frontend.updateConfig(options);
			Configuration config = frontend.getConfiguration();

			checkJobManagerAddress(
					config,
					CliFrontendTestUtils.TEST_YARN_JOB_MANAGER_ADDRESS,
					CliFrontendTestUtils.TEST_YARN_JOB_MANAGER_PORT);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testInvalidYarnConfig() {
		try {
			CliFrontend cli = new CliFrontend(CliFrontendTestUtils.getConfigDirWithInvalidYarnFile());

			CommandLineOptions options = mock(CommandLineOptions.class);

			cli.updateConfig(options);

			Configuration config = cli.getConfiguration();

			checkJobManagerAddress(
				config,
				CliFrontendTestUtils.TEST_JOB_MANAGER_ADDRESS,
				CliFrontendTestUtils.TEST_JOB_MANAGER_PORT);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testManualOptionsOverridesConfig() {
		try {
			CliFrontend frontend = new CliFrontend(CliFrontendTestUtils.getConfigDir());

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
	
	@Test
	public void testManualOptionsOverridesYarn() {
		try {
			CliFrontend frontend = new CliFrontend(CliFrontendTestUtils.getConfigDirWithYarnFile());

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

	public void checkJobManagerAddress(Configuration config, String expectedAddress, int expectedPort) {
		String jobManagerAddress = config.getString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, null);
		int jobManagerPort = config.getInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, -1);

		assertEquals(expectedAddress, jobManagerAddress);
		assertEquals(expectedPort, jobManagerPort);
	}
}
