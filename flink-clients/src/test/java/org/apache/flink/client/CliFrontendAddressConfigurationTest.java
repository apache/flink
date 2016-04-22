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

import static org.apache.flink.client.CliFrontendTestUtils.checkJobManagerAddress;
import static org.junit.Assert.fail;

import static org.mockito.Mockito.*;

import org.apache.flink.client.cli.CommandLineOptions;

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

}
