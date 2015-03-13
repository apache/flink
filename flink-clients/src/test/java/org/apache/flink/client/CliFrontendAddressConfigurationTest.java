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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import static org.mockito.Mockito.*;

import java.net.InetSocketAddress;

import org.apache.flink.client.cli.CommandLineOptions;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests that verify that the CLI client picks up the correct address for the JobManager
 * from configuration and configs.
 */
public class CliFrontendAddressConfigurationTest {
	
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

			try {
				frontend.getJobManagerAddress(options);
				fail("we expect an exception here because the we have no config");
			}
			catch (Exception e) {
				// expected
			}
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

			assertNotNull(frontend.getJobManagerAddress(options));
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
			InetSocketAddress address = frontend.getJobManagerAddress(options);
			
			assertNotNull(address);
			assertEquals(CliFrontendTestUtils.TEST_JOB_MANAGER_ADDRESS, address.getAddress().getHostAddress());
			assertEquals(CliFrontendTestUtils.TEST_JOB_MANAGER_PORT, address.getPort());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testYarnConfig() {
		try {
			CliFrontend frontend = new CliFrontend(CliFrontendTestUtils.getConfigDirWithYarnFile());

			CommandLineOptions options = mock(CommandLineOptions.class);
			InetSocketAddress address = frontend.getJobManagerAddress(options);
			
			assertNotNull(address);
			assertEquals(CliFrontendTestUtils.TEST_YARN_JOB_MANAGER_ADDRESS, address.getAddress().getHostAddress());
			assertEquals(CliFrontendTestUtils.TEST_YARN_JOB_MANAGER_PORT, address.getPort());
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

			InetSocketAddress address = cli.getJobManagerAddress(options);

			assertEquals(CliFrontendTestUtils.TEST_JOB_MANAGER_ADDRESS, address.getAddress().getHostAddress());
			assertEquals(CliFrontendTestUtils.TEST_JOB_MANAGER_PORT, address.getPort());
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

			InetSocketAddress address = frontend.getJobManagerAddress(options);
			
			assertNotNull(address);
			assertEquals("10.221.130.22", address.getAddress().getHostAddress());
			assertEquals(7788, address.getPort());
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

			InetSocketAddress address = frontend.getJobManagerAddress(options);
			
			assertNotNull(address);
			assertEquals("10.221.130.22", address.getAddress().getHostAddress());
			assertEquals(7788, address.getPort());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
