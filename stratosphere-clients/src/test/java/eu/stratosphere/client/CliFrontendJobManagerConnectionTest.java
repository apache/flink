/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.client;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.net.InetSocketAddress;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import eu.stratosphere.client.CliFrontendTestUtils.TestingCliFrontend;

public class CliFrontendJobManagerConnectionTest {
	
	@BeforeClass
	public static void init() {
		CliFrontendTestUtils.pipeSystemOutToNull();
	}
	
	@Before
	public void clearConfig() {
		CliFrontendTestUtils.clearGlobalConfiguration();
	}

	@Test
	public void testInvalidConfig() {
		try {
			String[] arguments = {};
			CommandLine line = new PosixParser().parse(CliFrontend.getJobManagerAddressOption(new Options()), arguments, false);
				
			TestingCliFrontend frontend = new TestingCliFrontend(CliFrontendTestUtils.getInvalidConfigDir());
			
			assertTrue(frontend.getJobManagerAddress(line) == null);
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Program caused an exception: " + e.getMessage());
		}
	}
	
	@Test
	public void testValidConfig() {
		try {
			String[] arguments = {};
			CommandLine line = new PosixParser().parse(CliFrontend.getJobManagerAddressOption(new Options()), arguments, false);
				
			TestingCliFrontend frontend = new TestingCliFrontend(CliFrontendTestUtils.getConfigDir());
			
			InetSocketAddress address = frontend.getJobManagerAddress(line);
			
			assertNotNull(address);
			assertEquals(CliFrontendTestUtils.TEST_JOB_MANAGER_ADDRESS, address.getAddress().getHostAddress());
			assertEquals(CliFrontendTestUtils.TEST_JOB_MANAGER_PORT, address.getPort());
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Program caused an exception: " + e.getMessage());
		}
	}
	
	@Test
	public void testYarnConfig() {
		try {
			String[] arguments = {};
			CommandLine line = new PosixParser().parse(CliFrontend.getJobManagerAddressOption(new Options()), arguments, false);
				
			TestingCliFrontend frontend = new TestingCliFrontend(CliFrontendTestUtils.getConfigDirWithYarnFile());
			
			InetSocketAddress address = frontend.getJobManagerAddress(line);
			
			assertNotNull(address);
			assertEquals(CliFrontendTestUtils.TEST_YARN_JOB_MANAGER_ADDRESS, address.getAddress().getHostAddress());
			assertEquals(CliFrontendTestUtils.TEST_YARN_JOB_MANAGER_PORT, address.getPort());
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Program caused an exception: " + e.getMessage());
		}
	}
	
	@Test
	public void testInvalidYarnConfig() {
		try {
			String[] arguments = {};
			CommandLine line = new PosixParser().parse(CliFrontend.getJobManagerAddressOption(new Options()), arguments, false);
				
			TestingCliFrontend frontend = new TestingCliFrontend(CliFrontendTestUtils.getConfigDirWithInvalidYarnFile());
			
			assertTrue(frontend.getJobManagerAddress(line) == null);
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Program caused an exception: " + e.getMessage());
		}
	}
	
	@Test
	public void testManualOptionsOverridesConfig() {
		try {
			String[] arguments = {"-m", "10.221.130.22:7788"};
			CommandLine line = new PosixParser().parse(CliFrontend.getJobManagerAddressOption(new Options()), arguments, false);
				
			TestingCliFrontend frontend = new TestingCliFrontend(CliFrontendTestUtils.getConfigDir());
			
			InetSocketAddress address = frontend.getJobManagerAddress(line);
			
			assertNotNull(address);
			assertEquals("10.221.130.22", address.getAddress().getHostAddress());
			assertEquals(7788, address.getPort());
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Program caused an exception: " + e.getMessage());
		}
	}
	
	@Test
	public void testManualOptionsOverridesYarn() {
		try {
			String[] arguments = {"-m", "10.221.130.22:7788"};
			CommandLine line = new PosixParser().parse(CliFrontend.getJobManagerAddressOption(new Options()), arguments, false);
				
			TestingCliFrontend frontend = new TestingCliFrontend(CliFrontendTestUtils.getConfigDirWithYarnFile());
			
			InetSocketAddress address = frontend.getJobManagerAddress(line);
			
			assertNotNull(address);
			assertEquals("10.221.130.22", address.getAddress().getHostAddress());
			assertEquals(7788, address.getPort());
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Program caused an exception: " + e.getMessage());
		}
	}
}
