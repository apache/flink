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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.BeforeClass;
import org.junit.Test;

import eu.stratosphere.client.CliFrontendTest.TestingCliFrontend;

/**
 * This needs to be its own test, because it loads a specific global configuration
 */
public class CliFrontendYarnFileTest {

	private static final String TEST_YARN_JOB_MANAGER_ADDRESS = "22.33.44.55";
	
	private static final int TEST_YARN_JOB_MANAGER_PORT = 6655;
	
	@BeforeClass
	public static void init() {
		CliFrontendTest.pipeSystemOutToNull();
		CliFrontendTest.clearGlobalConfiguration();
	}
	
	@Test
	public void testRunWithYarn() {
		try {
			// test with yarn JM file
			{
				String[] parameters = {"-v", CliFrontendTest.getTestJarPath(), "some", "program", "arguments"};
				
				TestingCliFrontend testFrontend = new TestingCliFrontend(CliFrontendTest.getConfigDirWithYarnFile());
				testFrontend.expectedArguments = new String[] {"some", "program", "arguments"};
				testFrontend.expectedJobManagerAddress = TEST_YARN_JOB_MANAGER_ADDRESS;
				testFrontend.expectedJobManagerPort = TEST_YARN_JOB_MANAGER_PORT;
				
				assertEquals(0, testFrontend.run(parameters));
			}
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Program caused an exception: " + e.getMessage());
		}
	}
	
	
	
	@Test
	public void testJobManagerOptionOverridesYarnFile() {
		try {
			// test with yarn JM file
			{
				String[] parameters = {"-v", "-m", "10.221.130.22:7788", CliFrontendTest.getTestJarPath(), "some", "program", "arguments"};
				
				TestingCliFrontend testFrontend = new TestingCliFrontend(CliFrontendTest.getConfigDirWithYarnFile());
				testFrontend.expectedArguments = new String[] {"some", "program", "arguments"};
				testFrontend.expectedJobManagerAddress = "10.221.130.22";
				testFrontend.expectedJobManagerPort = 7788;
				
				assertEquals(0, testFrontend.run(parameters));
			}
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Program caused an exception: " + e.getMessage());
		}
	}
}
