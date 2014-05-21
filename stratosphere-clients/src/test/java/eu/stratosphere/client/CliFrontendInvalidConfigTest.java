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
import static org.junit.Assert.fail;

import org.junit.BeforeClass;
import org.junit.Test;

import eu.stratosphere.client.CliFrontendTest.TestingCliFrontend;

/**
 * This needs to be its own test, because it loads a specific global configuration
 */
public class CliFrontendInvalidConfigTest {

	@BeforeClass
	public static void init() {
		CliFrontendTest.pipeSystemOutToNull();
		CliFrontendTest.clearGlobalConfiguration();
	}
	
	
	@Test
	public void testRunWithInvalidConfig() {
		try {
			// test run with invalid config
			{
				String[] parameters = {"-w", "-v", "-j", CliFrontendTest.getTestJarPath(), "-a", "some", "program", "arguments"};
				TestingCliFrontend testFrontend = new TestingCliFrontend(CliFrontendTest.getInvalidConfigDir());
				assertTrue(0 != testFrontend.run(parameters));
			}
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Program caused an exception: " + e.getMessage());
		}
	}
}
