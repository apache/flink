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

import static org.junit.Assert.*;
import static eu.stratosphere.client.CliFrontendTestUtils.*;


import org.junit.BeforeClass;
import org.junit.Test;

import eu.stratosphere.client.program.Client;
import eu.stratosphere.client.program.PackagedProgram;


public class CliFrontendRunTest {
	
	@BeforeClass
	public static void init() {
		CliFrontendTestUtils.pipeSystemOutToNull();
		CliFrontendTestUtils.clearGlobalConfiguration();
	}
	
	@Test
	public void testRun() {
		try {
			// test unrecognized option
			{
				String[] parameters = {"-v", "-l", "-a", "some", "program", "arguments"};
				TestingCliFrontend testFrontend = new TestingCliFrontend();
				int retCode = testFrontend.run(parameters);
				assertTrue(retCode != 0);
			}
			
			// test without parallelism
			{
				String[] parameters = {"-v", getTestJarPath()};
				RunTestingCliFrontend testFrontend = new RunTestingCliFrontend(-1);
				assertEquals(0, testFrontend.run(parameters));
			}
			
			// test configure parallelism
			{
				String[] parameters = {"-v", "-p", "42",  getTestJarPath()};
				RunTestingCliFrontend testFrontend = new RunTestingCliFrontend(42);
				assertEquals(0, testFrontend.run(parameters));
			}
			
			// test configure parallelism with non integer value
			{
				String[] parameters = {"-v", "-p", "text",  getTestJarPath()};
				TestingCliFrontend testFrontend = new TestingCliFrontend();
				assertTrue(0 != testFrontend.run(parameters));
			}
			
			// test configure parallelism with overflow integer value
			{
				String[] parameters = {"-v", "-p", "475871387138",  getTestJarPath()};
				TestingCliFrontend testFrontend = new TestingCliFrontend();
				assertTrue(0 != testFrontend.run(parameters));
			}
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Program caused an exception: " + e.getMessage());
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	public static final class RunTestingCliFrontend extends TestingCliFrontend {
		
		private final int expectedParallelim;
		
		public RunTestingCliFrontend(int expectedParallelim) {
			this.expectedParallelim = expectedParallelim;
		}

		@Override
		protected int executeProgram(PackagedProgram program, Client client, int parallelism) {
			assertEquals(this.expectedParallelim, parallelism);
			return 0;
		}
	}
}
