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

import org.apache.flink.client.cli.CommandLineOptions;
import org.apache.flink.client.program.Client;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.optimizer.CompilerException;
import org.apache.flink.configuration.Configuration;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.InetAddress;

import static org.junit.Assert.*;

public class CliFrontendInfoTest {
	
	@BeforeClass
	public static void init() {
		CliFrontendTestUtils.pipeSystemOutToNull();
		CliFrontendTestUtils.clearGlobalConfiguration();
	}
	
	@Test
	public void testErrorCases() {
		try {
			// test unrecognized option
			{
				String[] parameters = {"-v", "-l"};
				CliFrontend testFrontend = new CliFrontend(CliFrontendTestUtils.getConfigDir());
				int retCode = testFrontend.cancel(parameters);
				assertTrue(retCode != 0);
			}
			
			// test missing options
			{
				String[] parameters = {};
				CliFrontend testFrontend = new CliFrontend(CliFrontendTestUtils.getConfigDir());
				int retCode = testFrontend.cancel(parameters);
				assertTrue(retCode != 0);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail("Program caused an exception: " + e.getMessage());
		}
	}
	
	@Test
	public void testShowExecutionPlan() {
		try {
			String[] parameters = new String[] { CliFrontendTestUtils.getTestJarPath() };
			InfoTestCliFrontend testFrontend = new InfoTestCliFrontend(-1);
			int retCode = testFrontend.info(parameters);
			assertTrue(retCode == 0);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail("Program caused an exception: " + e.getMessage());
		}
	}
	
	@Test
	public void testShowExecutionPlanWithParallelism() {
		try {
			String[] parameters = {"-p", "17", CliFrontendTestUtils.getTestJarPath()};
			InfoTestCliFrontend testFrontend = new InfoTestCliFrontend(17);
			int retCode = testFrontend.info(parameters);
			assertTrue(retCode == 0);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail("Program caused an exception: " + e.getMessage());
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	private static final class InfoTestCliFrontend extends CliFrontend {
		
		private final int expectedDop;
		
		public InfoTestCliFrontend(int expectedDop) throws Exception {
			super(CliFrontendTestUtils.getConfigDir());
			this.expectedDop = expectedDop;
		}

		@Override
		protected Client getClient(CommandLineOptions options, ClassLoader loader, String programName, int par)
				throws Exception {
			Configuration config = new Configuration();

			config.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, InetAddress.getLocalHost().getHostName());
			config.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, 6176);

			return new TestClient(config, expectedDop);
		}
	}
	
	private static final class TestClient extends Client {
		
		private final int expectedDop;
		
		private TestClient(Configuration config, int expectedDop) throws Exception {
			super(config, CliFrontendInfoTest.class.getClassLoader(), -1);
			
			this.expectedDop = expectedDop;
		}
		
		@Override
		public String getOptimizedPlanAsJson(PackagedProgram prog, int parallelism)
				throws CompilerException, ProgramInvocationException
		{
			assertEquals(this.expectedDop, parallelism);
			return "";
		}
	}
}
