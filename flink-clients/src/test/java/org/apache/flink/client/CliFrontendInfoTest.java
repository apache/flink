/**
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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import org.apache.commons.cli.CommandLine;
import org.apache.flink.client.CliFrontend;
import org.apache.flink.client.CliFrontendTestUtils.TestingCliFrontend;
import org.apache.flink.client.program.Client;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.compiler.CompilerException;
import org.apache.flink.configuration.Configuration;
import org.junit.BeforeClass;
import org.junit.Test;

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
				CliFrontend testFrontend = new CliFrontend();
				int retCode = testFrontend.cancel(parameters);
				assertTrue(retCode == 2);
			}
			
			// test missing options
			{
				String[] parameters = {};
				CliFrontend testFrontend = new CliFrontend();
				int retCode = testFrontend.cancel(parameters);
				assertTrue(retCode != 0);
			}
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Program caused an exception: " + e.getMessage());
		}
	}
	
	@Test
	public void testShowDescription() {
		try {
			String[] parameters = {"-d", CliFrontendTestUtils.getTestJarPath()};
			CliFrontend testFrontend = new CliFrontend();
			int retCode = testFrontend.info(parameters);
			assertTrue(retCode == 0);
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Program caused an exception: " + e.getMessage());
		}
	}
	
	@Test
	public void testShowExecutionPlan() {
		try {
			String[] parameters = {"-e", CliFrontendTestUtils.getTestJarPath()};
			InfoTestCliFrontend testFrontend = new InfoTestCliFrontend(-1);
			int retCode = testFrontend.info(parameters);
			assertTrue(retCode == 0);
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Program caused an exception: " + e.getMessage());
		}
	}
	
	@Test
	public void testShowExecutionPlanWithParallelism() {
		try {
			String[] parameters = {"-e", "-p", "17", CliFrontendTestUtils.getTestJarPath()};
			InfoTestCliFrontend testFrontend = new InfoTestCliFrontend(17);
			int retCode = testFrontend.info(parameters);
			assertTrue(retCode == 0);
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Program caused an exception: " + e.getMessage());
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	private static final class InfoTestCliFrontend extends TestingCliFrontend {
		
		private final int expectedDop;
		
		public InfoTestCliFrontend(int expectedDop) {
			this.expectedDop = expectedDop;
		}

		@Override
		protected Client getClient(CommandLine line, ClassLoader loader) throws IOException {
			try {
				return new TestClient(expectedDop);
			}
			catch (Exception e) {
				throw new IOException(e);
			}
		}
	}
	
	private static final class TestClient extends Client {
		
		private final int expectedDop;
		
		private TestClient(int expectedDop) throws Exception {
			super(new InetSocketAddress(InetAddress.getLocalHost(), 6176), new Configuration(), CliFrontendInfoTest.class.getClassLoader());
			
			this.expectedDop = expectedDop;
		}
		
		@Override
		public String getOptimizedPlanAsJson(PackagedProgram prog, int parallelism) throws CompilerException, ProgramInvocationException  {
			assertEquals(this.expectedDop, parallelism);
			return "";
		}
	}
}
