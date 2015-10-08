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

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Collections;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

public class RemoteExecutorHostnameResolutionTest {

	private static final String nonExistingHostname = "foo.bar.com.invalid";
	private static final int port = 14451;
	
	
	@Test
	public void testUnresolvableHostname1() {
		
		checkPreconditions();
		
		try {
			RemoteExecutor exec = new RemoteExecutor(nonExistingHostname, port);
			exec.executePlan(getProgram());
			fail("This should fail with an ProgramInvocationException");
		}
		catch (IOException e) {
			// that is what we want!
			assertTrue(e.getCause() instanceof UnknownHostException);
		}
		catch (Exception e) {
			System.err.println("Wrong exception!");
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testUnresolvableHostname2() {

		checkPreconditions();
		
		try {
			InetSocketAddress add = new InetSocketAddress(nonExistingHostname, port);
			RemoteExecutor exec = new RemoteExecutor(add, new Configuration(),
				Collections.<URL>emptyList(), Collections.<URL>emptyList());
			exec.executePlan(getProgram());
			fail("This should fail with an ProgramInvocationException");
		}
		catch (IOException e) {
			// that is what we want!
			assertTrue(e.getCause() instanceof UnknownHostException);
		}
		catch (Exception e) {
			System.err.println("Wrong exception!");
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	private static Plan getProgram() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.fromElements(1, 2, 3).output(new DiscardingOutputFormat<Integer>());
		return env.createProgramPlan();
	}

	private static void checkPreconditions() {
		// the test can only work if the invalid URL cannot be resolves
		// some internet providers resolve unresolvable URLs to navigational aid servers,
		// voiding this test.
		boolean throwsException;
		try {
			//noinspection ResultOfMethodCallIgnored
			InetAddress.getByName(nonExistingHostname);
			throwsException = false;
		}
		catch (UnknownHostException e) {
			throwsException = true;
		}
		assumeTrue(throwsException);
	}
}
