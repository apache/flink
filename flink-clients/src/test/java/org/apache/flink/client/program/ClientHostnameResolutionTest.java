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

package org.apache.flink.client.program;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import static org.junit.Assert.*;
import static org.junit.Assume.*;

/**
 * Tests that verify that the client correctly handles non-resolvable host names and does not
 * fail with another exception
 */
public class ClientHostnameResolutionTest {
	
	private static final String nonExistingHostname = "foo.bar.com.invalid";
	
	@Test
	public void testUnresolvableHostname1() {
		
		checkPreconditions();
		
		try {
			InetSocketAddress addr = new InetSocketAddress(nonExistingHostname, 17234);
			new Client(addr, new Configuration(), getClass().getClassLoader(), 1);
			fail("This should fail with an UnknownHostException");
		}
		catch (UnknownHostException e) {
			// that is what we want!
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
			Configuration config = new Configuration();
			config.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, nonExistingHostname);
			config.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, 17234);
			
			new Client(config, getClass().getClassLoader());
			fail("This should fail with an UnknownHostException");
		}
		catch (UnknownHostException e) {
			// that is what we want!
		}
		catch (Exception e) {
			System.err.println("Wrong exception!");
			e.printStackTrace();
			fail(e.getMessage());
		}
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
