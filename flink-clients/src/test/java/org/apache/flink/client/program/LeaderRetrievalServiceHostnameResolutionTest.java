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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.jobmaster.JobMaster;
import org.apache.flink.runtime.util.StandaloneUtils;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.util.TestLogger;

import org.junit.BeforeClass;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

/**
 * Tests that verify that the LeaderRetrievalService correctly handles non-resolvable host names
 * and does not fail with another exception.
 */
public class LeaderRetrievalServiceHostnameResolutionTest extends TestLogger {

	private static final String nonExistingHostname = "foo.bar.com.invalid";

	@BeforeClass
	public static void check() {
		checkPreconditions();
	}

	/*
	 * Tests that the StandaloneLeaderRetrievalService resolves host names if specified.
	 */
	@Test
	public void testUnresolvableHostname1() throws UnknownHostException, ConfigurationException {
		Configuration config = new Configuration();

		config.setString(JobManagerOptions.ADDRESS, nonExistingHostname);
		config.setInteger(JobManagerOptions.PORT, 17234);

		StandaloneUtils.createLeaderRetrievalService(
			config,
			false,
			JobMaster.JOB_MANAGER_NAME);
	}

	/*
	 * Tests that the StandaloneLeaderRetrievalService does not resolve host names by default.
	 */
	@Test
	public void testUnresolvableHostname2() throws Exception {

		try {
			Configuration config = new Configuration();

			config.setString(JobManagerOptions.ADDRESS, nonExistingHostname);
			config.setInteger(JobManagerOptions.PORT, 17234);

			StandaloneUtils.createLeaderRetrievalService(
				config,
				true,
				JobMaster.JOB_MANAGER_NAME);
			fail("This should fail with an UnknownHostException");
		}
		catch (UnknownHostException e) {
			// that is what we want!
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
