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

package org.apache.flink.runtime.util;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.rpc.akka.AkkaRpcServiceUtils;
import org.apache.flink.util.TestLogger;

import org.junit.BeforeClass;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

/**
 * Unit tests for respecting {@link HighAvailabilityServicesUtils.AddressResolution}.
 */
public class AddressResolutionTest extends TestLogger {

	private static final String ENDPOINT_NAME = "endpoint";
	private static final String NON_EXISTING_HOSTNAME = "foo.bar.com.invalid";
	private static final int PORT = 17234;

	@BeforeClass
	public static void check() {
		checkPreconditions();
	}

	private static void checkPreconditions() {
		// the test can only work if the invalid URL cannot be resolves
		// some internet providers resolve unresolvable URLs to navigational aid servers,
		// voiding this test.
		boolean throwsException;

		try {
			//noinspection ResultOfMethodCallIgnored
			InetAddress.getByName(NON_EXISTING_HOSTNAME);
			throwsException = false;
		} catch (UnknownHostException e) {
			throwsException = true;
		}

		assumeTrue(throwsException);
	}

	@Test
	public void testNoAddressResolution() throws UnknownHostException {
		AkkaRpcServiceUtils.getRpcUrl(
			NON_EXISTING_HOSTNAME,
			PORT,
			ENDPOINT_NAME,
			HighAvailabilityServicesUtils.AddressResolution.NO_ADDRESS_RESOLUTION,
			new Configuration());
	}

	@Test
	public void testTryAddressResolution() {
		try {
			AkkaRpcServiceUtils.getRpcUrl(
				NON_EXISTING_HOSTNAME,
				PORT,
				ENDPOINT_NAME,
				HighAvailabilityServicesUtils.AddressResolution.TRY_ADDRESS_RESOLUTION,
				new Configuration());
			fail("This should fail with an UnknownHostException");
		} catch (UnknownHostException ignore) {
			// expected
		}
	}
}
