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
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.runtime.util.LeaderRetrievalUtils;
import org.apache.flink.util.TestLogger;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Tests that verify that the LeaderRetrievalSevice correctly handles non-resolvable host names
 * and does not fail with another exception
 */
public class LeaderRetrievalServiceHostnameResolutionTest extends TestLogger {

	private static final String nonExistingHostname = "foo.bar.com.invalid.";

	@Test
	public void testUnresolvableHostname1() {

		try {
			Configuration config = new Configuration();

			config.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, nonExistingHostname);
			config.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, 17234);

			LeaderRetrievalUtils.createLeaderRetrievalService(config);
			fail("This should fail with an UnknownHostException");
		}
		catch (IllegalConfigurationException e) {
			// that is what we want!
		}
		catch (Exception e) {
			System.err.println("Wrong exception!");
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

}
