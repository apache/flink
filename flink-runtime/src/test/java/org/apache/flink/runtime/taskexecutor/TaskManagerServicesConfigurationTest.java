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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.NetworkEnvironmentOptions;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.net.InetAddress;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for {@link TaskManagerServicesConfiguration}.
 */
public class TaskManagerServicesConfigurationTest extends TestLogger {

	private static final long MEM_SIZE_PARAM = 128L * 1024 * 1024;

	/**
	 * Verifies that {@link TaskManagerServicesConfiguration#fromConfiguration(Configuration, long, InetAddress, boolean)}
	 * returns the correct result for new configurations via
	 * {@link NetworkEnvironmentOptions#NETWORK_REQUEST_BACKOFF_INITIAL},
	 * {@link NetworkEnvironmentOptions#NETWORK_REQUEST_BACKOFF_MAX},
	 * {@link NetworkEnvironmentOptions#NETWORK_BUFFERS_PER_CHANNEL} and
	 * {@link NetworkEnvironmentOptions#NETWORK_EXTRA_BUFFERS_PER_GATE}
	 */
	@Test
	public void testNetworkRequestBackoffAndBuffers() throws Exception {

		// set some non-default values
		final Configuration config = new Configuration();
		config.setInteger(NetworkEnvironmentOptions.NETWORK_REQUEST_BACKOFF_INITIAL, 100);
		config.setInteger(NetworkEnvironmentOptions.NETWORK_REQUEST_BACKOFF_MAX, 200);
		config.setInteger(NetworkEnvironmentOptions.NETWORK_BUFFERS_PER_CHANNEL, 10);
		config.setInteger(NetworkEnvironmentOptions.NETWORK_EXTRA_BUFFERS_PER_GATE, 100);

		TaskManagerServicesConfiguration tmConfig =
			TaskManagerServicesConfiguration.fromConfiguration(config, MEM_SIZE_PARAM, InetAddress.getLoopbackAddress(), true);

		assertEquals(tmConfig.getNetworkConfig().partitionRequestInitialBackoff(), 100);
		assertEquals(tmConfig.getNetworkConfig().partitionRequestMaxBackoff(), 200);
		assertEquals(tmConfig.getNetworkConfig().networkBuffersPerChannel(), 10);
		assertEquals(tmConfig.getNetworkConfig().floatingNetworkBuffersPerGate(), 100);
	}
}
