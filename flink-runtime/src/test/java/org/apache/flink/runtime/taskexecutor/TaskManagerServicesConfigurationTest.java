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
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.net.InetAddress;

import static org.junit.Assert.*;

/**
 * Unit test for {@link TaskManagerServicesConfiguration}.
 */
public class TaskManagerServicesConfigurationTest extends TestLogger {
	/**
	 * Verifies that {@link TaskManagerServicesConfiguration#hasNewNetworkBufConf(Configuration)}
	 * returns the correct result for old configurations via
	 * {@link TaskManagerOptions#NETWORK_NUM_BUFFERS}.
	 */
	@SuppressWarnings("deprecation")
	@Test
	public void hasNewNetworkBufConfOld() throws Exception {
		Configuration config = new Configuration();
		config.setInteger(TaskManagerOptions.NETWORK_NUM_BUFFERS, 1);

		assertFalse(TaskManagerServicesConfiguration.hasNewNetworkBufConf(config));
	}

	/**
	 * Verifies that {@link TaskManagerServicesConfiguration#hasNewNetworkBufConf(Configuration)}
	 * returns the correct result for new configurations via
	 * {@link TaskManagerOptions#NETWORK_BUFFERS_MEMORY_FRACTION},
	 * {@link TaskManagerOptions#NETWORK_BUFFERS_MEMORY_MIN} and {@link
	 * TaskManagerOptions#NETWORK_BUFFERS_MEMORY_MAX}.
	 */
	@Test
	public void hasNewNetworkBufConfNew() throws Exception {
		Configuration config = new Configuration();
		assertTrue(TaskManagerServicesConfiguration.hasNewNetworkBufConf(config));

		// fully defined:
		config.setFloat(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_FRACTION, 0.1f);
		config.setString(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MIN, "1024");
		config.setString(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MAX, "2048");

		assertTrue(TaskManagerServicesConfiguration.hasNewNetworkBufConf(config));

		// partly defined:
		config = new Configuration();
		config.setFloat(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_FRACTION, 0.1f);
		assertTrue(TaskManagerServicesConfiguration.hasNewNetworkBufConf(config));
		config.setString(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MAX, "1024");
		assertTrue(TaskManagerServicesConfiguration.hasNewNetworkBufConf(config));

		config = new Configuration();
		config.setString(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MIN, "1024");
		assertTrue(TaskManagerServicesConfiguration.hasNewNetworkBufConf(config));
		config.setFloat(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_FRACTION, 0.1f);
		assertTrue(TaskManagerServicesConfiguration.hasNewNetworkBufConf(config));

		config = new Configuration();
		config.setString(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MAX, "1024");
		assertTrue(TaskManagerServicesConfiguration.hasNewNetworkBufConf(config));
		config.setString(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MIN, "1024");
		assertTrue(TaskManagerServicesConfiguration.hasNewNetworkBufConf(config));
	}

	/**
	 * Verifies that {@link TaskManagerServicesConfiguration#hasNewNetworkBufConf(Configuration)}
	 * returns the correct result for new configurations via
	 * {@link TaskManagerOptions#NETWORK_REQUEST_BACKOFF_INITIAL},
	 * {@link TaskManagerOptions#NETWORK_REQUEST_BACKOFF_MAX},
	 * {@link TaskManagerOptions#NETWORK_BUFFERS_PER_CHANNEL} and
	 * {@link TaskManagerOptions#NETWORK_EXTRA_BUFFERS_PER_GATE}
	 */
	@Test
	public void testNetworkRequestBackoffAndBuffers() throws Exception {

		// set some non-default values
		final Configuration config = new Configuration();
		config.setInteger(TaskManagerOptions.NETWORK_REQUEST_BACKOFF_INITIAL, 100);
		config.setInteger(TaskManagerOptions.NETWORK_REQUEST_BACKOFF_MAX, 200);
		config.setInteger(TaskManagerOptions.NETWORK_BUFFERS_PER_CHANNEL, 10);
		config.setInteger(TaskManagerOptions.NETWORK_EXTRA_BUFFERS_PER_GATE, 100);

		TaskManagerServicesConfiguration tmConfig =
			TaskManagerServicesConfiguration.fromConfiguration(config, InetAddress.getLoopbackAddress(), true);

		assertEquals(tmConfig.getNetworkConfig().partitionRequestInitialBackoff(), 100);
		assertEquals(tmConfig.getNetworkConfig().partitionRequestMaxBackoff(), 200);
		assertEquals(tmConfig.getNetworkConfig().networkBuffersPerChannel(), 10);
		assertEquals(tmConfig.getNetworkConfig().floatingNetworkBuffersPerGate(), 100);
	}

	/**
	 * Verifies that {@link TaskManagerServicesConfiguration#hasNewNetworkBufConf(Configuration)}
	 * returns the correct result for mixed old/new configurations.
	 */
	@SuppressWarnings("deprecation")
	@Test
	public void hasNewNetworkBufConfMixed() throws Exception {
		Configuration config = new Configuration();
		assertTrue(TaskManagerServicesConfiguration.hasNewNetworkBufConf(config));

		config.setInteger(TaskManagerOptions.NETWORK_NUM_BUFFERS, 1);
		assertFalse(TaskManagerServicesConfiguration.hasNewNetworkBufConf(config));

		// old + 1 new parameter = new:
		Configuration config1 = config.clone();
		config1.setFloat(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_FRACTION, 0.1f);
		assertTrue(TaskManagerServicesConfiguration.hasNewNetworkBufConf(config1));

		config1 = config.clone();
		config1.setString(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MIN, "1024");
		assertTrue(TaskManagerServicesConfiguration.hasNewNetworkBufConf(config1));

		config1 = config.clone();
		config1.setString(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MAX, "1024");
		assertTrue(TaskManagerServicesConfiguration.hasNewNetworkBufConf(config1));
	}

}
