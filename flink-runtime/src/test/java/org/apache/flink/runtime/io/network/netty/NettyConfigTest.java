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

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.util.NetUtils;
import org.junit.Test;

import java.net.InetAddress;

import static org.junit.Assert.assertEquals;

/**
 * Tests for the netty related config.
 */
public class NettyConfigTest {

	@Test
	public void testNettyArenaNum() throws Exception {
		final Configuration config = new Configuration();
		final int nettyMemoryMB = 79;
		final int numArenas = 10;
		final int numSlots = 1;
		config.setInteger(TaskManagerOptions.TASK_MANAGER_PROCESS_NETTY_MEMORY.key(), nettyMemoryMB);
		config.setInteger(NettyConfig.NUM_ARENAS.key(), numArenas);
		config.setInteger(NettyConfig.MAX_ORDER, 9);

		NettyConfig nettyConfig1 = createNettyConfig(numSlots, config);
		// The num equals to configured value
		assertEquals(numArenas, nettyConfig1.getNumberOfArenas());

		config.setInteger(NettyConfig.NUM_ARENAS.key(), -1);
		NettyConfig nettyConfig2 = createNettyConfig(numSlots, config);
		// The num equals to min{numSlots, nettyMemory/chunkSize}
		assertEquals(numSlots, nettyConfig2.getNumberOfArenas());

		NettyConfig nettyConfig3 = createNettyConfig(30, config);
		// The num equals to min{numSlots, nettyMemory/chunkSize}
		assertEquals(18, nettyConfig3.getNumberOfArenas());
	}

	@Test
	public void testNettyMaxOrderConfig() throws Exception {
		final Configuration config = new Configuration();

		final int[] configuredMaxOrder = new int[]{3, 7, 11, 13};
		final int[] expectedMaxOrder = new int[]{7, 7, 11, 13};

		for (int i = 0; i < configuredMaxOrder.length; ++i) {
			config.setInteger(NettyConfig.MAX_ORDER, configuredMaxOrder[i]);
			NettyConfig nettyConfig = createNettyConfig(1, config);
			int effectiveMaxOrder = nettyConfig.getMaxOrder();

			assertEquals("The " + i + " test configured " + configuredMaxOrder[i] +
					" and expected to read " + expectedMaxOrder[i] + ", but we got " + effectiveMaxOrder,
				expectedMaxOrder[i], effectiveMaxOrder);
		}
	}

	private NettyConfig createNettyConfig(int numberOfSlots, Configuration config) throws Exception {
		return new NettyConfig(
			InetAddress.getLocalHost(),
			NetUtils.getAvailablePort(),
			1024,
			numberOfSlots,
			config);
	}
}
