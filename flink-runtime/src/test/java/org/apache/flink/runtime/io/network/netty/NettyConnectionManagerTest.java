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
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.util.NetUtils;

import org.apache.flink.shaded.netty4.io.netty.bootstrap.Bootstrap;
import org.apache.flink.shaded.netty4.io.netty.bootstrap.ServerBootstrap;
import org.apache.flink.shaded.netty4.io.netty.channel.EventLoopGroup;

import org.junit.Test;

import java.lang.reflect.Field;
import java.net.InetAddress;

import static org.junit.Assert.assertEquals;

/**
 * Simple netty connection manager test.
 */
public class NettyConnectionManagerTest {

	/**
	 * Tests that the number of arenas and number of threads of the client and
	 * server are set to the same number, that is the number of configured
	 * task slots.
	 */
	@Test
	public void testMatchingNumberOfArenasAndThreadsAsDefault() throws Exception {
		// Expected number of arenas and threads
		int numberOfSlots = 2;

		NettyConfig config = new NettyConfig(
				InetAddress.getLocalHost(),
				NetUtils.getAvailablePort(),
				1024,
				numberOfSlots,
				new Configuration());

		NettyConnectionManager connectionManager = createNettyConnectionManager(config);
		connectionManager.start();

		assertEquals(numberOfSlots, connectionManager.getBufferPool().getNumberOfArenas());

		{
			// Client event loop group
			Bootstrap boostrap = connectionManager.getClient().getBootstrap();
			EventLoopGroup group = boostrap.group();

			Field f = group.getClass().getSuperclass().getSuperclass().getDeclaredField("children");
			f.setAccessible(true);
			Object[] eventExecutors = (Object[]) f.get(group);

			assertEquals(numberOfSlots, eventExecutors.length);
		}

		{
			// Server event loop group
			ServerBootstrap bootstrap = connectionManager.getServer().getBootstrap();
			EventLoopGroup group = bootstrap.group();

			Field f = group.getClass().getSuperclass().getSuperclass().getDeclaredField("children");
			f.setAccessible(true);
			Object[] eventExecutors = (Object[]) f.get(group);

			assertEquals(numberOfSlots, eventExecutors.length);
		}

		{
			// Server child event loop group
			ServerBootstrap bootstrap = connectionManager.getServer().getBootstrap();
			EventLoopGroup group = bootstrap.childGroup();

			Field f = group.getClass().getSuperclass().getSuperclass().getDeclaredField("children");
			f.setAccessible(true);
			Object[] eventExecutors = (Object[]) f.get(group);

			assertEquals(numberOfSlots, eventExecutors.length);
		}
	}

	/**
	 * Tests that the number of arenas and threads can be configured manually.
	 */
	@Test
	public void testManualConfiguration() throws Exception {
		// Expected numbers
		int numberOfArenas = 1;
		int numberOfClientThreads = 3;
		int numberOfServerThreads = 4;

		// Expected number of threads
		Configuration flinkConfig = new Configuration();
		flinkConfig.setInteger(NettyShuffleEnvironmentOptions.NUM_ARENAS, numberOfArenas);
		flinkConfig.setInteger(NettyShuffleEnvironmentOptions.NUM_THREADS_CLIENT, 3);
		flinkConfig.setInteger(NettyShuffleEnvironmentOptions.NUM_THREADS_SERVER, 4);

		NettyConfig config = new NettyConfig(
				InetAddress.getLocalHost(),
				NetUtils.getAvailablePort(),
				1024,
				1337,
				flinkConfig);

		NettyConnectionManager connectionManager = createNettyConnectionManager(config);
		connectionManager.start();

		assertEquals(numberOfArenas, connectionManager.getBufferPool().getNumberOfArenas());

		{
			// Client event loop group
			Bootstrap boostrap = connectionManager.getClient().getBootstrap();
			EventLoopGroup group = boostrap.group();

			Field f = group.getClass().getSuperclass().getSuperclass().getDeclaredField("children");
			f.setAccessible(true);
			Object[] eventExecutors = (Object[]) f.get(group);

			assertEquals(numberOfClientThreads, eventExecutors.length);
		}

		{
			// Server event loop group
			ServerBootstrap bootstrap = connectionManager.getServer().getBootstrap();
			EventLoopGroup group = bootstrap.group();

			Field f = group.getClass().getSuperclass().getSuperclass().getDeclaredField("children");
			f.setAccessible(true);
			Object[] eventExecutors = (Object[]) f.get(group);

			assertEquals(numberOfServerThreads, eventExecutors.length);
		}

		{
			// Server child event loop group
			ServerBootstrap bootstrap = connectionManager.getServer().getBootstrap();
			EventLoopGroup group = bootstrap.childGroup();

			Field f = group.getClass().getSuperclass().getSuperclass().getDeclaredField("children");
			f.setAccessible(true);
			Object[] eventExecutors = (Object[]) f.get(group);

			assertEquals(numberOfServerThreads, eventExecutors.length);
		}
	}

	private NettyConnectionManager createNettyConnectionManager(NettyConfig config) {
		return new NettyConnectionManager(new ResultPartitionManager(), new TaskEventDispatcher(), config, true);
	}
}
