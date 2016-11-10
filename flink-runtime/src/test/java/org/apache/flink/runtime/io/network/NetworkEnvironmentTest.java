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

package org.apache.flink.runtime.io.network;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.MemoryType;
import org.apache.flink.runtime.instance.DummyActorGateway;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.netty.NettyConfig;
import org.apache.flink.runtime.taskmanager.NetworkEnvironmentConfiguration;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.NetUtils;
import org.junit.Test;
import scala.Some;
import scala.Tuple2;
import scala.concurrent.duration.FiniteDuration;

import java.net.InetAddress;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class NetworkEnvironmentTest {

	@Test
	public void testAssociateDisassociate() {
		final int BUFFER_SIZE = 1024;
		final int NUM_BUFFERS = 20;

		final int port;
		try {
			port = NetUtils.getAvailablePort();
		}
		catch (Throwable t) {
			// ignore
			return;
		}

		try {
			NettyConfig nettyConf = new NettyConfig(InetAddress.getLocalHost(), port, BUFFER_SIZE, 1, new Configuration());
			NetworkEnvironmentConfiguration config = new NetworkEnvironmentConfiguration(
					NUM_BUFFERS, BUFFER_SIZE, MemoryType.HEAP,
					IOManager.IOMode.SYNC, new Some<>(nettyConf),
					new Tuple2<>(0, 0));

			NetworkEnvironment env = new NetworkEnvironment(
				TestingUtils.defaultExecutionContext(),
				new FiniteDuration(30, TimeUnit.SECONDS),
				config);

			assertFalse(env.isShutdown());
			assertFalse(env.isAssociated());

			// pool must be started already
			assertNotNull(env.getNetworkBufferPool());
			assertEquals(NUM_BUFFERS, env.getNetworkBufferPool().getTotalNumberOfMemorySegments());

			// others components are still shut down
			assertNull(env.getConnectionManager());
			assertNull(env.getPartitionConsumableNotifier());
			assertNull(env.getTaskEventDispatcher());
			assertNull(env.getPartitionManager());

			// associate the environment with some mock actors
			env.associateWithTaskManagerAndJobManager(
					DummyActorGateway.INSTANCE,
					DummyActorGateway.INSTANCE);

			assertNotNull(env.getConnectionManager());
			assertNotNull(env.getPartitionConsumableNotifier());
			assertNotNull(env.getTaskEventDispatcher());
			assertNotNull(env.getPartitionManager());

			// allocate some buffer pool
			BufferPool localPool = env.getNetworkBufferPool().createBufferPool(10, false);
			assertNotNull(localPool);

			// disassociate
			env.disassociate();

			assertNull(env.getConnectionManager());
			assertNull(env.getPartitionConsumableNotifier());
			assertNull(env.getTaskEventDispatcher());
			assertNull(env.getPartitionManager());

			assertNotNull(env.getNetworkBufferPool());
			assertTrue(localPool.isDestroyed());

			// associate once again
			env.associateWithTaskManagerAndJobManager(
					DummyActorGateway.INSTANCE,
					DummyActorGateway.INSTANCE
			);

			assertNotNull(env.getConnectionManager());
			assertNotNull(env.getPartitionConsumableNotifier());
			assertNotNull(env.getTaskEventDispatcher());
			assertNotNull(env.getPartitionManager());

			// shutdown for good
			env.shutdown();

			assertTrue(env.isShutdown());
			assertFalse(env.isAssociated());
			assertNull(env.getConnectionManager());
			assertNull(env.getPartitionConsumableNotifier());
			assertNull(env.getTaskEventDispatcher());
			assertNull(env.getPartitionManager());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
