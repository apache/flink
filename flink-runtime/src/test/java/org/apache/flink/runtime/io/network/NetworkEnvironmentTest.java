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

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.MemoryType;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.instance.DummyActorGateway;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.netty.NettyConfig;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.messages.JobManagerMessages.ScheduleOrUpdateConsumers;
import org.apache.flink.runtime.taskmanager.NetworkEnvironmentConfiguration;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.NetUtils;
import org.junit.Test;
import scala.Some;
import scala.Tuple2;
import scala.concurrent.duration.FiniteDuration;
import scala.concurrent.impl.Promise;

import java.net.InetAddress;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
			NettyConfig nettyConf = new NettyConfig(InetAddress.getLocalHost(), port, BUFFER_SIZE, new Configuration());
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


	/**
	 * Registers a task with an eager and non-eager partition at the network
	 * environment and verifies that there is exactly on schedule or update
	 * message to the job manager for the eager partition.
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void testEagerlyDeployConsumers() throws Exception {
		// Mock job manager => expected interactions will be verified
		ActorGateway jobManager = mock(ActorGateway.class);
		when(jobManager.ask(anyObject(), any(FiniteDuration.class)))
				.thenReturn(new Promise.DefaultPromise<>().future());

		// Network environment setup
		NetworkEnvironmentConfiguration config = new NetworkEnvironmentConfiguration(
				20,
				1024,
				MemoryType.HEAP,
				IOManager.IOMode.SYNC,
				Some.<NettyConfig>empty(),
				new Tuple2<>(0, 0));

		NetworkEnvironment env = new NetworkEnvironment(
				TestingUtils.defaultExecutionContext(),
				new FiniteDuration(30, TimeUnit.SECONDS),
				config);

		// Associate the environment with the mock actors
		env.associateWithTaskManagerAndJobManager(
				jobManager,
				DummyActorGateway.INSTANCE);

		// Register mock task
		JobID jobId = new JobID();

		ResultPartition[] partitions = new ResultPartition[2];
		partitions[0] = createPartition("p1", jobId, true, env);
		partitions[1] = createPartition("p2", jobId, false, env);

		ResultPartitionWriter[] writers = new ResultPartitionWriter[2];
		writers[0] = new ResultPartitionWriter(partitions[0]);
		writers[1] = new ResultPartitionWriter(partitions[1]);

		Task mockTask = mock(Task.class);
		when(mockTask.getAllInputGates()).thenReturn(new SingleInputGate[0]);
		when(mockTask.getAllWriters()).thenReturn(writers);
		when(mockTask.getProducedPartitions()).thenReturn(partitions);

		env.registerTask(mockTask);

		// Verify
		ResultPartitionID eagerPartitionId = partitions[0].getPartitionId();

		verify(jobManager, times(1)).ask(
				eq(new ScheduleOrUpdateConsumers(jobId, eagerPartitionId)),
				any(FiniteDuration.class));
	}

	/**
	 * Helper to create a mock result partition.
	 */
	private static ResultPartition createPartition(
			String name,
			JobID jobId,
			boolean eagerlyDeployConsumers,
			NetworkEnvironment env) {

		return new ResultPartition(
				name,
				jobId,
				new ResultPartitionID(),
				ResultPartitionType.PIPELINED,
				eagerlyDeployConsumers,
				1,
				env.getPartitionManager(),
				env.getPartitionConsumableNotifier(),
				mock(IOManager.class),
				env.getDefaultIOMode());
	}
}
