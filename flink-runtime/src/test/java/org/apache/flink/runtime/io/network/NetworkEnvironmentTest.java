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
import org.apache.flink.core.memory.MemoryType;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionConsumableNotifier;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.messages.JobManagerMessages.ScheduleOrUpdateConsumers;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.taskmanager.ActorGatewayResultPartitionConsumableNotifier;
import org.apache.flink.runtime.taskmanager.NetworkEnvironmentConfiguration;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.junit.Test;
import scala.concurrent.duration.FiniteDuration;
import scala.concurrent.impl.Promise;

import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class NetworkEnvironmentTest {
	/**
	 * Registers a task with an eager and non-eager partition at the network
	 * environment and verifies that there is exactly on schedule or update
	 * message to the job manager for the eager partition.
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void testEagerlyDeployConsumers() throws Exception {
		// Mock job manager => expected interactions will be verified
		final ActorGateway jobManager = mock(ActorGateway.class);
		when(jobManager.ask(anyObject(), any(FiniteDuration.class)))
				.thenReturn(new Promise.DefaultPromise<>().future());

		// Network environment setup
		NetworkEnvironmentConfiguration config = new NetworkEnvironmentConfiguration(
			20,
			1024,
			MemoryType.HEAP,
			IOManager.IOMode.SYNC,
			0,
			0,
			0,
			null,
			0,
			0);

		NetworkEnvironment env = new NetworkEnvironment(
			new NetworkBufferPool(config.numNetworkBuffers(), config.networkBufferSize(), config.memoryType()),
			new LocalConnectionManager(),
			new ResultPartitionManager(),
			new TaskEventDispatcher(),
			new KvStateRegistry(),
			null,
			config.ioMode(),
			config.partitionRequestInitialBackoff(),
			config.partitinRequestMaxBackoff());

		env.start();

		ResultPartitionConsumableNotifier resultPartitionConsumableNotifier = new ActorGatewayResultPartitionConsumableNotifier(
			TestingUtils.defaultExecutionContext(),
			jobManager,
			new FiniteDuration(30L, TimeUnit.SECONDS));

		// Register mock task
		JobID jobId = new JobID();
		Task mockTask = mock(Task.class);

		ResultPartition[] partitions = new ResultPartition[2];
		partitions[0] = createPartition(mockTask, "p1", jobId, true, env, resultPartitionConsumableNotifier);
		partitions[1] = createPartition(mockTask, "p2", jobId, false, env, resultPartitionConsumableNotifier);

		ResultPartitionWriter[] writers = new ResultPartitionWriter[2];
		writers[0] = new ResultPartitionWriter(partitions[0]);
		writers[1] = new ResultPartitionWriter(partitions[1]);

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
		Task owningTask,
		String name,
		JobID jobId,
		boolean eagerlyDeployConsumers,
		NetworkEnvironment env,
		ResultPartitionConsumableNotifier resultPartitionConsumableNotifier) {

		return new ResultPartition(
			name,
			owningTask,
			jobId,
			new ResultPartitionID(),
			ResultPartitionType.PIPELINED,
			eagerlyDeployConsumers,
			1,
			env.getResultPartitionManager(),
			resultPartitionConsumableNotifier,
			mock(IOManager.class),
			env.getDefaultIOMode());
	}
}
