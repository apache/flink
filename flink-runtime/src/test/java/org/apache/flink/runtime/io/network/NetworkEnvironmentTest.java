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
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionConsumableNotifier;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.taskmanager.TaskActions;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Various tests for the {@link NetworkEnvironment} class.
 */
public class NetworkEnvironmentTest {
	private static final int numBuffers = 1024;

	private static final int memorySegmentSize = 128;

	/**
	 * Verifies that {@link NetworkEnvironment#registerTask(Task)} sets up (un)bounded buffer pool
	 * instances for various types of input and output channels.
	 */
	@Test
	public void testRegisterTaskUsesBoundedBuffers() throws Exception {

		final NetworkEnvironment network = new NetworkEnvironment(
			new NetworkBufferPool(numBuffers, memorySegmentSize),
			new LocalConnectionManager(),
			new ResultPartitionManager(),
			new TaskEventDispatcher(),
			new KvStateRegistry(),
			null,
			null,
			IOManager.IOMode.SYNC,
			0,
			0,
			2,
			8,
			true);

		// result partitions
		ResultPartition rp1 = createResultPartition(ResultPartitionType.PIPELINED, 2);
		ResultPartition rp2 = createResultPartition(ResultPartitionType.BLOCKING, 2);
		ResultPartition rp3 = createResultPartition(ResultPartitionType.PIPELINED_BOUNDED, 2);
		ResultPartition rp4 = createResultPartition(ResultPartitionType.PIPELINED_BOUNDED, 8);
		final ResultPartition[] resultPartitions = new ResultPartition[] {rp1, rp2, rp3, rp4};

		// input gates
		SingleInputGate ig1 = createSingleInputGateMock(ResultPartitionType.PIPELINED, 2);
		SingleInputGate ig2 = createSingleInputGateMock(ResultPartitionType.BLOCKING, 2);
		SingleInputGate ig3 = createSingleInputGateMock(ResultPartitionType.PIPELINED_BOUNDED, 2);
		final SingleInputGate[] inputGates = new SingleInputGate[] {ig1, ig2, ig3};

		// overall task to register
		Task task = mock(Task.class);
		when(task.getProducedPartitions()).thenReturn(resultPartitions);
		when(task.getAllInputGates()).thenReturn(inputGates);

		network.registerTask(task);

		assertEquals(Integer.MAX_VALUE, rp1.getBufferPool().getMaxNumberOfMemorySegments());
		assertEquals(Integer.MAX_VALUE, rp2.getBufferPool().getMaxNumberOfMemorySegments());
		assertEquals(2 * 2 + 8, rp3.getBufferPool().getMaxNumberOfMemorySegments());
		assertEquals(8 * 2 + 8, rp4.getBufferPool().getMaxNumberOfMemorySegments());

		verify(ig1, times(1)).assignExclusiveSegments(network.getNetworkBufferPool(), 2);
		verify(ig2, times(1)).assignExclusiveSegments(network.getNetworkBufferPool(), 2);
		verify(ig3, times(1)).assignExclusiveSegments(network.getNetworkBufferPool(), 2);

		network.shutdown();
	}

	/**
	 * Helper to create simple {@link ResultPartition} instance for use by a {@link Task} inside
	 * {@link NetworkEnvironment#registerTask(Task)}.
	 *
	 * @param partitionType
	 * 		the produced partition type
	 * @param channels
	 * 		the number of output channels
	 *
	 * @return instance with minimal data set and some mocks so that it is useful for {@link
	 * NetworkEnvironment#registerTask(Task)}
	 */
	private static ResultPartition createResultPartition(
			final ResultPartitionType partitionType, final int channels) {
		return new ResultPartition(
			"TestTask-" + partitionType + ":" + channels,
			mock(TaskActions.class),
			new JobID(),
			new ResultPartitionID(),
			partitionType,
			channels,
			channels,
			mock(ResultPartitionManager.class),
			mock(ResultPartitionConsumableNotifier.class),
			mock(IOManager.class),
			false);
	}

	/**
	 * Helper to create a mock of a {@link SingleInputGate} for use by a {@link Task} inside
	 * {@link NetworkEnvironment#registerTask(Task)}.
	 *
	 * @param partitionType
	 * 		the consumed partition type
	 * @param channels
	 * 		the number of input channels
	 *
	 * @return mock with minimal functionality necessary by {@link NetworkEnvironment#registerTask(Task)}
	 */
	private static SingleInputGate createSingleInputGateMock(
			final ResultPartitionType partitionType, final int channels) {
		SingleInputGate ig = mock(SingleInputGate.class);
		when(ig.getConsumedPartitionType()).thenReturn(partitionType);
		when(ig.getNumberOfInputChannels()).thenReturn(channels);
		doAnswer(new Answer<Void>() {
			@Override
			public Void answer(final InvocationOnMock invocation) throws Throwable {
				BufferPool bp = invocation.getArgumentAt(0, BufferPool.class);
				if (partitionType == ResultPartitionType.PIPELINED_BOUNDED) {
					assertEquals(8, bp.getMaxNumberOfMemorySegments());
				} else {
					assertEquals(Integer.MAX_VALUE, bp.getMaxNumberOfMemorySegments());
				}
				return null;
			}
		}).when(ig).setBufferPool(any(BufferPool.class));

		return ig;
	}
}
