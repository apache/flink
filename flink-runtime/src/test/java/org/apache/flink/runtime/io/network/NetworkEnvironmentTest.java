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
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.InternalResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionConsumableNotifier;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.PartitionRequestManager;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.taskmanager.TaskActions;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;

import static org.apache.flink.runtime.io.network.partition.InputChannelTestUtils.createDummyConnectionManager;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.spy;

/**
 * Various tests for the {@link NetworkEnvironment} class.
 */
@RunWith(Parameterized.class)
public class NetworkEnvironmentTest {
	private static final int numBuffers = 1024;

	private static final int memorySegmentSize = 128;

	@Parameterized.Parameter
	public boolean enableCreditBasedFlowControl;

	@Parameterized.Parameters(name = "Credit-based = {0}")
	public static List<Boolean> parameters() {
		return Arrays.asList(Boolean.TRUE, Boolean.FALSE);
	}

	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	/**
	 * Verifies that {@link NetworkEnvironment#registerTask(Task)} sets up (un)bounded buffer pool
	 * instances for various types of input and output channels.
	 */
	@Test
	public void testRegisterTaskUsesBoundedBuffers() throws Exception {
		int networkBuffersPerChannel = 2;
		int extraNetworkBuffersPerGate = 8;
		int networkBuffersPerBlockingChannel = 128;
		int extraNetworkBuffersPerBlockingGate = 0;
		int networkBuffersPerSubpartition = 2;

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
			networkBuffersPerChannel,
			extraNetworkBuffersPerGate,
			networkBuffersPerBlockingChannel,
			extraNetworkBuffersPerBlockingGate,
			networkBuffersPerSubpartition,
			enableCreditBasedFlowControl);

		// result partitions
		InternalResultPartition rp1 = createResultPartition(ResultPartitionType.PIPELINED, 2);
		InternalResultPartition rp2 = createResultPartition(ResultPartitionType.BLOCKING, 2);
		InternalResultPartition rp3 = createResultPartition(ResultPartitionType.PIPELINED, 2);
		InternalResultPartition rp4 = createResultPartition(ResultPartitionType.PIPELINED, 8);
		final InternalResultPartition[] internalResultPartitions = new InternalResultPartition[] {rp1, rp2, rp3, rp4};

		// input gates
		SingleInputGate ig1 = createSingleInputGate(ResultPartitionType.PIPELINED, 2);
		SingleInputGate ig2 = createSingleInputGate(ResultPartitionType.BLOCKING, 2);
		SingleInputGate ig3 = createSingleInputGate(ResultPartitionType.PIPELINED, 2);
		SingleInputGate ig4 = createSingleInputGate(ResultPartitionType.PIPELINED, 8);
		final SingleInputGate[] inputGates = new SingleInputGate[] {ig1, ig2, ig3, ig4};

		// overall task to register
		Task task = mock(Task.class);
		when(task.getInternalPartitions()).thenReturn(Arrays.asList(internalResultPartitions));
		when(task.getAllInputGates()).thenReturn(inputGates);

		network.registerTask(task);

		// verify buffer pools for the result partitions
		for (InternalResultPartition rp : internalResultPartitions) {
			assertEquals(rp.getNumberOfSubpartitions() * networkBuffersPerSubpartition + extraNetworkBuffersPerGate, rp.getBufferPool().getNumberOfRequiredMemorySegments());
			assertEquals(rp.getNumberOfSubpartitions() * networkBuffersPerSubpartition + extraNetworkBuffersPerGate, rp.getBufferPool().getMaxNumberOfMemorySegments());
		}

		// verify buffer pools for the input gates (NOTE: credit-based uses minimum required buffers
		// for exclusive buffers not managed by the buffer pool)
		for (SingleInputGate ig: inputGates) {
			assertEquals(enableCreditBasedFlowControl ? 0 : ig.getNumberOfInputChannels(), ig.getBufferPool().getNumberOfRequiredMemorySegments());
			assertEquals(enableCreditBasedFlowControl ? extraNetworkBuffersPerGate : networkBuffersPerChannel * ig.getNumberOfInputChannels() + extraNetworkBuffersPerGate, ig.getBufferPool().getMaxNumberOfMemorySegments());
			verify(ig, times(enableCreditBasedFlowControl ? 1 : 0)).setNetworkProperties(any(NetworkBufferPool.class), anyInt());
		}

		for (InternalResultPartition rp : internalResultPartitions) {
			rp.release();
		}
		for (SingleInputGate ig : inputGates) {
			ig.releaseAllResources();
		}
		network.shutdown();
	}

	/**
	 * Verifies that {@link NetworkEnvironment#registerTask(Task)} sets up (un)bounded buffer pool
	 * instances for various types of input and output channels working with the bare minimum of
	 * required buffers.
	 */
	@Test
	public void testRegisterTaskWithLimitedBuffers() throws Exception {
		final int bufferCount;
		// outgoing: 1 buffer per channel (always)
		if (!enableCreditBasedFlowControl) {
			// incoming: 1 buffer per channel
			bufferCount = 10 + 10 * 2 + 8 * 4;
		} else {
			// incoming: 2 exclusive buffers per channel
			bufferCount = 10 * 2 + 2 * 128 + 8 * 3 + 2 * 8;
		}

		testRegisterTaskWithLimitedBuffers(bufferCount);
	}

	/**
	 * Verifies that {@link NetworkEnvironment#registerTask(Task)} fails if the bare minimum of
	 * required buffers is not available (we are one buffer short).
	 */
	@Test
	public void testRegisterTaskWithInsufficientBuffers() throws Exception {
		final int bufferCount;
		// outgoing: 1 buffer per channel (always)
		if (!enableCreditBasedFlowControl) {
			// incoming: 1 buffer per channel
			bufferCount = 19;
		} else {
			// incoming: 2 exclusive buffers per channel
			bufferCount = 10 + 10 * TaskManagerOptions.NETWORK_BUFFERS_PER_CHANNEL.defaultValue() - 1;
		}

		expectedException.expect(IOException.class);
		expectedException.expectMessage("Insufficient number of network buffers");
		testRegisterTaskWithLimitedBuffers(bufferCount);
	}

	private void testRegisterTaskWithLimitedBuffers(int bufferPoolSize) throws Exception {
		int networkBuffersPerChannel = 2;
		int extraNetworkBuffersPerGate = 8;
		int networkBuffersPerBlockingChannel = 128;
		int extraNetworkBuffersPerBlockingGate = 0;
		int networkBuffersPerSubpartition = 2;

		final NetworkEnvironment network = new NetworkEnvironment(
			new NetworkBufferPool(bufferPoolSize, memorySegmentSize),
			new LocalConnectionManager(),
			new ResultPartitionManager(),
			new TaskEventDispatcher(),
			new KvStateRegistry(),
			null,
			null,
			IOManager.IOMode.SYNC,
			0,
			0,
			networkBuffersPerChannel,
			extraNetworkBuffersPerGate,
			networkBuffersPerBlockingChannel,
			extraNetworkBuffersPerBlockingGate,
			networkBuffersPerSubpartition,
			enableCreditBasedFlowControl);

		final ConnectionManager connManager = createDummyConnectionManager();

		// result partitions
		InternalResultPartition rp1 = createResultPartition(ResultPartitionType.PIPELINED, 2);
		InternalResultPartition rp2 = createResultPartition(ResultPartitionType.BLOCKING, 2);
		InternalResultPartition rp3 = createResultPartition(ResultPartitionType.PIPELINED, 2);
		InternalResultPartition rp4 = createResultPartition(ResultPartitionType.PIPELINED, 4);
		final InternalResultPartition[] internalResultPartitions = new InternalResultPartition[] {rp1, rp2, rp3, rp4};

		// input gates
		SingleInputGate ig1 = createSingleInputGate(ResultPartitionType.PIPELINED, 2);
		SingleInputGate ig2 = createSingleInputGate(ResultPartitionType.BLOCKING, 2);
		SingleInputGate ig3 = createSingleInputGate(ResultPartitionType.PIPELINED, 2);
		SingleInputGate ig4 = createSingleInputGate(ResultPartitionType.PIPELINED, 4);
		final SingleInputGate[] inputGates = new SingleInputGate[] {ig1, ig2, ig3, ig4};

		// set up remote input channels for the exclusive buffers of the credit-based flow control
		// (note that this does not obey the partition types which is ok for the scope of the test)
		if (enableCreditBasedFlowControl) {
			createRemoteInputChannel(ig4, 0, rp1, connManager);
			createRemoteInputChannel(ig4, 0, rp2, connManager);
			createRemoteInputChannel(ig4, 0, rp3, connManager);
			createRemoteInputChannel(ig4, 0, rp4, connManager);

			createRemoteInputChannel(ig1, 1, rp1, connManager);
			createRemoteInputChannel(ig1, 1, rp4, connManager);

			createRemoteInputChannel(ig2, 1, rp2, connManager);
			createRemoteInputChannel(ig2, 2, rp4, connManager);

			createRemoteInputChannel(ig3, 1, rp3, connManager);
			createRemoteInputChannel(ig3, 3, rp4, connManager);
		}

		// overall task to register
		Task task = mock(Task.class);
		when(task.getInternalPartitions()).thenReturn(Arrays.asList(internalResultPartitions));
		when(task.getAllInputGates()).thenReturn(inputGates);

		network.registerTask(task);

		for (InternalResultPartition rp : internalResultPartitions) {
			assertEquals(rp.getNumberOfSubpartitions() * networkBuffersPerSubpartition + extraNetworkBuffersPerGate, rp.getBufferPool().getNumberOfRequiredMemorySegments());
			assertEquals(rp.getNumberOfSubpartitions() * networkBuffersPerSubpartition + extraNetworkBuffersPerGate, rp.getBufferPool().getNumBuffers());
			assertEquals(rp.getNumberOfSubpartitions() * networkBuffersPerSubpartition + extraNetworkBuffersPerGate, rp.getBufferPool().getMaxNumberOfMemorySegments());
		}

		// verify buffer pools for the input gates (NOTE: credit-based uses minimum required buffers
		// for exclusive buffers not managed by the buffer pool)
		for (SingleInputGate ig: inputGates) {
			assertEquals(enableCreditBasedFlowControl ? 0 : ig.getNumberOfInputChannels(), ig.getBufferPool().getNumberOfRequiredMemorySegments());
			assertEquals(enableCreditBasedFlowControl ? extraNetworkBuffersPerGate : networkBuffersPerChannel * ig.getNumberOfInputChannels() + extraNetworkBuffersPerGate, ig.getBufferPool().getMaxNumberOfMemorySegments());
			verify(ig, times(enableCreditBasedFlowControl ? 1 : 0)).setNetworkProperties(any(NetworkBufferPool.class), anyInt());
		}

		if (enableCreditBasedFlowControl) {
			for (SingleInputGate ig : inputGates) {
				ig.assignExclusiveSegments();
			}
		}

		for (InternalResultPartition rp : internalResultPartitions) {
			rp.release();
		}
		for (SingleInputGate ig : inputGates) {
			ig.releaseAllResources();
		}
		network.shutdown();
	}

	/**
	 * Verifies that {@link NetworkEnvironment#registerTask(Task)} sets up buffer pool instances for
	 * various types of input and output channels when the per-gate parameters are negative.
	 */
	@Test
	public void registerTaskWithNegativeExtraGateBuffer() throws Exception {
		int networkBuffersPerChannel = 2;
		int extraNetworkBuffersPerGate = -1;
		int networkBuffersPerBlockingChannel = 128;
		int extraNetworkBuffersPerBlockingGate = -1;
		int networkBuffersPerSubpartition = 2;

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
			networkBuffersPerChannel,
			extraNetworkBuffersPerGate,
			networkBuffersPerBlockingChannel,
			extraNetworkBuffersPerBlockingGate,
			networkBuffersPerSubpartition,
			enableCreditBasedFlowControl);

		// result partitions
		InternalResultPartition rp1 = createResultPartition(ResultPartitionType.PIPELINED, 2);
		InternalResultPartition rp2 = createResultPartition(ResultPartitionType.BLOCKING, 2);
		InternalResultPartition rp3 = createResultPartition(ResultPartitionType.PIPELINED, 2);
		InternalResultPartition rp4 = createResultPartition(ResultPartitionType.PIPELINED, 8);
		final InternalResultPartition[] internalResultPartitions = new InternalResultPartition[] {rp1, rp2, rp3, rp4};

		// input gates
		SingleInputGate ig1 = createSingleInputGate(ResultPartitionType.PIPELINED, 2);
		SingleInputGate ig2 = createSingleInputGate(ResultPartitionType.BLOCKING, 2);
		SingleInputGate ig3 = createSingleInputGate(ResultPartitionType.PIPELINED, 2);
		SingleInputGate ig4 = createSingleInputGate(ResultPartitionType.PIPELINED, 8);
		final SingleInputGate[] inputGates = new SingleInputGate[] {ig1, ig2, ig3, ig4};

		// overall task to register
		Task task = mock(Task.class);
		when(task.getInternalPartitions()).thenReturn(Arrays.asList(internalResultPartitions));
		when(task.getAllInputGates()).thenReturn(inputGates);

		network.registerTask(task);

		// verify buffer pools for the result partitions
		for (InternalResultPartition rp: internalResultPartitions) {
			assertEquals(2 * rp.getNumberOfSubpartitions() * networkBuffersPerSubpartition, rp.getBufferPool().getNumberOfRequiredMemorySegments());
			assertEquals(2 * rp.getNumberOfSubpartitions() * networkBuffersPerSubpartition, rp.getBufferPool().getMaxNumberOfMemorySegments());
		}

		// verify buffer pools for the input gates (NOTE: credit-based uses minimum required buffers
		// for exclusive buffers not managed by the buffer pool)
		for (SingleInputGate ig: inputGates) {
			assertEquals(enableCreditBasedFlowControl ? 0 : ig.getNumberOfInputChannels(), ig.getBufferPool().getNumberOfRequiredMemorySegments());
			assertEquals(enableCreditBasedFlowControl ? networkBuffersPerChannel * ig.getNumberOfInputChannels() : 2 * networkBuffersPerChannel * ig.getNumberOfInputChannels(), ig.getBufferPool().getMaxNumberOfMemorySegments());
			verify(ig, times(enableCreditBasedFlowControl ? 1 : 0)).setNetworkProperties(any(NetworkBufferPool.class), anyInt());
		}

		for (InternalResultPartition rp : internalResultPartitions) {
			rp.release();
		}
		for (SingleInputGate ig : inputGates) {
			ig.releaseAllResources();
		}
		network.shutdown();
	}

	/**
	 * Helper to create simple {@link InternalResultPartition} instance for use by a {@link Task} inside
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
	private static InternalResultPartition createResultPartition(
			final ResultPartitionType partitionType, final int channels) {
		return new InternalResultPartition(
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
	 * Helper to create spy of a {@link SingleInputGate} for use by a {@link Task} inside
	 * {@link NetworkEnvironment#registerTask(Task)}.
	 *
	 * @param partitionType
	 * 		the consumed partition type
	 * @param channels
	 * 		the number of input channels
	 *
	 * @return input gate with some fake settings
	 */
	private SingleInputGate createSingleInputGate(
			final ResultPartitionType partitionType, final int channels) {
		return spy(new SingleInputGate(
			"Test Task Name",
			new JobID(),
			new IntermediateDataSetID(),
			partitionType,
			0,
			channels,
			mock(TaskActions.class),
			UnregisteredMetricGroups.createUnregisteredTaskMetricGroup().getIOMetricGroup(),
			new PartitionRequestManager(Integer.MAX_VALUE, 1),
			Executors.newSingleThreadExecutor(),
			enableCreditBasedFlowControl,
			false));
	}

	private static void createRemoteInputChannel(
			SingleInputGate inputGate,
			int channelIndex,
			InternalResultPartition internalResultPartition,
			ConnectionManager connManager) {
		RemoteInputChannel channel = new RemoteInputChannel(
			inputGate,
			channelIndex,
			internalResultPartition.getPartitionId(),
			mock(ConnectionID.class),
			connManager,
			0,
			0,
			UnregisteredMetricGroups.createUnregisteredTaskMetricGroup().getIOMetricGroup());
		inputGate.setInputChannel(internalResultPartition.getPartitionId().getPartitionId(), channel);
	}
}
