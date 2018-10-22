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
import org.apache.flink.runtime.io.network.partition.NoOpResultPartitionConsumableNotifier;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
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

import static org.apache.flink.runtime.io.network.partition.InputChannelTestUtils.createDummyConnectionManager;
import static org.junit.Assert.assertEquals;
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
		final NetworkEnvironment network = new NetworkEnvironment(
			numBuffers, memorySegmentSize, 0, 0, 2, 8, enableCreditBasedFlowControl);

		// result partitions
		ResultPartition rp1 = createResultPartition(ResultPartitionType.PIPELINED, 2);
		ResultPartition rp2 = createResultPartition(ResultPartitionType.BLOCKING, 2);
		ResultPartition rp3 = createResultPartition(ResultPartitionType.PIPELINED_BOUNDED, 2);
		ResultPartition rp4 = createResultPartition(ResultPartitionType.PIPELINED_BOUNDED, 8);
		final ResultPartition[] resultPartitions = new ResultPartition[] {rp1, rp2, rp3, rp4};

		// input gates
		SingleInputGate ig1 = createSingleInputGate(ResultPartitionType.PIPELINED, 2);
		SingleInputGate ig2 = createSingleInputGate(ResultPartitionType.BLOCKING, 2);
		SingleInputGate ig3 = createSingleInputGate(ResultPartitionType.PIPELINED_BOUNDED, 2);
		SingleInputGate ig4 = createSingleInputGate(ResultPartitionType.PIPELINED_BOUNDED, 8);
		final SingleInputGate[] inputGates = new SingleInputGate[] {ig1, ig2, ig3, ig4};

		// overall task to register
		Task task = mock(Task.class);
		when(task.getProducedPartitions()).thenReturn(resultPartitions);
		when(task.getAllInputGates()).thenReturn(inputGates);

		network.registerTask(task);

		// verify buffer pools for the result partitions
		assertEquals(rp1.getNumberOfSubpartitions(), rp1.getBufferPool().getNumberOfRequiredMemorySegments());
		assertEquals(rp2.getNumberOfSubpartitions(), rp2.getBufferPool().getNumberOfRequiredMemorySegments());
		assertEquals(rp3.getNumberOfSubpartitions(), rp3.getBufferPool().getNumberOfRequiredMemorySegments());
		assertEquals(rp4.getNumberOfSubpartitions(), rp4.getBufferPool().getNumberOfRequiredMemorySegments());

		assertEquals(Integer.MAX_VALUE, rp1.getBufferPool().getMaxNumberOfMemorySegments());
		assertEquals(Integer.MAX_VALUE, rp2.getBufferPool().getMaxNumberOfMemorySegments());
		assertEquals(2 * 2 + 8, rp3.getBufferPool().getMaxNumberOfMemorySegments());
		assertEquals(8 * 2 + 8, rp4.getBufferPool().getMaxNumberOfMemorySegments());

		// verify buffer pools for the input gates (NOTE: credit-based uses minimum required buffers
		// for exclusive buffers not managed by the buffer pool)
		assertEquals(enableCreditBasedFlowControl ? 0 : 2, ig1.getBufferPool().getNumberOfRequiredMemorySegments());
		assertEquals(enableCreditBasedFlowControl ? 0 : 2, ig2.getBufferPool().getNumberOfRequiredMemorySegments());
		assertEquals(enableCreditBasedFlowControl ? 0 : 2, ig3.getBufferPool().getNumberOfRequiredMemorySegments());
		assertEquals(enableCreditBasedFlowControl ? 0 : 8, ig4.getBufferPool().getNumberOfRequiredMemorySegments());

		assertEquals(Integer.MAX_VALUE, ig1.getBufferPool().getMaxNumberOfMemorySegments());
		assertEquals(Integer.MAX_VALUE, ig2.getBufferPool().getMaxNumberOfMemorySegments());
		assertEquals(enableCreditBasedFlowControl ? 8 : 2 * 2 + 8, ig3.getBufferPool().getMaxNumberOfMemorySegments());
		assertEquals(enableCreditBasedFlowControl ? 8 : 8 * 2 + 8, ig4.getBufferPool().getMaxNumberOfMemorySegments());

		int invokations = enableCreditBasedFlowControl ? 1 : 0;
		verify(ig1, times(invokations)).assignExclusiveSegments(network.getNetworkBufferPool(), 2);
		verify(ig2, times(invokations)).assignExclusiveSegments(network.getNetworkBufferPool(), 2);
		verify(ig3, times(invokations)).assignExclusiveSegments(network.getNetworkBufferPool(), 2);
		verify(ig4, times(invokations)).assignExclusiveSegments(network.getNetworkBufferPool(), 2);

		for (ResultPartition rp : resultPartitions) {
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
			bufferCount = 20;
		} else {
			// incoming: 2 exclusive buffers per channel
			bufferCount = 10 + 10 * TaskManagerOptions.NETWORK_BUFFERS_PER_CHANNEL.defaultValue();
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
		final NetworkEnvironment network = new NetworkEnvironment(
			bufferPoolSize, memorySegmentSize, 0, 0, 2, 8, enableCreditBasedFlowControl);

		final ConnectionManager connManager = createDummyConnectionManager();

		// result partitions
		ResultPartition rp1 = createResultPartition(ResultPartitionType.PIPELINED, 2);
		ResultPartition rp2 = createResultPartition(ResultPartitionType.BLOCKING, 2);
		ResultPartition rp3 = createResultPartition(ResultPartitionType.PIPELINED_BOUNDED, 2);
		ResultPartition rp4 = createResultPartition(ResultPartitionType.PIPELINED_BOUNDED, 4);
		final ResultPartition[] resultPartitions = new ResultPartition[] {rp1, rp2, rp3, rp4};

		// input gates
		SingleInputGate ig1 = createSingleInputGate(ResultPartitionType.PIPELINED, 2);
		SingleInputGate ig2 = createSingleInputGate(ResultPartitionType.BLOCKING, 2);
		SingleInputGate ig3 = createSingleInputGate(ResultPartitionType.PIPELINED_BOUNDED, 2);
		SingleInputGate ig4 = createSingleInputGate(ResultPartitionType.PIPELINED_BOUNDED, 4);
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
		when(task.getProducedPartitions()).thenReturn(resultPartitions);
		when(task.getAllInputGates()).thenReturn(inputGates);

		network.registerTask(task);

		// verify buffer pools for the result partitions
		assertEquals(Integer.MAX_VALUE, rp1.getBufferPool().getMaxNumberOfMemorySegments());
		assertEquals(Integer.MAX_VALUE, rp2.getBufferPool().getMaxNumberOfMemorySegments());
		assertEquals(2 * 2 + 8, rp3.getBufferPool().getMaxNumberOfMemorySegments());
		assertEquals(4 * 2 + 8, rp4.getBufferPool().getMaxNumberOfMemorySegments());

		for (ResultPartition rp : resultPartitions) {
			assertEquals(rp.getNumberOfSubpartitions(), rp.getBufferPool().getNumberOfRequiredMemorySegments());
			assertEquals(rp.getNumberOfSubpartitions(), rp.getBufferPool().getNumBuffers());
		}

		// verify buffer pools for the input gates (NOTE: credit-based uses minimum required buffers
		// for exclusive buffers not managed by the buffer pool)
		assertEquals(enableCreditBasedFlowControl ? 0 : 2, ig1.getBufferPool().getNumberOfRequiredMemorySegments());
		assertEquals(enableCreditBasedFlowControl ? 0 : 2, ig2.getBufferPool().getNumberOfRequiredMemorySegments());
		assertEquals(enableCreditBasedFlowControl ? 0 : 2, ig3.getBufferPool().getNumberOfRequiredMemorySegments());
		assertEquals(enableCreditBasedFlowControl ? 0 : 4, ig4.getBufferPool().getNumberOfRequiredMemorySegments());

		assertEquals(Integer.MAX_VALUE, ig1.getBufferPool().getMaxNumberOfMemorySegments());
		assertEquals(Integer.MAX_VALUE, ig2.getBufferPool().getMaxNumberOfMemorySegments());
		assertEquals(enableCreditBasedFlowControl ? 8 : 2 * 2 + 8, ig3.getBufferPool().getMaxNumberOfMemorySegments());
		assertEquals(enableCreditBasedFlowControl ? 8 : 4 * 2 + 8, ig4.getBufferPool().getMaxNumberOfMemorySegments());

		int invokations = enableCreditBasedFlowControl ? 1 : 0;
		verify(ig1, times(invokations)).assignExclusiveSegments(network.getNetworkBufferPool(), 2);
		verify(ig2, times(invokations)).assignExclusiveSegments(network.getNetworkBufferPool(), 2);
		verify(ig3, times(invokations)).assignExclusiveSegments(network.getNetworkBufferPool(), 2);
		verify(ig4, times(invokations)).assignExclusiveSegments(network.getNetworkBufferPool(), 2);

		for (ResultPartition rp : resultPartitions) {
			rp.release();
		}
		for (SingleInputGate ig : inputGates) {
			ig.releaseAllResources();
		}
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
			new NoOpResultPartitionConsumableNotifier(),
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
			enableCreditBasedFlowControl));
	}

	private static void createRemoteInputChannel(
			SingleInputGate inputGate,
			int channelIndex,
			ResultPartition resultPartition,
			ConnectionManager connManager) {
		RemoteInputChannel channel = new RemoteInputChannel(
			inputGate,
			channelIndex,
			resultPartition.getPartitionId(),
			mock(ConnectionID.class),
			connManager,
			0,
			0,
			UnregisteredMetricGroups.createUnregisteredTaskMetricGroup().getIOMetricGroup());
		inputGate.setInputChannel(resultPartition.getPartitionId().getPartitionId(), channel);
	}
}
