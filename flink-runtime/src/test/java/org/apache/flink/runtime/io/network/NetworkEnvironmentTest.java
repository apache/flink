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

import org.apache.flink.configuration.NetworkEnvironmentOptions;
import org.apache.flink.core.memory.MemorySegmentProvider;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelBuilder;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGateBuilder;
import org.apache.flink.runtime.taskmanager.Task;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.runtime.io.network.partition.InputChannelTestUtils.createDummyConnectionManager;
import static org.apache.flink.runtime.io.network.partition.PartitionTestUtils.createPartition;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.spy;

/**
 * Various tests for the {@link NetworkEnvironment} class.
 */
@RunWith(Parameterized.class)
public class NetworkEnvironmentTest {

	@Parameterized.Parameter
	public boolean enableCreditBasedFlowControl;

	@Parameterized.Parameters(name = "Credit-based = {0}")
	public static List<Boolean> parameters() {
		return Arrays.asList(Boolean.TRUE, Boolean.FALSE);
	}

	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	/**
	 * Verifies that {@link Task#setupPartitionsAndGates(ResultPartitionWriter[], InputGate[])}} sets up (un)bounded buffer pool
	 * instances for various types of input and output channels.
	 */
	@Test
	public void testRegisterTaskUsesBoundedBuffers() throws Exception {
		final NetworkEnvironment network = new NetworkEnvironmentBuilder()
			.setIsCreditBased(enableCreditBasedFlowControl)
			.build();

		// result partitions
		ResultPartition rp1 = createPartition(network, ResultPartitionType.PIPELINED, 2);
		ResultPartition rp2 = createPartition(network, ResultPartitionType.BLOCKING, 2);
		ResultPartition rp3 = createPartition(network, ResultPartitionType.PIPELINED_BOUNDED, 2);
		ResultPartition rp4 = createPartition(network, ResultPartitionType.PIPELINED_BOUNDED, 8);
		final ResultPartition[] resultPartitions = new ResultPartition[] {rp1, rp2, rp3, rp4};

		// input gates
		SingleInputGate ig1 = createSingleInputGate(network, ResultPartitionType.PIPELINED, 2);
		SingleInputGate ig2 = createSingleInputGate(network, ResultPartitionType.BLOCKING, 2);
		SingleInputGate ig3 = createSingleInputGate(network, ResultPartitionType.PIPELINED_BOUNDED, 2);
		SingleInputGate ig4 = createSingleInputGate(network, ResultPartitionType.PIPELINED_BOUNDED, 8);
		final SingleInputGate[] inputGates = new SingleInputGate[] {ig1, ig2, ig3, ig4};

		Task.setupPartitionsAndGates(resultPartitions, inputGates);

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
		verify(ig1, times(invokations)).assignExclusiveSegments();
		verify(ig2, times(invokations)).assignExclusiveSegments();
		verify(ig3, times(invokations)).assignExclusiveSegments();
		verify(ig4, times(invokations)).assignExclusiveSegments();

		for (ResultPartition rp : resultPartitions) {
			rp.release();
		}
		for (SingleInputGate ig : inputGates) {
			ig.close();
		}
		network.close();
	}

	/**
	 * Verifies that {@link Task#setupPartitionsAndGates(ResultPartitionWriter[], InputGate[])}} sets up (un)bounded buffer pool
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
			bufferCount = 10 + 10 * NetworkEnvironmentOptions.NETWORK_BUFFERS_PER_CHANNEL.defaultValue();
		}

		testRegisterTaskWithLimitedBuffers(bufferCount);
	}

	/**
	 * Verifies that {@link Task#setupPartitionsAndGates(ResultPartitionWriter[], InputGate[])}} fails if the bare minimum of
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
			bufferCount = 10 + 10 * NetworkEnvironmentOptions.NETWORK_BUFFERS_PER_CHANNEL.defaultValue() - 1;
		}

		expectedException.expect(IOException.class);
		expectedException.expectMessage("Insufficient number of network buffers");
		testRegisterTaskWithLimitedBuffers(bufferCount);
	}

	private void testRegisterTaskWithLimitedBuffers(int bufferPoolSize) throws Exception {
		final NetworkEnvironment network = new NetworkEnvironmentBuilder()
			.setNumNetworkBuffers(bufferPoolSize)
			.setIsCreditBased(enableCreditBasedFlowControl)
			.build();

		final ConnectionManager connManager = createDummyConnectionManager();

		// result partitions
		ResultPartition rp1 = createPartition(network, ResultPartitionType.PIPELINED, 2);
		ResultPartition rp2 = createPartition(network, ResultPartitionType.BLOCKING, 2);
		ResultPartition rp3 = createPartition(network, ResultPartitionType.PIPELINED_BOUNDED, 2);
		ResultPartition rp4 = createPartition(network, ResultPartitionType.PIPELINED_BOUNDED, 4);
		final ResultPartition[] resultPartitions = new ResultPartition[] {rp1, rp2, rp3, rp4};

		// input gates
		SingleInputGate ig1 = createSingleInputGate(network, ResultPartitionType.PIPELINED, 2);
		SingleInputGate ig2 = createSingleInputGate(network, ResultPartitionType.BLOCKING, 2);
		SingleInputGate ig3 = createSingleInputGate(network, ResultPartitionType.PIPELINED_BOUNDED, 2);
		SingleInputGate ig4 = createSingleInputGate(network, ResultPartitionType.PIPELINED_BOUNDED, 4);
		final SingleInputGate[] inputGates = new SingleInputGate[] {ig1, ig2, ig3, ig4};

		// set up remote input channels for the exclusive buffers of the credit-based flow control
		// (note that this does not obey the partition types which is ok for the scope of the test)
		if (enableCreditBasedFlowControl) {
			createRemoteInputChannel(ig4, 0, rp1, connManager, network.getNetworkBufferPool());
			createRemoteInputChannel(ig4, 0, rp2, connManager, network.getNetworkBufferPool());
			createRemoteInputChannel(ig4, 0, rp3, connManager, network.getNetworkBufferPool());
			createRemoteInputChannel(ig4, 0, rp4, connManager, network.getNetworkBufferPool());

			createRemoteInputChannel(ig1, 1, rp1, connManager, network.getNetworkBufferPool());
			createRemoteInputChannel(ig1, 1, rp4, connManager, network.getNetworkBufferPool());

			createRemoteInputChannel(ig2, 1, rp2, connManager, network.getNetworkBufferPool());
			createRemoteInputChannel(ig2, 2, rp4, connManager, network.getNetworkBufferPool());

			createRemoteInputChannel(ig3, 1, rp3, connManager, network.getNetworkBufferPool());
			createRemoteInputChannel(ig3, 3, rp4, connManager, network.getNetworkBufferPool());
		}

		Task.setupPartitionsAndGates(resultPartitions, inputGates);

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
		verify(ig1, times(invokations)).assignExclusiveSegments();
		verify(ig2, times(invokations)).assignExclusiveSegments();
		verify(ig3, times(invokations)).assignExclusiveSegments();
		verify(ig4, times(invokations)).assignExclusiveSegments();

		for (ResultPartition rp : resultPartitions) {
			rp.release();
		}
		for (SingleInputGate ig : inputGates) {
			ig.close();
		}
		network.close();
	}

	/**
	 * Helper to create spy of a {@link SingleInputGate} for use by a {@link Task} inside
	 * {@link Task#setupPartitionsAndGates(ResultPartitionWriter[], InputGate[])}}.
	 *
	 * @param network
	 * 	    network environment to create buffer pool factory for {@link SingleInputGate}
	 * @param partitionType
	 * 		the consumed partition type
	 * @param numberOfChannels
	 * 		the number of input channels
	 *
	 * @return input gate with some fake settings
	 */
	private SingleInputGate createSingleInputGate(
		NetworkEnvironment network, ResultPartitionType partitionType, int numberOfChannels) {

		return spy(new SingleInputGateBuilder()
			.setNumberOfChannels(numberOfChannels)
			.setResultPartitionType(partitionType)
			.setIsCreditBased(enableCreditBasedFlowControl)
			.setupBufferPoolFactory(network)
			.build());
	}

	private static void createRemoteInputChannel(
			SingleInputGate inputGate,
			int channelIndex,
			ResultPartition resultPartition,
			ConnectionManager connManager,
			MemorySegmentProvider memorySegmentProvider) {
		InputChannelBuilder.newBuilder()
			.setChannelIndex(channelIndex)
			.setPartitionId(resultPartition.getPartitionId())
			.setConnectionManager(connManager)
			.setMemorySegmentProvider(memorySegmentProvider)
			.buildRemoteAndSetToGate(inputGate);
	}
}
