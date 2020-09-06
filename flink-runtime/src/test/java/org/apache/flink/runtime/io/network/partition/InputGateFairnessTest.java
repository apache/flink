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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NoOpBufferPool;
import org.apache.flink.runtime.io.network.partition.InputChannelTestUtils.UnpooledMemorySegmentProvider;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelBuilder;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGateBuilder;
import org.apache.flink.runtime.io.network.util.TestBufferFactory;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.util.function.SupplierWithException;

import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;

import static org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils.createFilledFinishedBufferConsumer;
import static org.apache.flink.runtime.io.network.partition.InputChannelTestUtils.createDummyConnectionManager;
import static org.apache.flink.runtime.io.network.partition.InputChannelTestUtils.createLocalInputChannel;
import static org.apache.flink.runtime.io.network.partition.InputChannelTestUtils.createResultPartitionManager;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

/**
 * Tests verifying fairness in input gates.
 */
public class InputGateFairnessTest {

	@Test
	public void testFairConsumptionLocalChannelsPreFilled() throws Exception {
		final int numberOfChannels = 37;
		final int buffersPerChannel = 27;

		final ResultPartition resultPartition = mock(ResultPartition.class);
		final BufferConsumer bufferConsumer = createFilledFinishedBufferConsumer(42);

		// ----- create some source channels and fill them with buffers -----

		final PipelinedSubpartition[] sources = new PipelinedSubpartition[numberOfChannels];

		for (int i = 0; i < numberOfChannels; i++) {
			PipelinedSubpartition partition = new PipelinedSubpartition(0, resultPartition);

			for (int p = 0; p < buffersPerChannel; p++) {
				partition.add(bufferConsumer.copy());
			}

			partition.finish();
			sources[i] = partition;
		}

		// ----- create reading side -----

		ResultPartitionManager resultPartitionManager = createResultPartitionManager(sources);

		final SingleInputGate gate = createFairnessVerifyingInputGate(numberOfChannels);
		final InputChannel[] inputChannels = new InputChannel[numberOfChannels];

		for (int i = 0; i < numberOfChannels; i++) {
			inputChannels[i] = createLocalInputChannel(gate, i, resultPartitionManager);
		}

		setupInputGate(gate, inputChannels);

		// read all the buffers and the EOF event
		for (int i = numberOfChannels * (buffersPerChannel + 1); i > 0; --i) {
			assertNotNull(gate.getNext());

			int min = Integer.MAX_VALUE;
			int max = 0;

			for (PipelinedSubpartition source : sources) {
				int size = source.getCurrentNumberOfBuffers();
				min = Math.min(min, size);
				max = Math.max(max, size);
			}

			assertTrue(max == min || max == (min + 1));
		}

		assertFalse(gate.getNext().isPresent());
	}

	@Test
	public void testFairConsumptionLocalChannels() throws Exception {
		final int numberOfChannels = 37;
		final int buffersPerChannel = 27;

		final ResultPartition resultPartition = mock(ResultPartition.class);
		try (BufferConsumer bufferConsumer = createFilledFinishedBufferConsumer(42)) {

			// ----- create some source channels and fill them with one buffer each -----

			final PipelinedSubpartition[] sources = new PipelinedSubpartition[numberOfChannels];

			for (int i = 0; i < numberOfChannels; i++) {
				sources[i] = new PipelinedSubpartition(0, resultPartition);
			}

			// ----- create reading side -----

			ResultPartitionManager resultPartitionManager = createResultPartitionManager(sources);

			final SingleInputGate gate = createFairnessVerifyingInputGate(numberOfChannels);
			final InputChannel[] inputChannels = new InputChannel[numberOfChannels];

			for (int i = 0; i < numberOfChannels; i++) {
				inputChannels[i] = createLocalInputChannel(gate, i, resultPartitionManager);
			}

			// seed one initial buffer
			sources[12].add(bufferConsumer.copy());

			setupInputGate(gate, inputChannels);

			// read all the buffers and the EOF event
			for (int i = 0; i < numberOfChannels * buffersPerChannel; i++) {
				assertNotNull(gate.getNext());

				int min = Integer.MAX_VALUE;
				int max = 0;

				for (PipelinedSubpartition source : sources) {
					int size = source.getCurrentNumberOfBuffers();
					min = Math.min(min, size);
					max = Math.max(max, size);
				}

				assertTrue(max == min || max == min + 1);

				if (i % (2 * numberOfChannels) == 0) {
					// add three buffers to each channel, in random order
					fillRandom(sources, 3, bufferConsumer);
				}
			}
			// there is still more in the queues
		}
	}

	@Test
	public void testFairConsumptionRemoteChannelsPreFilled() throws Exception {
		final int numberOfChannels = 37;
		final int buffersPerChannel = 27;

		final Buffer mockBuffer = TestBufferFactory.createBuffer(42);

		// ----- create some source channels and fill them with buffers -----

		final SingleInputGate gate = createFairnessVerifyingInputGate(numberOfChannels);

		final ConnectionManager connManager = createDummyConnectionManager();

		final RemoteInputChannel[] channels = new RemoteInputChannel[numberOfChannels];

		for (int i = 0; i < numberOfChannels; i++) {
			RemoteInputChannel channel = createRemoteInputChannel(gate, i, connManager);
			channels[i] = channel;

			for (int p = 0; p < buffersPerChannel; p++) {
				channel.onBuffer(mockBuffer, p, -1);
			}
			channel.onBuffer(EventSerializer.toBuffer(EndOfPartitionEvent.INSTANCE), buffersPerChannel, -1);
		}

		gate.setInputChannels(channels);
		gate.setup();
		gate.requestPartitions();

		// read all the buffers and the EOF event
		for (int i = numberOfChannels * (buffersPerChannel + 1); i > 0; --i) {
			assertNotNull(gate.getNext());

			int min = Integer.MAX_VALUE;
			int max = 0;

			for (RemoteInputChannel channel : channels) {
				int size = channel.getNumberOfQueuedBuffers();
				min = Math.min(min, size);
				max = Math.max(max, size);
			}

			assertTrue(max == min || max == (min + 1));
		}

		assertFalse(gate.getNext().isPresent());
	}

	@Test
	public void testFairConsumptionRemoteChannels() throws Exception {
		final int numberOfChannels = 37;
		final int buffersPerChannel = 27;

		final Buffer mockBuffer = TestBufferFactory.createBuffer(42);

		// ----- create some source channels and fill them with buffers -----

		final SingleInputGate gate = createFairnessVerifyingInputGate(numberOfChannels);

		final ConnectionManager connManager = createDummyConnectionManager();

		final RemoteInputChannel[] channels = new RemoteInputChannel[numberOfChannels];
		final int[] channelSequenceNums = new int[numberOfChannels];

		for (int i = 0; i < numberOfChannels; i++) {
			RemoteInputChannel channel = createRemoteInputChannel(gate, i, connManager);
			channels[i] = channel;
		}

		channels[11].onBuffer(mockBuffer, 0, -1);
		channelSequenceNums[11]++;

		setupInputGate(gate, channels);

		// read all the buffers and the EOF event
		for (int i = 0; i < numberOfChannels * buffersPerChannel; i++) {
			assertNotNull(gate.getNext());

			int min = Integer.MAX_VALUE;
			int max = 0;

			for (RemoteInputChannel channel : channels) {
				int size = channel.getNumberOfQueuedBuffers();
				min = Math.min(min, size);
				max = Math.max(max, size);
			}

			assertTrue(max == min || max == (min + 1));

			if (i % (2 * numberOfChannels) == 0) {
				// add three buffers to each channel, in random order
				fillRandom(channels, channelSequenceNums, 3, mockBuffer);
			}
		}
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	private SingleInputGate createFairnessVerifyingInputGate(int numberOfChannels) {
		return new FairnessVerifyingInputGate(
			"Test Task Name",
			new IntermediateDataSetID(),
			0,
			numberOfChannels);
	}

	private void fillRandom(PipelinedSubpartition[] partitions, int numPerPartition, BufferConsumer buffer) throws Exception {
		ArrayList<Integer> poss = new ArrayList<>(partitions.length * numPerPartition);

		for (int i = 0; i < partitions.length; i++) {
			for (int k = 0; k < numPerPartition; k++) {
				poss.add(i);
			}
		}

		Collections.shuffle(poss);

		for (Integer i : poss) {
			partitions[i].add(buffer.copy());
		}
	}

	private void fillRandom(
			RemoteInputChannel[] partitions,
			int[] sequenceNumbers,
			int numPerPartition,
			Buffer buffer) throws Exception {

		ArrayList<Integer> poss = new ArrayList<>(partitions.length * numPerPartition);

		for (int i = 0; i < partitions.length; i++) {
			for (int k = 0; k < numPerPartition; k++) {
				poss.add(i);
			}
		}

		Collections.shuffle(poss);

		for (int i : poss) {
			partitions[i].onBuffer(buffer, sequenceNumbers[i]++, -1);
		}
	}

	// ------------------------------------------------------------------------

	private static class FairnessVerifyingInputGate extends SingleInputGate {
		private static final SupplierWithException<BufferPool, IOException> STUB_BUFFER_POOL_FACTORY =
			NoOpBufferPool::new;

		private final ArrayDeque<InputChannel> channelsWithData;

		private final HashSet<InputChannel> uniquenessChecker;

		@SuppressWarnings("unchecked")
		public FairnessVerifyingInputGate(
				String owningTaskName,
				IntermediateDataSetID consumedResultId,
				int consumedSubpartitionIndex,
				int numberOfInputChannels) {

			super(
				owningTaskName,
				0,
				consumedResultId,
				ResultPartitionType.PIPELINED,
				consumedSubpartitionIndex,
				numberOfInputChannels,
				SingleInputGateBuilder.NO_OP_PRODUCER_CHECKER,
				STUB_BUFFER_POOL_FACTORY,
				null,
				new UnpooledMemorySegmentProvider(32 * 1024));

			try {
				Field f = SingleInputGate.class.getDeclaredField("inputChannelsWithData");
				f.setAccessible(true);
				channelsWithData = (ArrayDeque<InputChannel>) f.get(this);
			}
			catch (Exception e) {
				throw new RuntimeException(e);
			}

			this.uniquenessChecker = new HashSet<>();
		}

		@Override
		public Optional<BufferOrEvent> getNext() throws IOException, InterruptedException {
			synchronized (channelsWithData) {
				assertTrue("too many input channels", channelsWithData.size() <= getNumberOfInputChannels());
				ensureUnique(channelsWithData);
			}

			return super.getNext();
		}

		private void ensureUnique(Collection<InputChannel> channels) {
			HashSet<InputChannel> uniquenessChecker = this.uniquenessChecker;

			for (InputChannel channel : channels) {
				if (!uniquenessChecker.add(channel)) {
					fail("Duplicate channel in input gate: " + channel);
				}
			}

			assertTrue("found duplicate input channels", uniquenessChecker.size() == channels.size());
			uniquenessChecker.clear();
		}
	}

	public static RemoteInputChannel createRemoteInputChannel(
		SingleInputGate inputGate,
		int channelIndex,
		ConnectionManager connectionManager) {

		return InputChannelBuilder.newBuilder()
			.setChannelIndex(channelIndex)
			.setConnectionManager(connectionManager)
			.buildRemoteChannel(inputGate);
	}

	public static void setupInputGate(SingleInputGate gate, InputChannel... channels) throws IOException {
		gate.setInputChannels(channels);
		gate.setup();
		gate.requestPartitions();
	}
}
