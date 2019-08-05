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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironmentBuilder;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.TestingConnectionManager;
import org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.InputChannelTestUtils;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionBuilder;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.util.TestBufferFactory;
import org.apache.flink.runtime.io.network.util.TestTaskEvent;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.UnknownShuffleDescriptor;

import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.runtime.io.network.partition.InputChannelTestUtils.createLocalInputChannel;
import static org.apache.flink.runtime.io.network.partition.InputChannelTestUtils.createSingleInputGate;
import static org.apache.flink.runtime.util.NettyShuffleDescriptorBuilder.createRemoteWithIdAndLocation;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link SingleInputGate}.
 */
public class SingleInputGateTest extends InputGateTestBase {
	/**
	 * Tests basic correctness of buffer-or-event interleaving and correct <code>null</code> return
	 * value after receiving all end-of-partition events.
	 */
	@Test
	public void testBasicGetNextLogic() throws Exception {
		// Setup
		final SingleInputGate inputGate = createInputGate();

		final TestInputChannel[] inputChannels = new TestInputChannel[]{
			new TestInputChannel(inputGate, 0),
			new TestInputChannel(inputGate, 1)
		};

		inputGate.setInputChannel(
			new IntermediateResultPartitionID(), inputChannels[0]);

		inputGate.setInputChannel(
			new IntermediateResultPartitionID(), inputChannels[1]);

		// Test
		inputChannels[0].readBuffer();
		inputChannels[0].readBuffer();
		inputChannels[1].readBuffer();
		inputChannels[1].readEndOfPartitionEvent();
		inputChannels[0].readEndOfPartitionEvent();

		inputGate.notifyChannelNonEmpty(inputChannels[0]);
		inputGate.notifyChannelNonEmpty(inputChannels[1]);

		verifyBufferOrEvent(inputGate, true, 0, true);
		verifyBufferOrEvent(inputGate, true, 1, true);
		verifyBufferOrEvent(inputGate, true, 0, true);
		verifyBufferOrEvent(inputGate, false, 1, true);
		verifyBufferOrEvent(inputGate, false, 0, false);

		// Return null when the input gate has received all end-of-partition events
		assertTrue(inputGate.isFinished());

		for (TestInputChannel ic : inputChannels) {
			ic.assertReturnedEventsAreRecycled();
		}
	}

	@Test
	public void testIsAvailable() throws Exception {
		final SingleInputGate inputGate = createInputGate(1);
		TestInputChannel inputChannel = new TestInputChannel(inputGate, 0);
		inputGate.setInputChannel(new IntermediateResultPartitionID(), inputChannel);

		testIsAvailable(inputGate, inputGate, inputChannel);
	}

	@Test
	public void testIsAvailableAfterFinished() throws Exception {
		final SingleInputGate inputGate = createInputGate(1);
		TestInputChannel inputChannel = new TestInputChannel(inputGate, 0);
		inputGate.setInputChannel(new IntermediateResultPartitionID(), inputChannel);

		testIsAvailableAfterFinished(
			inputGate,
			() -> {
				inputChannel.readEndOfPartitionEvent();
				inputGate.notifyChannelNonEmpty(inputChannel);
			});
	}

	@Test
	public void testIsMoreAvailableReadingFromSingleInputChannel() throws Exception {
		// Setup
		final SingleInputGate inputGate = createInputGate();

		final TestInputChannel[] inputChannels = new TestInputChannel[]{
			new TestInputChannel(inputGate, 0),
			new TestInputChannel(inputGate, 1)
		};

		inputGate.setInputChannel(
			new IntermediateResultPartitionID(), inputChannels[0]);

		inputGate.setInputChannel(
			new IntermediateResultPartitionID(), inputChannels[1]);

		// Test
		inputChannels[0].readBuffer();
		inputChannels[0].readBuffer(false);

		inputGate.notifyChannelNonEmpty(inputChannels[0]);

		verifyBufferOrEvent(inputGate, true, 0, true);
		verifyBufferOrEvent(inputGate, true, 0, false);
	}

	@Test
	public void testBackwardsEventWithUninitializedChannel() throws Exception {
		// Setup environment
		final TaskEventDispatcher taskEventDispatcher = mock(TaskEventDispatcher.class);
		when(taskEventDispatcher.publish(any(ResultPartitionID.class), any(TaskEvent.class))).thenReturn(true);

		final ResultSubpartitionView iterator = mock(ResultSubpartitionView.class);
		when(iterator.getNextBuffer()).thenReturn(
			new BufferAndBacklog(new NetworkBuffer(MemorySegmentFactory.allocateUnpooledSegment(1024), FreeingBufferRecycler.INSTANCE), false, 0, false));

		final ResultPartitionManager partitionManager = mock(ResultPartitionManager.class);
		when(partitionManager.createSubpartitionView(
			any(ResultPartitionID.class),
			anyInt(),
			any(BufferAvailabilityListener.class))).thenReturn(iterator);

		// Setup reader with one local and one unknown input channel

		NettyShuffleEnvironment environment = createNettyShuffleEnvironment();
		final SingleInputGate inputGate = createInputGate(environment, 2, ResultPartitionType.PIPELINED);
		try {
			// Local
			ResultPartitionID localPartitionId = new ResultPartitionID();

			InputChannelBuilder.newBuilder()
				.setPartitionId(localPartitionId)
				.setPartitionManager(partitionManager)
				.setTaskEventPublisher(taskEventDispatcher)
				.buildLocalAndSetToGate(inputGate);

			// Unknown
			ResultPartitionID unknownPartitionId = new ResultPartitionID();

			InputChannelBuilder.newBuilder()
				.setChannelIndex(1)
				.setPartitionId(unknownPartitionId)
				.setPartitionManager(partitionManager)
				.setTaskEventPublisher(taskEventDispatcher)
				.buildUnknownAndSetToGate(inputGate);

			inputGate.setup();

			// Only the local channel can request
			verify(partitionManager, times(1)).createSubpartitionView(any(ResultPartitionID.class), anyInt(), any(BufferAvailabilityListener.class));

			// Send event backwards and initialize unknown channel afterwards
			final TaskEvent event = new TestTaskEvent();
			inputGate.sendTaskEvent(event);

			// Only the local channel can send out the event
			verify(taskEventDispatcher, times(1)).publish(any(ResultPartitionID.class), any(TaskEvent.class));

			// After the update, the pending event should be send to local channel

			ResourceID location = ResourceID.generate();
			inputGate.updateInputChannel(location, createRemoteWithIdAndLocation(unknownPartitionId.getPartitionId(), location));

			verify(partitionManager, times(2)).createSubpartitionView(any(ResultPartitionID.class), anyInt(), any(BufferAvailabilityListener.class));
			verify(taskEventDispatcher, times(2)).publish(any(ResultPartitionID.class), any(TaskEvent.class));
		}
		finally {
			inputGate.close();
			environment.close();
		}
	}

	/**
	 * Tests that an update channel does not trigger a partition request before the UDF has
	 * requested any partitions. Otherwise, this can lead to races when registering a listener at
	 * the gate (e.g. in UnionInputGate), which can result in missed buffer notifications at the
	 * listener.
	 */
	@Test
	public void testUpdateChannelBeforeRequest() throws Exception {
		SingleInputGate inputGate = createInputGate(1);

		ResultPartitionManager partitionManager = mock(ResultPartitionManager.class);

		InputChannel unknown = InputChannelBuilder.newBuilder()
			.setPartitionManager(partitionManager)
			.buildUnknownAndSetToGate(inputGate);

		// Update to a local channel and verify that no request is triggered
		ResultPartitionID resultPartitionID = unknown.getPartitionId();
		ResourceID location = ResourceID.generate();
		inputGate.updateInputChannel(location, createRemoteWithIdAndLocation(resultPartitionID.getPartitionId(), location));

		verify(partitionManager, never()).createSubpartitionView(
			any(ResultPartitionID.class), anyInt(), any(BufferAvailabilityListener.class));
	}

	/**
	 * Tests that the release of the input gate is noticed while polling the
	 * channels for available data.
	 */
	@Test
	public void testReleaseWhilePollingChannel() throws Exception {
		final AtomicReference<Exception> asyncException = new AtomicReference<>();

		// Setup the input gate with a single channel that does nothing
		final SingleInputGate inputGate = createInputGate(1);

		InputChannelBuilder.newBuilder().buildUnknownAndSetToGate(inputGate);

		// Start the consumer in a separate Thread
		Thread asyncConsumer = new Thread() {
			@Override
			public void run() {
				try {
					inputGate.getNext();
				} catch (Exception e) {
					asyncException.set(e);
				}
			}
		};
		asyncConsumer.start();

		// Wait for blocking queue poll call and release input gate
		boolean success = false;
		for (int i = 0; i < 50; i++) {
			if (asyncConsumer.isAlive()) {
				success = asyncConsumer.getState() == Thread.State.WAITING;
			}

			if (success) {
				break;
			} else {
				// Retry
				Thread.sleep(100);
			}
		}

		// Verify that async consumer is in blocking request
		assertTrue("Did not trigger blocking buffer request.", success);

		// Release the input gate
		inputGate.close();

		// Wait for Thread to finish and verify expected Exceptions. If the
		// input gate status is not properly checked during requests, this
		// call will never return.
		asyncConsumer.join();

		assertNotNull(asyncException.get());
		assertEquals(IllegalStateException.class, asyncException.get().getClass());
	}

	/**
	 * Tests request back off configuration is correctly forwarded to the channels.
	 */
	@Test
	public void testRequestBackoffConfiguration() throws Exception {
		IntermediateResultPartitionID[] partitionIds = new IntermediateResultPartitionID[] {
			new IntermediateResultPartitionID(),
			new IntermediateResultPartitionID(),
			new IntermediateResultPartitionID()
		};

		ResourceID localLocation = ResourceID.generate();
		ShuffleDescriptor[] channelDescs = new ShuffleDescriptor[]{
			// Local
			createRemoteWithIdAndLocation(partitionIds[0], localLocation),
			// Remote
			createRemoteWithIdAndLocation(partitionIds[1], ResourceID.generate()),
			// Unknown
			new UnknownShuffleDescriptor(new ResultPartitionID(partitionIds[2], new ExecutionAttemptID()))};

		InputGateDeploymentDescriptor gateDesc = new InputGateDeploymentDescriptor(
			new IntermediateDataSetID(),
			ResultPartitionType.PIPELINED,
			0,
			channelDescs);

		int initialBackoff = 137;
		int maxBackoff = 1001;

		final NettyShuffleEnvironment netEnv = new NettyShuffleEnvironmentBuilder()
			.setPartitionRequestInitialBackoff(initialBackoff)
			.setPartitionRequestMaxBackoff(maxBackoff)
			.setIsCreditBased(enableCreditBasedFlowControl)
			.build();

		SingleInputGate gate = new SingleInputGateFactory(
			localLocation,
			netEnv.getConfiguration(),
			netEnv.getConnectionManager(),
			netEnv.getResultPartitionManager(),
			new TaskEventDispatcher(),
			netEnv.getNetworkBufferPool())
			.create(
				"TestTask",
				gateDesc,
				SingleInputGateBuilder.NO_OP_PRODUCER_CHECKER,
				InputChannelTestUtils.newUnregisteredInputChannelMetrics());

		try {
			assertEquals(gateDesc.getConsumedPartitionType(), gate.getConsumedPartitionType());

			Map<IntermediateResultPartitionID, InputChannel> channelMap = gate.getInputChannels();

			assertEquals(3, channelMap.size());
			InputChannel localChannel = channelMap.get(partitionIds[0]);
			assertEquals(LocalInputChannel.class, localChannel.getClass());

			InputChannel remoteChannel = channelMap.get(partitionIds[1]);
			assertEquals(RemoteInputChannel.class, remoteChannel.getClass());

			InputChannel unknownChannel = channelMap.get(partitionIds[2]);
			assertEquals(UnknownInputChannel.class, unknownChannel.getClass());

			InputChannel[] channels =
				new InputChannel[] {localChannel, remoteChannel, unknownChannel};
			for (InputChannel ch : channels) {
				assertEquals(0, ch.getCurrentBackoff());

				assertTrue(ch.increaseBackoff());
				assertEquals(initialBackoff, ch.getCurrentBackoff());

				assertTrue(ch.increaseBackoff());
				assertEquals(initialBackoff * 2, ch.getCurrentBackoff());

				assertTrue(ch.increaseBackoff());
				assertEquals(initialBackoff * 2 * 2, ch.getCurrentBackoff());

				assertTrue(ch.increaseBackoff());
				assertEquals(maxBackoff, ch.getCurrentBackoff());

				assertFalse(ch.increaseBackoff());
			}
		} finally {
			gate.close();
			netEnv.close();
		}
	}

	/**
	 * Tests that input gate requests and assigns network buffers for remote input channel.
	 */
	@Test
	public void testRequestBuffersWithRemoteInputChannel() throws Exception {
		final NettyShuffleEnvironment network = createNettyShuffleEnvironment();
		final SingleInputGate inputGate = createInputGate(network, 1, ResultPartitionType.PIPELINED_BOUNDED);
		int buffersPerChannel = 2;
		int extraNetworkBuffersPerGate = 8;

		try {
			RemoteInputChannel remote =
				InputChannelBuilder.newBuilder()
					.setupFromNettyShuffleEnvironment(network)
					.setConnectionManager(new TestingConnectionManager())
					.buildRemoteAndSetToGate(inputGate);
			inputGate.setup();

			NetworkBufferPool bufferPool = network.getNetworkBufferPool();
			if (enableCreditBasedFlowControl) {
				// only the exclusive buffers should be assigned/available now
				assertEquals(buffersPerChannel, remote.getNumberOfAvailableBuffers());

				assertEquals(bufferPool.getTotalNumberOfMemorySegments() - buffersPerChannel,
					bufferPool.getNumberOfAvailableMemorySegments());
				// note: exclusive buffers are not handed out into LocalBufferPool and are thus not counted
				assertEquals(extraNetworkBuffersPerGate, bufferPool.countBuffers());
			} else {
				assertEquals(buffersPerChannel + extraNetworkBuffersPerGate, bufferPool.countBuffers());
			}
		} finally {
			inputGate.close();
			network.close();
		}
	}

	/**
	 * Tests that input gate requests and assigns network buffers when unknown input channel
	 * updates to remote input channel.
	 */
	@Test
	public void testRequestBuffersWithUnknownInputChannel() throws Exception {
		final NettyShuffleEnvironment network = createNettyShuffleEnvironment();
		final SingleInputGate inputGate = createInputGate(network, 1, ResultPartitionType.PIPELINED_BOUNDED);
		int buffersPerChannel = 2;
		int extraNetworkBuffersPerGate = 8;

		try {
			final ResultPartitionID resultPartitionId = new ResultPartitionID();
			addUnknownInputChannel(network, inputGate, resultPartitionId, 0);

			inputGate.setup();
			NetworkBufferPool bufferPool = network.getNetworkBufferPool();

			if (enableCreditBasedFlowControl) {
				assertEquals(bufferPool.getTotalNumberOfMemorySegments(),
					bufferPool.getNumberOfAvailableMemorySegments());
				// note: exclusive buffers are not handed out into LocalBufferPool and are thus not counted
				assertEquals(extraNetworkBuffersPerGate, bufferPool.countBuffers());
			} else {
				assertEquals(buffersPerChannel + extraNetworkBuffersPerGate, bufferPool.countBuffers());
			}

			// Trigger updates to remote input channel from unknown input channel
			inputGate.updateInputChannel(
				ResourceID.generate(),
				createRemoteWithIdAndLocation(resultPartitionId.getPartitionId(), ResourceID.generate()));

			if (enableCreditBasedFlowControl) {
				RemoteInputChannel remote = (RemoteInputChannel) inputGate.getInputChannels()
					.get(resultPartitionId.getPartitionId());
				// only the exclusive buffers should be assigned/available now
				assertEquals(buffersPerChannel, remote.getNumberOfAvailableBuffers());

				assertEquals(bufferPool.getTotalNumberOfMemorySegments() - buffersPerChannel,
					bufferPool.getNumberOfAvailableMemorySegments());
				// note: exclusive buffers are not handed out into LocalBufferPool and are thus not counted
				assertEquals(extraNetworkBuffersPerGate, bufferPool.countBuffers());
			} else {
				assertEquals(buffersPerChannel + extraNetworkBuffersPerGate, bufferPool.countBuffers());
			}
		} finally {
			inputGate.close();
			network.close();
		}
	}

	/**
	 * Tests that input gate can successfully convert unknown input channels into local and remote
	 * channels.
	 */
	@Test
	public void testUpdateUnknownInputChannel() throws Exception {
		final NettyShuffleEnvironment network = createNettyShuffleEnvironment();

		final ResultPartition localResultPartition = new ResultPartitionBuilder()
			.setResultPartitionManager(network.getResultPartitionManager())
			.setupBufferPoolFactoryFromNettyShuffleEnvironment(network)
			.build();

		final ResultPartition remoteResultPartition = new ResultPartitionBuilder()
			.setResultPartitionManager(network.getResultPartitionManager())
			.setupBufferPoolFactoryFromNettyShuffleEnvironment(network)
			.build();

		localResultPartition.setup();
		remoteResultPartition.setup();

		final SingleInputGate inputGate = createInputGate(network, 2, ResultPartitionType.PIPELINED);

		try {
			final ResultPartitionID localResultPartitionId = localResultPartition.getPartitionId();
			addUnknownInputChannel(network, inputGate, localResultPartitionId, 0);

			final ResultPartitionID remoteResultPartitionId = remoteResultPartition.getPartitionId();
			addUnknownInputChannel(network, inputGate, remoteResultPartitionId, 1);

			inputGate.setup();

			assertThat(inputGate.getInputChannels().get(remoteResultPartitionId.getPartitionId()),
				is(instanceOf((UnknownInputChannel.class))));
			assertThat(inputGate.getInputChannels().get(localResultPartitionId.getPartitionId()),
				is(instanceOf((UnknownInputChannel.class))));

			ResourceID localLocation = ResourceID.generate();

			// Trigger updates to remote input channel from unknown input channel
			inputGate.updateInputChannel(
				localLocation,
				createRemoteWithIdAndLocation(remoteResultPartitionId.getPartitionId(), ResourceID.generate()));

			assertThat(inputGate.getInputChannels().get(remoteResultPartitionId.getPartitionId()),
				is(instanceOf((RemoteInputChannel.class))));
			assertThat(inputGate.getInputChannels().get(localResultPartitionId.getPartitionId()),
				is(instanceOf((UnknownInputChannel.class))));

			// Trigger updates to local input channel from unknown input channel
			inputGate.updateInputChannel(
				localLocation,
				createRemoteWithIdAndLocation(localResultPartitionId.getPartitionId(), localLocation));

			assertThat(inputGate.getInputChannels().get(remoteResultPartitionId.getPartitionId()),
				is(instanceOf((RemoteInputChannel.class))));
			assertThat(inputGate.getInputChannels().get(localResultPartitionId.getPartitionId()),
				is(instanceOf((LocalInputChannel.class))));
		} finally {
			inputGate.close();
			network.close();
		}
	}

	@Test
	public void testQueuedBuffers() throws Exception {
		final NettyShuffleEnvironment network = createNettyShuffleEnvironment();

		final ResultPartition resultPartition = new ResultPartitionBuilder()
			.setResultPartitionManager(network.getResultPartitionManager())
			.setupBufferPoolFactoryFromNettyShuffleEnvironment(network)
			.build();

		final SingleInputGate inputGate = createInputGate(network, 2, ResultPartitionType.PIPELINED);

		final ResultPartitionID localResultPartitionId = resultPartition.getPartitionId();

		final RemoteInputChannel remoteInputChannel = InputChannelBuilder.newBuilder()
			.setChannelIndex(1)
			.setupFromNettyShuffleEnvironment(network)
			.setConnectionManager(new TestingConnectionManager())
			.buildRemoteAndSetToGate(inputGate);

		InputChannelBuilder.newBuilder()
			.setChannelIndex(0)
			.setPartitionId(localResultPartitionId)
			.setupFromNettyShuffleEnvironment(network)
			.setConnectionManager(new TestingConnectionManager())
			.buildLocalAndSetToGate(inputGate);

		try {
			resultPartition.setup();
			inputGate.setup();

			remoteInputChannel.onBuffer(TestBufferFactory.createBuffer(1), 0, 0);
			assertEquals(1, inputGate.getNumberOfQueuedBuffers());

			resultPartition.addBufferConsumer(BufferBuilderTestUtils.createFilledBufferConsumer(1), 0);
			assertEquals(2, inputGate.getNumberOfQueuedBuffers());
		} finally {
			resultPartition.release();
			inputGate.close();
			network.close();
		}
	}

	/**
	 * Tests that if the {@link PartitionNotFoundException} is set onto one {@link InputChannel},
	 * then it would be thrown directly via {@link SingleInputGate#getNextBufferOrEvent()}. So we
	 * could confirm the {@link SingleInputGate} would not swallow or transform the original exception.
	 */
	@Test
	public void testPartitionNotFoundExceptionWhileGetNextBuffer() throws Exception {
		final SingleInputGate inputGate = createSingleInputGate(1);
		final LocalInputChannel localChannel = createLocalInputChannel(inputGate, new ResultPartitionManager());
		final ResultPartitionID partitionId = localChannel.getPartitionId();

		inputGate.setInputChannel(partitionId.getPartitionId(), localChannel);
		localChannel.setError(new PartitionNotFoundException(partitionId));
		try {
			inputGate.getNext();

			fail("Should throw a PartitionNotFoundException.");
		} catch (PartitionNotFoundException notFound) {
			assertThat(partitionId, is(notFound.getPartitionId()));
		}
	}

	@Test
	public void testInputGateRemovalFromNettyShuffleEnvironment() throws Exception {
		NettyShuffleEnvironment network = createNettyShuffleEnvironment();

		try {
			int numberOfGates = 10;
			Map<InputGateID, SingleInputGate> createdInputGatesById =
				createInputGateWithLocalChannels(network, numberOfGates, 1);

			assertEquals(numberOfGates, createdInputGatesById.size());

			for (InputGateID id : createdInputGatesById.keySet()) {
				assertThat(network.getInputGate(id).isPresent(), is(true));
				createdInputGatesById.get(id).close();
				assertThat(network.getInputGate(id).isPresent(), is(false));
			}
		} finally {
			network.close();
		}
	}

	// ---------------------------------------------------------------------------------------------

	private static Map<InputGateID, SingleInputGate> createInputGateWithLocalChannels(
			NettyShuffleEnvironment network,
			int numberOfGates,
			@SuppressWarnings("SameParameterValue") int numberOfLocalChannels) {
		ShuffleDescriptor[] channelDescs = new NettyShuffleDescriptor[numberOfLocalChannels];
		for (int i = 0; i < numberOfLocalChannels; i++) {
			channelDescs[i] = createRemoteWithIdAndLocation(new IntermediateResultPartitionID(), ResourceID.generate());
		}

		InputGateDeploymentDescriptor[] gateDescs = new InputGateDeploymentDescriptor[numberOfGates];
		IntermediateDataSetID[] ids = new IntermediateDataSetID[numberOfGates];
		for (int i = 0; i < numberOfGates; i++) {
			ids[i] = new IntermediateDataSetID();
			gateDescs[i] = new InputGateDeploymentDescriptor(
				ids[i],
				ResultPartitionType.PIPELINED,
				0,
				channelDescs);
		}

		ExecutionAttemptID consumerID = new ExecutionAttemptID();
		SingleInputGate[] gates = network.createInputGates(
			network.createShuffleIOOwnerContext("", consumerID, new UnregisteredMetricsGroup()),
			SingleInputGateBuilder.NO_OP_PRODUCER_CHECKER,
			Arrays.asList(gateDescs)).toArray(new SingleInputGate[] {});
		Map<InputGateID, SingleInputGate> inputGatesById = new HashMap<>();
		for (int i = 0; i < numberOfGates; i++) {
			inputGatesById.put(new InputGateID(ids[i], consumerID), gates[i]);
		}

		return inputGatesById;
	}

	private void addUnknownInputChannel(
			NettyShuffleEnvironment network,
			SingleInputGate inputGate,
			ResultPartitionID partitionId,
			int channelIndex) {
		InputChannelBuilder.newBuilder()
			.setChannelIndex(channelIndex)
			.setPartitionId(partitionId)
			.setupFromNettyShuffleEnvironment(network)
			.setConnectionManager(new TestingConnectionManager())
			.buildUnknownAndSetToGate(inputGate);
	}

	private NettyShuffleEnvironment createNettyShuffleEnvironment() {
		return new NettyShuffleEnvironmentBuilder()
			.setIsCreditBased(enableCreditBasedFlowControl)
			.build();
	}

	static void verifyBufferOrEvent(
			InputGate inputGate,
			boolean expectedIsBuffer,
			int expectedChannelIndex,
			boolean expectedMoreAvailable) throws IOException, InterruptedException {

		final Optional<BufferOrEvent> bufferOrEvent = inputGate.getNext();
		assertTrue(bufferOrEvent.isPresent());
		assertEquals(expectedIsBuffer, bufferOrEvent.get().isBuffer());
		assertEquals(expectedChannelIndex, bufferOrEvent.get().getChannelIndex());
		assertEquals(expectedMoreAvailable, bufferOrEvent.get().moreAvailable());
		if (!expectedMoreAvailable) {
			assertFalse(inputGate.pollNext().isPresent());
		}
	}
}
