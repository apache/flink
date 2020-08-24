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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateReader;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriterImpl;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironmentBuilder;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.TaskEventPublisher;
import org.apache.flink.runtime.io.network.TestingConnectionManager;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilderAndConsumerTest;
import org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferDecompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferReceivedListener;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.InputChannelTestUtils;
import org.apache.flink.runtime.io.network.partition.NoOpResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionBuilder;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionTest;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.util.TestTaskEvent;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.UnknownShuffleDescriptor;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.ExceptionUtils;

import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.apache.flink.runtime.io.network.partition.InputChannelTestUtils.createLocalInputChannel;
import static org.apache.flink.runtime.io.network.partition.InputChannelTestUtils.createSingleInputGate;
import static org.apache.flink.runtime.io.network.partition.InputGateFairnessTest.setupInputGate;
import static org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannelTest.submitTasksAndWaitForResults;
import static org.apache.flink.runtime.io.network.util.TestBufferFactory.createBuffer;
import static org.apache.flink.runtime.util.NettyShuffleDescriptorBuilder.createRemoteWithIdAndLocation;
import static org.apache.flink.util.ExceptionUtils.rethrow;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link SingleInputGate}.
 */
public class SingleInputGateTest extends InputGateTestBase {

	/**
	 * Tests {@link InputGate#setup()} should create the respective {@link BufferPool} and assign
	 * exclusive buffers for {@link RemoteInputChannel}s, but should not request partitions.
	 */
	@Test
	public void testSetupLogic() throws Exception {
		final NettyShuffleEnvironment environment = createNettyShuffleEnvironment();
		final SingleInputGate inputGate = createInputGate(environment);
		try {
			// before setup
			assertNull(inputGate.getBufferPool());
			for (InputChannel inputChannel : inputGate.getInputChannels().values()) {
				assertTrue(inputChannel instanceof RecoveredInputChannel || inputChannel instanceof UnknownInputChannel);
				if (inputChannel instanceof RecoveredInputChannel) {
					assertEquals(0, ((RecoveredInputChannel) inputChannel).bufferManager.getNumberOfAvailableBuffers());
				}
			}

			inputGate.setup();

			// after setup
			assertNotNull(inputGate.getBufferPool());
			assertEquals(1, inputGate.getBufferPool().getNumberOfRequiredMemorySegments());
			for (InputChannel inputChannel : inputGate.getInputChannels().values()) {
				if (inputChannel instanceof RemoteRecoveredInputChannel) {
					assertEquals(2, ((RemoteRecoveredInputChannel) inputChannel).bufferManager.getNumberOfAvailableBuffers());
				} else if (inputChannel instanceof LocalRecoveredInputChannel) {
					assertEquals(0, ((LocalRecoveredInputChannel) inputChannel).bufferManager.getNumberOfAvailableBuffers());
				}
			}
		} finally {
			inputGate.close();
			environment.close();
		}
	}

	/**
	 * Tests when the total buffer amount is less than the number of recovered states, all the states
	 * can be read and processed correctly via reusing the recycled buffers.
	 */
	@Test
	public void testReadRecoveredState() throws Exception {
		final int totalStates = 5;
		final int[] states = {1, 2, 3, 4};
		final ChannelStateReader stateReader = new ResultPartitionTest.FiniteChannelStateReader(totalStates, states);

		final int totalBuffers = 3; // the total buffers are less than the requirement from total states
		final NettyShuffleEnvironment environment = new NettyShuffleEnvironmentBuilder()
			.setBufferSize(states.length * Integer.BYTES)
			.setNumNetworkBuffers(totalBuffers)
			.build();
		final SingleInputGate inputGate = createInputGate(environment);

		final ExecutorService executor = Executors.newFixedThreadPool(1);
		try {
			inputGate.setup();
			CompletableFuture<?> future = inputGate.readRecoveredState(executor, stateReader);

			int numConsumedBuffers = 0;
			// consume all the state buffers from one local channel and one remote channel
			while (!(future.isDone() && numConsumedBuffers == totalStates * 2)) {
				if (getNextBufferAndVerify(inputGate, states)) {
					numConsumedBuffers++;
				} else {
					Thread.sleep(3);
				}
			}

			// release the gate to verify that all the requested buffers are recycled
			inputGate.close();
			assertEquals(totalBuffers, environment.getNetworkBufferPool().getNumberOfAvailableMemorySegments());
		} finally {
			executor.shutdown();
			environment.close();
		}
	}

	/**
	 * Tests that {@link SingleInputGate#readRecoveredState(ExecutorService, ChannelStateReader)} and
	 * {@link SingleInputGate#pollNext()} and {@link SingleInputGate#close()} execute concurrently, then
	 * it should not have deadlock issue and potential buffer leak.
	 */
	@Test
	public void testConcurrentReadStateAndProcessAndClose() throws Exception {
		final int totalStates = 5;
		final int[] states = {1, 2, 3, 4};
		final ChannelStateReader stateReader = new ResultPartitionTest.FiniteChannelStateReader(totalStates, states);

		final int totalBuffers = 3;
		final NettyShuffleEnvironment environment = new NettyShuffleEnvironmentBuilder()
			.setBufferSize(states.length * Integer.BYTES)
			.setNumNetworkBuffers(totalBuffers)
			.build();
		final SingleInputGate inputGate = createInputGate(environment);

		final ExecutorService executor = Executors.newFixedThreadPool(3);
		try {
			inputGate.setup();

			Callable<Void> closeTask = () -> {
				inputGate.close();
				return null;
			};

			Callable<Void> readRecoveredStateTask = () -> {
				inputGate.readRecoveredState(executor, stateReader);
				return null;
			};

			Callable<Void> processStateTask = () -> {
				while (true) {
					try {
						if (!getNextBufferAndVerify(inputGate, states)) {
							Thread.sleep(1);
						}
					} catch (Throwable t) {
						return null;
					}
				}
			};

			submitTasksAndWaitForResults(executor, new Callable[] {closeTask, readRecoveredStateTask, processStateTask});
		} finally {
			executor.shutdown();
			// wait until the internal channel state recover task finishes
			executor.awaitTermination(60, TimeUnit.SECONDS);
			assertEquals(totalBuffers, environment.getNetworkBufferPool().getNumberOfAvailableMemorySegments());
			assertTrue(inputGate.getCloseFuture().isDone());

			environment.close();
		}
	}

	@Test
	public void testPartitionRequestLogic() throws Exception {
		final NettyShuffleEnvironment environment = new NettyShuffleEnvironmentBuilder().build();
		final SingleInputGate gate = createInputGate(environment);

		try {
			gate.requestPartitions();
			// check channel error during above partition request
			gate.pollNext();

			Collection<InputChannel> channels = gate.getInputChannels().values();
			for (InputChannel channel: channels) {
				if (channel.getChannelIndex() == 0) {
					assertThat(channel, instanceOf(RemoteInputChannel.class));
					assertNotNull(((RemoteInputChannel) channel).getPartitionRequestClient());
					assertEquals(2, ((RemoteInputChannel) channel).getInitialCredit());
				} else if (channel.getChannelIndex() == 1) {
					assertThat(channel, instanceOf(LocalInputChannel.class));
					assertNotNull(((LocalInputChannel) channel).getSubpartitionView());
				} else if (channel.getChannelIndex() == 2) {
					assertThat(channel, instanceOf(UnknownInputChannel.class));
				}
			}
		} finally {
			gate.close();
			environment.close();
		}
	}

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

		inputGate.setInputChannels(inputChannels);

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

	/**
	 * Tests that the compressed buffer will be decompressed after calling {@link SingleInputGate#getNext()}.
	 */
	@Test
	public void testGetCompressedBuffer() throws Exception {
		int bufferSize = 1024;
		String compressionCodec = "LZ4";
		BufferCompressor compressor = new BufferCompressor(bufferSize, compressionCodec);
		BufferDecompressor decompressor = new BufferDecompressor(bufferSize, compressionCodec);

		try (SingleInputGate inputGate = new SingleInputGateBuilder().setBufferDecompressor(decompressor).build()) {
			TestInputChannel inputChannel = new TestInputChannel(inputGate, 0);

			MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(bufferSize);
			for (int i = 0; i < bufferSize; i += 8) {
				segment.putLongLittleEndian(i, i);
			}
			Buffer uncompressedBuffer = new NetworkBuffer(segment, FreeingBufferRecycler.INSTANCE);
			uncompressedBuffer.setSize(bufferSize);
			Buffer compressedBuffer = compressor.compressToOriginalBuffer(uncompressedBuffer);
			assertTrue(compressedBuffer.isCompressed());

			inputChannel.read(compressedBuffer);
			inputGate.setInputChannels(inputChannel);
			inputGate.notifyChannelNonEmpty(inputChannel);

			Optional<BufferOrEvent> bufferOrEvent = inputGate.getNext();
			assertTrue(bufferOrEvent.isPresent());
			assertTrue(bufferOrEvent.get().isBuffer());
			ByteBuffer buffer = bufferOrEvent.get().getBuffer().getNioBufferReadable().order(ByteOrder.LITTLE_ENDIAN);
			for (int i = 0; i < bufferSize; i += 8) {
				assertEquals(i, buffer.getLong());
			}
		}
	}

	@Test
	public void testIsAvailable() throws Exception {
		final SingleInputGate inputGate = createInputGate(1);
		TestInputChannel inputChannel = new TestInputChannel(inputGate, 0);
		inputGate.setInputChannels(inputChannel);

		testIsAvailable(inputGate, inputGate, inputChannel);
	}

	@Test
	public void testIsAvailableAfterFinished() throws Exception {
		final SingleInputGate inputGate = createInputGate(1);
		TestInputChannel inputChannel = new TestInputChannel(inputGate, 0);
		inputGate.setInputChannels(inputChannel);

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

		inputGate.setInputChannels(inputChannels);

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
		TestingTaskEventPublisher taskEventPublisher = new TestingTaskEventPublisher();

		TestingResultPartitionManager partitionManager = new TestingResultPartitionManager(new NoOpResultSubpartitionView());

		// Setup reader with one local and one unknown input channel

		NettyShuffleEnvironment environment = createNettyShuffleEnvironment();
		final SingleInputGate inputGate = createInputGate(environment, 2, ResultPartitionType.PIPELINED);
		final InputChannel[] inputChannels = new InputChannel[2];
		try {
			// Local
			ResultPartitionID localPartitionId = new ResultPartitionID();

			inputChannels[0] = InputChannelBuilder.newBuilder()
				.setPartitionId(localPartitionId)
				.setPartitionManager(partitionManager)
				.setTaskEventPublisher(taskEventPublisher)
				.buildLocalChannel(inputGate);

			// Unknown
			ResultPartitionID unknownPartitionId = new ResultPartitionID();

			inputChannels[1] = InputChannelBuilder.newBuilder()
				.setChannelIndex(1)
				.setPartitionId(unknownPartitionId)
				.setPartitionManager(partitionManager)
				.setTaskEventPublisher(taskEventPublisher)
				.buildUnknownChannel(inputGate);

			setupInputGate(inputGate, inputChannels);

			// Only the local channel can request
			assertEquals(1, partitionManager.counter);

			// Send event backwards and initialize unknown channel afterwards
			final TaskEvent event = new TestTaskEvent();
			inputGate.sendTaskEvent(event);

			// Only the local channel can send out the event
			assertEquals(1, taskEventPublisher.counter);

			// After the update, the pending event should be send to local channel

			ResourceID location = ResourceID.generate();
			inputGate.updateInputChannel(location, createRemoteWithIdAndLocation(unknownPartitionId.getPartitionId(), location));

			assertEquals(2, partitionManager.counter);
			assertEquals(2, taskEventPublisher.counter);
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

		TestingResultPartitionManager partitionManager = new TestingResultPartitionManager(new NoOpResultSubpartitionView());

		InputChannel unknown = InputChannelBuilder.newBuilder()
			.setPartitionManager(partitionManager)
			.buildUnknownChannel(inputGate);
		inputGate.setInputChannels(unknown);

		// Update to a local channel and verify that no request is triggered
		ResultPartitionID resultPartitionID = unknown.getPartitionId();
		ResourceID location = ResourceID.generate();
		inputGate.updateInputChannel(location, createRemoteWithIdAndLocation(resultPartitionID.getPartitionId(), location));

		assertEquals(0, partitionManager.counter);
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

		InputChannel inputChannel = InputChannelBuilder.newBuilder().buildUnknownChannel(inputGate);
		inputGate.setInputChannels(inputChannel);

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
				0,
				gateDesc,
				SingleInputGateBuilder.NO_OP_PRODUCER_CHECKER,
				InputChannelTestUtils.newUnregisteredInputChannelMetrics());

		gate.convertRecoveredInputChannels();

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
					.buildRemoteChannel(inputGate);
			inputGate.setInputChannels(remote);
			inputGate.setup();

			NetworkBufferPool bufferPool = network.getNetworkBufferPool();
			// only the exclusive buffers should be assigned/available now
			assertEquals(buffersPerChannel, remote.getNumberOfAvailableBuffers());

			assertEquals(bufferPool.getTotalNumberOfMemorySegments() - buffersPerChannel,
				bufferPool.getNumberOfAvailableMemorySegments());
			// note: exclusive buffers are not handed out into LocalBufferPool and are thus not counted
			assertEquals(extraNetworkBuffersPerGate, bufferPool.countBuffers());
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
			InputChannel inputChannel = buildUnknownInputChannel(network, inputGate, resultPartitionId, 0);

			inputGate.setInputChannels(inputChannel);
			inputGate.setup();
			NetworkBufferPool bufferPool = network.getNetworkBufferPool();

			assertEquals(bufferPool.getTotalNumberOfMemorySegments(),
				bufferPool.getNumberOfAvailableMemorySegments());
			// note: exclusive buffers are not handed out into LocalBufferPool and are thus not counted
			assertEquals(extraNetworkBuffersPerGate, bufferPool.countBuffers());

			// Trigger updates to remote input channel from unknown input channel
			inputGate.updateInputChannel(
				ResourceID.generate(),
				createRemoteWithIdAndLocation(resultPartitionId.getPartitionId(), ResourceID.generate()));

			RemoteInputChannel remote = (RemoteInputChannel) inputGate.getInputChannels()
				.get(resultPartitionId.getPartitionId());
			// only the exclusive buffers should be assigned/available now
			assertEquals(buffersPerChannel, remote.getNumberOfAvailableBuffers());

			assertEquals(bufferPool.getTotalNumberOfMemorySegments() - buffersPerChannel,
				bufferPool.getNumberOfAvailableMemorySegments());
			// note: exclusive buffers are not handed out into LocalBufferPool and are thus not counted
			assertEquals(extraNetworkBuffersPerGate, bufferPool.countBuffers());
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
		final InputChannel[] inputChannels = new InputChannel[2];

		try {
			final ResultPartitionID localResultPartitionId = localResultPartition.getPartitionId();
			inputChannels[0] = buildUnknownInputChannel(network, inputGate, localResultPartitionId, 0);

			final ResultPartitionID remoteResultPartitionId = remoteResultPartition.getPartitionId();
			inputChannels[1] = buildUnknownInputChannel(network, inputGate, remoteResultPartitionId, 1);

			inputGate.setInputChannels(inputChannels);
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
		final InputChannel[] inputChannels = new InputChannel[2];

		final RemoteInputChannel remoteInputChannel = InputChannelBuilder.newBuilder()
			.setChannelIndex(1)
			.setupFromNettyShuffleEnvironment(network)
			.setConnectionManager(new TestingConnectionManager())
			.buildRemoteChannel(inputGate);
		inputChannels[0] = remoteInputChannel;

		inputChannels[1] = InputChannelBuilder.newBuilder()
			.setChannelIndex(0)
			.setPartitionId(localResultPartitionId)
			.setupFromNettyShuffleEnvironment(network)
			.setConnectionManager(new TestingConnectionManager())
			.buildLocalChannel(inputGate);

		try {
			resultPartition.setup();
			setupInputGate(inputGate, inputChannels);

			remoteInputChannel.onBuffer(createBuffer(1), 0, 0);
			assertEquals(1, inputGate.getNumberOfQueuedBuffers());

			resultPartition.addBufferConsumer(BufferBuilderTestUtils.createFilledFinishedBufferConsumer(1), 0);
			assertEquals(2, inputGate.getNumberOfQueuedBuffers());
		} finally {
			resultPartition.release();
			inputGate.close();
			network.close();
		}
	}

	@Test
	public void testBufferReceivedListener() throws Exception {
		final NettyShuffleEnvironment network = createNettyShuffleEnvironment();

		// Setup
		final SingleInputGate inputGate = createInputGate(network, 2, ResultPartitionType.PIPELINED);

		final RemoteInputChannel remoteInputChannel1 = InputChannelBuilder.newBuilder()
			.setChannelIndex(0)
			.setupFromNettyShuffleEnvironment(network)
			.setConnectionManager(new TestingConnectionManager())
			.buildRemoteChannel(inputGate);
		final RemoteInputChannel remoteInputChannel2 = InputChannelBuilder.newBuilder()
			.setChannelIndex(1)
			.setupFromNettyShuffleEnvironment(network)
			.setConnectionManager(new TestingConnectionManager())
			.buildRemoteChannel(inputGate);
		inputGate.setInputChannels(remoteInputChannel1, remoteInputChannel2);

		final List<BufferOrEvent> notifications = new ArrayList<>();
		inputGate.registerBufferReceivedListener(new BufferReceivedListener() {
			@Override
			public void notifyBufferReceived(Buffer buffer, InputChannelInfo channelInfo) {
				notifications.add(new BufferOrEvent(buffer, channelInfo));
			}

			@Override
			public void notifyBarrierReceived(CheckpointBarrier barrier, InputChannelInfo channelInfo) {
				notifications.add(new BufferOrEvent(barrier, channelInfo));
			}
		});
		setupInputGate(inputGate, remoteInputChannel1, remoteInputChannel2);

		// Test
		CheckpointOptions options = new CheckpointOptions(
			CheckpointType.CHECKPOINT,
			new CheckpointStorageLocationReference(new byte[]{0, 1, 2}));

		remoteInputChannel1.onBuffer(createBuffer(1), 0, 0);
		remoteInputChannel2.onBuffer(EventSerializer.toBuffer(new CheckpointBarrier(0, 0, options)), 0, 0);
		remoteInputChannel1.spillInflightBuffers(0, ChannelStateWriter.NO_OP);
		remoteInputChannel2.spillInflightBuffers(0, ChannelStateWriter.NO_OP);
		remoteInputChannel1.onBuffer(createBuffer(11), 1, 0);
		remoteInputChannel2.onBuffer(createBuffer(12), 1, 0);
		remoteInputChannel1.onBuffer(EventSerializer.toBuffer(new CheckpointBarrier(1, 0, options)), 2, 0);
		remoteInputChannel1.spillInflightBuffers(1, ChannelStateWriter.NO_OP);
		remoteInputChannel2.spillInflightBuffers(1, ChannelStateWriter.NO_OP);
		remoteInputChannel1.onBuffer(createBuffer(21), 3, 0);
		remoteInputChannel2.onBuffer(createBuffer(22), 2, 0);

		inputGate.notifyChannelNonEmpty(remoteInputChannel1);
		inputGate.notifyChannelNonEmpty(remoteInputChannel2);

		while (inputGate.pollNext().isPresent()) {
			// do nothing here, all logic is handled by the listener in this test
		}

		assertEquals(getIds(asList(
			new BufferOrEvent(new CheckpointBarrier(0, 0, options), remoteInputChannel2.getChannelInfo()),
			new BufferOrEvent(createBuffer(11), remoteInputChannel1.getChannelInfo()),
			new BufferOrEvent(new CheckpointBarrier(1, 0, options), remoteInputChannel1.getChannelInfo()),
			new BufferOrEvent(createBuffer(22), remoteInputChannel2.getChannelInfo())
		)), getIds(notifications));
	}

	private List<Object> getIds(Collection<BufferOrEvent> buffers) {
		return buffers.stream()
			.map(boe -> boe.isBuffer() ? boe.getSize() : boe.getEvent())
			.collect(Collectors.toList());
	}

	/**
	 * Tests that if the {@link PartitionNotFoundException} is set onto one {@link InputChannel},
	 * then it would be thrown directly via {@link SingleInputGate#getNext()}. So we
	 * could confirm the {@link SingleInputGate} would not swallow or transform the original exception.
	 */
	@Test
	public void testPartitionNotFoundExceptionWhileGetNextBuffer() throws Exception {
		final SingleInputGate inputGate = createSingleInputGate(1);
		final LocalInputChannel localChannel = createLocalInputChannel(inputGate, new ResultPartitionManager());
		final ResultPartitionID partitionId = localChannel.getPartitionId();

		inputGate.setInputChannels(localChannel);
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

	@Test
	public void testSingleInputGateInfo() {
		final int numSingleInputGates = 2;
		final int numInputChannels = 3;

		for (int i = 0; i < numSingleInputGates; i++) {
			final SingleInputGate gate = new SingleInputGateBuilder()
				.setSingleInputGateIndex(i)
				.setNumberOfChannels(numInputChannels)
				.build();

			int channelCounter = 0;
			for (InputChannel inputChannel : gate.getInputChannels().values()) {
				InputChannelInfo channelInfo = inputChannel.getChannelInfo();

				assertEquals(i, channelInfo.getGateIdx());
				assertEquals(channelCounter++, channelInfo.getInputChannelIdx());
			}
		}
	}

	@Test
	public void testConcurrentReceiveBuffersAndSpillInflightBuffers() throws Exception {
		NettyShuffleEnvironment network = createNettyShuffleEnvironment();
		SingleInputGate inputGate = createInputGate(network, 1, ResultPartitionType.PIPELINED);
		RemoteInputChannel inputChannel = InputChannelBuilder.newBuilder()
			.setChannelIndex(0)
			.setupFromNettyShuffleEnvironment(network)
			.setConnectionManager(new TestingConnectionManager())
			.setNetworkBuffersPerChannel(0)
			.buildRemoteChannel(inputGate);

		List<Buffer> inflightBuffers = new ArrayList<>();
		inputGate.registerBufferReceivedListener(new BufferReceivedListener() {
			@Override
			public void notifyBufferReceived(Buffer buffer, InputChannelInfo channelInfo) {
				inflightBuffers.add(buffer);
			}

			@Override
			public void notifyBarrierReceived(CheckpointBarrier barrier, InputChannelInfo channelInfo) {
			}
		});

		List<Buffer> buffers = new ArrayList<>();
		for (int i = 0; i < 1024; ++i) {
			buffers.add(BufferBuilderTestUtils.buildSomeBuffer(1024));
		}
		CheckpointBarrier barrier = new CheckpointBarrier(0, 0, CheckpointOptions.forCheckpointWithDefaultLocation());
		Thread bufferReceiver = new Thread(() -> {
			try {
				for (int i = 0; i < buffers.size(); ++i) {
					inputChannel.onBuffer(buffers.get(i), i, 0);
				}
				// add checkpoint barrier
				inputChannel.onBuffer(EventSerializer.toBuffer(barrier), buffers.size(), 0);
				// one additional buffer which won't be added to inflight buffer queue
				inputChannel.onBuffer(BufferBuilderTestUtils.buildSomeBuffer(1024), buffers.size() + 1, 0);
			} catch (IOException e) {
				ExceptionUtils.rethrow(e);
			}
		});
		bufferReceiver.start();

		inputChannel.spillInflightBuffers(0, new ChannelStateWriterImpl.NoOpChannelStateWriter() {
			@Override
			public void addInputData(long checkpointId, InputChannelInfo info, int startSeqNum, CloseableIterator<Buffer> iterator) {
				List<Buffer> list = new ArrayList<>();
				iterator.forEachRemaining(list::add);
				inflightBuffers.addAll(list);
				try {
					iterator.close();
				} catch (Exception e) {
					rethrow(e);
				}
			}
		});

		bufferReceiver.join();
		assertArrayEquals(buffers.toArray(), inflightBuffers.toArray());
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
			asList(gateDescs)).toArray(new SingleInputGate[] {});
		Map<InputGateID, SingleInputGate> inputGatesById = new HashMap<>();
		for (int i = 0; i < numberOfGates; i++) {
			inputGatesById.put(new InputGateID(ids[i], consumerID), gates[i]);
		}

		return inputGatesById;
	}

	private InputChannel buildUnknownInputChannel(
			NettyShuffleEnvironment network,
			SingleInputGate inputGate,
			ResultPartitionID partitionId,
			int channelIndex) {
		return InputChannelBuilder.newBuilder()
			.setChannelIndex(channelIndex)
			.setPartitionId(partitionId)
			.setupFromNettyShuffleEnvironment(network)
			.setConnectionManager(new TestingConnectionManager())
			.buildUnknownChannel(inputGate);
	}

	private NettyShuffleEnvironment createNettyShuffleEnvironment() {
		return new NettyShuffleEnvironmentBuilder().build();
	}

	static void verifyBufferOrEvent(
			InputGate inputGate,
			boolean expectedIsBuffer,
			int expectedChannelIndex,
			boolean expectedMoreAvailable) throws IOException, InterruptedException {

		final Optional<BufferOrEvent> bufferOrEvent = inputGate.getNext();
		assertTrue(bufferOrEvent.isPresent());
		assertEquals(expectedIsBuffer, bufferOrEvent.get().isBuffer());
		assertEquals(inputGate.getChannel(expectedChannelIndex).getChannelInfo(), bufferOrEvent.get().getChannelInfo());
		assertEquals(expectedMoreAvailable, bufferOrEvent.get().moreAvailable());
		if (!expectedMoreAvailable) {
			assertFalse(inputGate.pollNext().isPresent());
		}
	}

	private SingleInputGate createInputGate(NettyShuffleEnvironment environment) {
		SingleInputGate inputGate = createInputGate(environment, 3, ResultPartitionType.PIPELINED);
		InputChannel remoteChannel = new InputChannelBuilder().setChannelIndex(0).buildRemoteRecoveredChannel(inputGate);
		InputChannel localChannel = new InputChannelBuilder().setChannelIndex(1).buildLocalRecoveredChannel(inputGate);
		InputChannel unknownChannel = new InputChannelBuilder().setChannelIndex(2).buildUnknownChannel(inputGate);
		inputGate.setInputChannels(remoteChannel, localChannel, unknownChannel);
		return inputGate;
	}

	private boolean getNextBufferAndVerify(SingleInputGate inputGate, int[] states) throws Exception {
		Optional<BufferOrEvent> bufferOrEvent = inputGate.pollNext();
		if (bufferOrEvent.isPresent()) {
			assertTrue(bufferOrEvent.get().isBuffer());
			Buffer buffer = bufferOrEvent.get().getBuffer();
			BufferBuilderAndConsumerTest.assertContent(buffer, null, states);
			buffer.recycleBuffer();
			return true;
		}
		return false;
	}

	/**
	 * A testing implementation of {@link ResultPartitionManager} which counts the number of
	 * {@link ResultSubpartitionView} created.
	 */
	public static class TestingResultPartitionManager extends ResultPartitionManager {
		private int counter = 0;
		private final ResultSubpartitionView subpartitionView;

		public TestingResultPartitionManager(ResultSubpartitionView subpartitionView) {
			this.subpartitionView = subpartitionView;
		}

		@Override
		public ResultSubpartitionView createSubpartitionView(
				ResultPartitionID partitionId,
				int subpartitionIndex,
				BufferAvailabilityListener availabilityListener) throws IOException {
			++counter;
			return subpartitionView;
		}
	}

	/**
	 * A testing implementation of {@link TaskEventPublisher} which counts the number of publish times.
	 */
	private static class TestingTaskEventPublisher implements TaskEventPublisher {
		private int counter = 0;

		@Override
		public boolean publish(ResultPartitionID partitionId, TaskEvent event) {
			++counter;
			return true;
		}
	}
}
