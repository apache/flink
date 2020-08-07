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

import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.io.disk.NoOpFileChannelManager;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.buffer.BufferReceivedListener;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.PartitionTestUtils;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionBuilder;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.util.TestBufferFactory;
import org.apache.flink.runtime.io.network.util.TestPartitionProducer;
import org.apache.flink.runtime.io.network.util.TestProducerSource;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.util.function.CheckedSupplier;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import org.hamcrest.Matchers;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.runtime.checkpoint.CheckpointType.SAVEPOINT;
import static org.apache.flink.runtime.io.network.api.serialization.EventSerializer.toBuffer;
import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType.getDataType;
import static org.apache.flink.runtime.io.network.partition.InputChannelTestUtils.createLocalInputChannel;
import static org.apache.flink.runtime.io.network.partition.InputChannelTestUtils.createSingleInputGate;
import static org.apache.flink.runtime.io.network.partition.InputGateFairnessTest.setupInputGate;
import static org.apache.flink.runtime.io.network.partition.consumer.SingleInputGateTest.TestingResultPartitionManager;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link LocalInputChannel}.
 */
public class LocalInputChannelTest {

	/**
	 * Tests the consumption of multiple subpartitions via local input channels.
	 *
	 * <p>Multiple producer tasks produce pipelined partitions, which are consumed by multiple
	 * tasks via local input channels.
	 */
	@Test
	public void testConcurrentConsumeMultiplePartitions() throws Exception {
		// Config
		final int parallelism = 32;
		final int producerBufferPoolSize = parallelism + 1;
		final int numberOfBuffersPerChannel = 1024;

		checkArgument(parallelism >= 1);
		checkArgument(producerBufferPoolSize >= parallelism);
		checkArgument(numberOfBuffersPerChannel >= 1);

		// Setup
		// One thread per produced partition and one per consumer
		final ExecutorService executor = Executors.newFixedThreadPool(2 * parallelism);

		final NetworkBufferPool networkBuffers = new NetworkBufferPool(
			(parallelism * producerBufferPoolSize) + (parallelism * parallelism),
			TestBufferFactory.BUFFER_SIZE);

		final ResultPartitionManager partitionManager = new ResultPartitionManager();

		final ResultPartitionID[] partitionIds = new ResultPartitionID[parallelism];
		final TestPartitionProducer[] partitionProducers = new TestPartitionProducer[parallelism];

		// Create all partitions
		for (int i = 0; i < parallelism; i++) {
			partitionIds[i] = new ResultPartitionID();

			final ResultPartition partition = new ResultPartitionBuilder()
				.setResultPartitionId(partitionIds[i])
				.setNumberOfSubpartitions(parallelism)
				.setNumTargetKeyGroups(parallelism)
				.setResultPartitionManager(partitionManager)
				.setBufferPoolFactory(p ->
					networkBuffers.createBufferPool(producerBufferPoolSize, producerBufferPoolSize))
				.build();

			// Create a buffer pool for this partition
			partition.setup();

			// Create the producer
			partitionProducers[i] = new TestPartitionProducer(
				partition,
				false,
				new TestPartitionProducerBufferSource(
					parallelism,
					partition.getBufferPool(),
					numberOfBuffersPerChannel)
			);
		}

		// Test
		try {
			// Submit producer tasks
			List<CompletableFuture<?>> results = Lists.newArrayListWithCapacity(
				parallelism + 1);

			for (int i = 0; i < parallelism; i++) {
				results.add(CompletableFuture.supplyAsync(
					CheckedSupplier.unchecked(partitionProducers[i]::call), executor));
			}

			// Submit consumer
			for (int i = 0; i < parallelism; i++) {
				final TestLocalInputChannelConsumer consumer = new TestLocalInputChannelConsumer(
					i,
					parallelism,
					numberOfBuffersPerChannel,
					networkBuffers.createBufferPool(parallelism, parallelism),
					partitionManager,
					new TaskEventDispatcher(),
					partitionIds);

				results.add(CompletableFuture.supplyAsync(CheckedSupplier.unchecked(consumer::call), executor));
			}

			FutureUtils.waitForAll(results).get();
		}
		finally {
			networkBuffers.destroyAllBufferPools();
			networkBuffers.destroy();
			executor.shutdown();
		}
	}

	@Test
	public void testPartitionRequestExponentialBackoff() throws Exception {
		// Config
		int initialBackoff = 500;
		int maxBackoff = 3000;

		// Start with initial backoff, then keep doubling, and cap at max.
		int[] expectedDelays = {initialBackoff, 1000, 2000, maxBackoff};

		// Setup
		SingleInputGate inputGate = mock(SingleInputGate.class);

		BufferProvider bufferProvider = mock(BufferProvider.class);
		when(inputGate.getBufferProvider()).thenReturn(bufferProvider);

		ResultPartitionManager partitionManager = mock(ResultPartitionManager.class);

		LocalInputChannel ch = createLocalInputChannel(inputGate, partitionManager, initialBackoff, maxBackoff);

		when(partitionManager
				.createSubpartitionView(eq(ch.partitionId), eq(0), any(BufferAvailabilityListener.class)))
				.thenThrow(new PartitionNotFoundException(ch.partitionId));

		Timer timer = mock(Timer.class);
		doAnswer(new Answer<Void>() {

			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				((TimerTask) invocation.getArguments()[0]).run();
				return null;
			}
		}).when(timer).schedule(any(TimerTask.class), anyLong());

		// Initial request
		ch.requestSubpartition(0);
		verify(partitionManager)
				.createSubpartitionView(eq(ch.partitionId), eq(0), any(BufferAvailabilityListener.class));

		// Request subpartition and verify that the actual requests are delayed.
		for (long expected : expectedDelays) {
			ch.retriggerSubpartitionRequest(timer, 0);

			verify(timer).schedule(any(TimerTask.class), eq(expected));
		}

		// Exception after backoff is greater than the maximum backoff.
		try {
			ch.retriggerSubpartitionRequest(timer, 0);
			ch.getNextBuffer();
			fail("Did not throw expected exception.");
		}
		catch (Exception expected) {
		}
	}

	@Test(expected = CancelTaskException.class)
	public void testProducerFailedException() throws Exception {
		ResultSubpartitionView view = mock(ResultSubpartitionView.class);
		when(view.isReleased()).thenReturn(true);
		when(view.getFailureCause()).thenReturn(new Exception("Expected test exception"));

		ResultPartitionManager partitionManager = mock(ResultPartitionManager.class);
		when(partitionManager
				.createSubpartitionView(any(ResultPartitionID.class), anyInt(), any(BufferAvailabilityListener.class)))
				.thenReturn(view);

		SingleInputGate inputGate = mock(SingleInputGate.class);
		BufferProvider bufferProvider = mock(BufferProvider.class);
		when(inputGate.getBufferProvider()).thenReturn(bufferProvider);

		LocalInputChannel ch = createLocalInputChannel(inputGate, partitionManager);

		ch.requestSubpartition(0);

		// Should throw an instance of CancelTaskException.
		ch.getNextBuffer();
	}

	/**
	 * Tests that {@link LocalInputChannel#requestSubpartition(int)} throws {@link PartitionNotFoundException}
	 * if the result partition was not registered in {@link ResultPartitionManager} and no backoff.
	 */
	@Test
	public void testPartitionNotFoundExceptionWhileRequestingPartition() throws Exception {
		final SingleInputGate inputGate = createSingleInputGate(1);
		final LocalInputChannel localChannel = createLocalInputChannel(inputGate, new ResultPartitionManager());

		try {
			localChannel.requestSubpartition(0);

			fail("Should throw a PartitionNotFoundException.");
		} catch (PartitionNotFoundException notFound) {
			assertThat(localChannel.getPartitionId(), Matchers.is(notFound.getPartitionId()));
		}
	}

	/**
	 * Tests that {@link SingleInputGate#retriggerPartitionRequest(IntermediateResultPartitionID)} is triggered
	 * after {@link LocalInputChannel#requestSubpartition(int)} throws {@link PartitionNotFoundException}
	 * within backoff.
	 */
	@Test
	public void testRetriggerPartitionRequestWhilePartitionNotFound() throws Exception {
		final SingleInputGate inputGate = createSingleInputGate(1);
		final LocalInputChannel localChannel = createLocalInputChannel(
			inputGate, new ResultPartitionManager(), 1, 1);

		inputGate.setInputChannels(localChannel);
		localChannel.requestSubpartition(0);

		// The timer should be initialized at the first time of retriggering partition request.
		assertNotNull(inputGate.getRetriggerLocalRequestTimer());
	}

	/**
	 * Tests that {@link LocalInputChannel#retriggerSubpartitionRequest(Timer, int)} would throw
	 * {@link PartitionNotFoundException} which is set onto the input channel then.
	 */
	@Test
	public void testChannelErrorWhileRetriggeringRequest() {
		final SingleInputGate inputGate = createSingleInputGate(1);
		final LocalInputChannel localChannel = createLocalInputChannel(inputGate, new ResultPartitionManager());

		final Timer timer = new Timer(true) {
			@Override
			public void schedule(TimerTask task, long delay) {
				task.run();

				try {
					localChannel.checkError();

					fail("Should throw a PartitionNotFoundException.");
				} catch (PartitionNotFoundException notFound) {
					assertThat(localChannel.partitionId, Matchers.is(notFound.getPartitionId()));
				} catch (IOException ex) {
					fail("Should throw a PartitionNotFoundException.");
				}
			}
		};

		try {
			localChannel.retriggerSubpartitionRequest(timer, 0);
		} finally {
			timer.cancel();
		}
	}

	/**
	 * Verifies that concurrent release via the SingleInputGate and re-triggering
	 * of a partition request works smoothly.
	 *
	 * <ul>
	 * <li>SingleInputGate acquires its request lock and tries to release all
	 * registered channels. When releasing a channel, it needs to acquire
	 * the channel's shared request-release lock.</li>
	 * <li>If a LocalInputChannel concurrently retriggers a partition request via
	 * a Timer Thread it acquires the channel's request-release lock and calls
	 * the retrigger callback on the SingleInputGate, which again tries to
	 * acquire the gate's request lock.</li>
	 * </ul>
	 *
	 * <p>For certain timings this obviously leads to a deadlock. This test reliably
	 * reproduced such a timing (reported in FLINK-5228). This test is pretty much
	 * testing the buggy implementation and has not much more general value. If it
	 * becomes obsolete at some point (future greatness ;)), feel free to remove it.
	 *
	 * <p>The fix in the end was to to not acquire the channels lock when releasing it
	 * and/or not doing any input gate callbacks while holding the channel's lock.
	 * I decided to do both.
	 */
	@Test
	public void testConcurrentReleaseAndRetriggerPartitionRequest() throws Exception {
		final SingleInputGate gate = createSingleInputGate(1);

		ResultPartitionManager partitionManager = mock(ResultPartitionManager.class);
		when(partitionManager
			.createSubpartitionView(
				any(ResultPartitionID.class),
				anyInt(),
				any(BufferAvailabilityListener.class)))
			.thenAnswer(new Answer<ResultSubpartitionView>() {
				@Override
				public ResultSubpartitionView answer(InvocationOnMock invocationOnMock) throws Throwable {
					// Sleep here a little to give the releaser Thread
					// time to acquire the input gate lock. We throw
					// the Exception to retrigger the request.
					Thread.sleep(100);
					throw new PartitionNotFoundException(new ResultPartitionID());
				}
			});

		final LocalInputChannel channel = createLocalInputChannel(gate, partitionManager, 1, 1);

		Thread releaser = new Thread() {
			@Override
			public void run() {
				try {
					gate.close();
				} catch (IOException ignored) {
				}
			}
		};

		Thread requester = new Thread() {
			@Override
			public void run() {
				try {
					channel.requestSubpartition(0);
				} catch (IOException ignored) {
				}
			}
		};

		requester.start();
		releaser.start();

		releaser.join();
		requester.join();
	}

	/**
	 * Tests that reading from a channel when after the partition has been
	 * released are handled and don't lead to NPEs.
	 */
	@Test
	public void testGetNextAfterPartitionReleased() throws Exception {
		ResultSubpartitionView subpartitionView = createResultSubpartitionView(false);
		TestingResultPartitionManager partitionManager = new TestingResultPartitionManager(subpartitionView);
		LocalInputChannel channel = createLocalInputChannel(new SingleInputGateBuilder().build(), partitionManager);

		channel.requestSubpartition(0);
		assertFalse(channel.getNextBuffer().isPresent());

		// release the subpartition view
		subpartitionView.releaseAllResources();

		try {
			channel.getNextBuffer();
			fail("Did not throw expected CancelTaskException");
		} catch (CancelTaskException ignored) {
		}

		channel.releaseAllResources();
		assertFalse(channel.getNextBuffer().isPresent());
	}

	/**
	 * Verifies that buffer is not compressed when getting from a {@link LocalInputChannel}.
	 */
	@Test
	public void testGetBufferFromLocalChannelWhenCompressionEnabled() throws Exception {
		ResultSubpartitionView subpartitionView = createResultSubpartitionView(true);
		TestingResultPartitionManager partitionManager = new TestingResultPartitionManager(subpartitionView);
		LocalInputChannel channel = createLocalInputChannel(new SingleInputGateBuilder().build(), partitionManager);

		// request partition and get next buffer
		channel.requestSubpartition(0);
		Optional<InputChannel.BufferAndAvailability> bufferAndAvailability = channel.getNextBuffer();
		assertTrue(bufferAndAvailability.isPresent());
		assertFalse(bufferAndAvailability.get().buffer().isCompressed());
	}

	@Test(expected = IllegalStateException.class)
	public void testUnblockReleasedChannel() throws Exception {
		SingleInputGate inputGate = createSingleInputGate(1);
		LocalInputChannel localChannel = createLocalInputChannel(inputGate, new ResultPartitionManager());

		localChannel.releaseAllResources();
		localChannel.resumeConsumption();
	}

	@Test
	public void testNoNotifyOnSavepoint() throws IOException {
		TestBufferReceivedListener listener = new TestBufferReceivedListener();
		SingleInputGate inputGate = new SingleInputGateBuilder().build();
		inputGate.registerBufferReceivedListener(listener);
		LocalInputChannel channel = InputChannelBuilder.newBuilder().buildLocalChannel(inputGate);
		CheckpointBarrier barrier = new CheckpointBarrier(123L, 123L, new CheckpointOptions(SAVEPOINT, CheckpointStorageLocationReference.getDefault()));
		channel.notifyPriorityEvent(new BufferConsumer(toBuffer(barrier).getMemorySegment(), FreeingBufferRecycler.INSTANCE, getDataType(barrier)));
		channel.checkError();
		assertTrue(listener.notifiedOnBarriers.isEmpty());
	}

	@Test
	public void testCheckpointingInflightData() throws Exception {
		SingleInputGate inputGate = new SingleInputGateBuilder().build();
		List<Buffer> receivedBuffers = new ArrayList<>();
		inputGate.registerBufferReceivedListener(new BufferReceivedListener() {
			@Override
			public void notifyBufferReceived(Buffer buffer, InputChannelInfo channelInfo) {
				receivedBuffers.add(buffer);
			}

			@Override
			public void notifyBarrierReceived(CheckpointBarrier barrier, InputChannelInfo channelInfo) {}
		});

		ResultPartition parent = PartitionTestUtils.createPartition(
			ResultPartitionType.PIPELINED,
			NoOpFileChannelManager.INSTANCE);
		ResultSubpartition subpartition = parent.getAllPartitions()[0];
		ResultSubpartitionView subpartitionView = subpartition.createReadView(() -> {});

		TestingResultPartitionManager partitionManager = new TestingResultPartitionManager(subpartitionView);
		LocalInputChannel channel = createLocalInputChannel(inputGate, partitionManager);
		channel.requestSubpartition(0);

		channel.spillInflightBuffers(0L, ChannelStateWriter.NO_OP);
		assertEquals(receivedBuffers, Collections.emptyList());

		// add 1 buffer + 1 event and check that this buffer has also been propagated to ChannelStateWriter
		subpartition.add(BufferBuilderTestUtils.createFilledFinishedBufferConsumer(1));
		Optional<InputChannel.BufferAndAvailability> bufferAndAvailability = channel.getNextBuffer();
		assertTrue(bufferAndAvailability.isPresent());
		subpartition.add(BufferBuilderTestUtils.createEventBufferConsumer(33, Buffer.DataType.EVENT_BUFFER));
		assertTrue(channel.getNextBuffer().isPresent());
		assertEquals(receivedBuffers, Collections.singletonList(bufferAndAvailability.get().buffer()));
	}

	// ---------------------------------------------------------------------------------------------

	private static ResultSubpartitionView createResultSubpartitionView(boolean addBuffer) throws IOException {
		int bufferSize = 4096;
		ResultPartition parent = PartitionTestUtils.createPartition(
			ResultPartitionType.PIPELINED,
			NoOpFileChannelManager.INSTANCE,
			true,
			bufferSize);
		ResultSubpartition subpartition = parent.getAllPartitions()[0];
		if (addBuffer) {
			subpartition.add(BufferBuilderTestUtils.createFilledFinishedBufferConsumer(bufferSize));
		}
		return subpartition.createReadView(() -> {});
	}

	/**
	 * Returns the configured number of buffers for each channel in a random order.
	 */
	private static class TestPartitionProducerBufferSource implements TestProducerSource {

		private final BufferProvider bufferProvider;

		private final List<Byte> channelIndexes;

		public TestPartitionProducerBufferSource(
				int parallelism,
				BufferProvider bufferProvider,
				int numberOfBuffersToProduce) {

			this.bufferProvider = bufferProvider;
			this.channelIndexes = Lists.newArrayListWithCapacity(
					parallelism * numberOfBuffersToProduce);

			// Array of channel indexes to produce buffers for
			for (byte i = 0; i < parallelism; i++) {
				for (int j = 0; j < numberOfBuffersToProduce; j++) {
					channelIndexes.add(i);
				}
			}

			// Random buffer to channel ordering
			Collections.shuffle(channelIndexes);
		}

		@Override
		public BufferConsumerAndChannel getNextBufferConsumer() throws Exception {
			if (channelIndexes.size() > 0) {
				final int channelIndex = channelIndexes.remove(0);
				BufferBuilder bufferBuilder = bufferProvider.requestBufferBuilderBlocking();
				BufferConsumer bufferConsumer = bufferBuilder.createBufferConsumer();
				bufferBuilder.appendAndCommit(ByteBuffer.wrap(new byte[4]));
				bufferBuilder.finish();
				return new BufferConsumerAndChannel(bufferConsumer, channelIndex);
			}

			return null;
		}
	}

	/**
	 * Consumed the configured result partitions and verifies that each channel receives the
	 * expected number of buffers.
	 */
	private static class TestLocalInputChannelConsumer implements Callable<Void> {

		private final SingleInputGate inputGate;

		private final int numberOfInputChannels;

		private final int numberOfExpectedBuffersPerChannel;

		public TestLocalInputChannelConsumer(
				int subpartitionIndex,
				int numberOfInputChannels,
				int numberOfExpectedBuffersPerChannel,
				BufferPool bufferPool,
				ResultPartitionManager partitionManager,
				TaskEventDispatcher taskEventDispatcher,
				ResultPartitionID[] consumedPartitionIds) throws IOException, InterruptedException {

			checkArgument(numberOfInputChannels >= 1);
			checkArgument(numberOfExpectedBuffersPerChannel >= 1);

			this.inputGate = new SingleInputGateBuilder()
				.setConsumedSubpartitionIndex(subpartitionIndex)
				.setNumberOfChannels(numberOfInputChannels)
				.setBufferPoolFactory(bufferPool)
				.build();
			InputChannel[] inputChannels = new InputChannel[numberOfInputChannels];

			// Setup input channels
			for (int i = 0; i < numberOfInputChannels; i++) {
				inputChannels[i] = InputChannelBuilder.newBuilder()
					.setChannelIndex(i)
					.setPartitionManager(partitionManager)
					.setPartitionId(consumedPartitionIds[i])
					.setTaskEventPublisher(taskEventDispatcher)
					.buildLocalChannel(inputGate);
			}

			setupInputGate(inputGate, inputChannels);

			this.numberOfInputChannels = numberOfInputChannels;
			this.numberOfExpectedBuffersPerChannel = numberOfExpectedBuffersPerChannel;
		}

		@Override
		public Void call() throws Exception {
			// One counter per input channel. Expect the same number of buffers from each channel.
			final int[] numberOfBuffersPerChannel = new int[numberOfInputChannels];

			try {
				Optional<BufferOrEvent> boe;
				while ((boe = inputGate.getNext()).isPresent()) {
					if (boe.get().isBuffer()) {
						boe.get().getBuffer().recycleBuffer();

						// Check that we don't receive too many buffers
						if (++numberOfBuffersPerChannel[boe.get().getChannelInfo().getInputChannelIdx()]
								> numberOfExpectedBuffersPerChannel) {

							throw new IllegalStateException("Received more buffers than expected " +
									"on channel " + boe.get().getChannelInfo() + ".");
						}
					}
				}

				// Verify that we received the expected number of buffers on each channel
				for (int i = 0; i < numberOfBuffersPerChannel.length; i++) {
					final int actualNumberOfReceivedBuffers = numberOfBuffersPerChannel[i];

					if (actualNumberOfReceivedBuffers != numberOfExpectedBuffersPerChannel) {
						throw new IllegalStateException("Received unexpected number of buffers " +
								"on channel " + i + " (" + actualNumberOfReceivedBuffers + " instead " +
								"of " + numberOfExpectedBuffersPerChannel + ").");
					}
				}
			}
			finally {
				inputGate.close();
			}

			return null;
		}

	}
}
