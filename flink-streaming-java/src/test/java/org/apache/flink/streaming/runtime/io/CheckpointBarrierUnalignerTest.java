/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.checkpoint.channel.RecordingChannelStateWriter;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironmentBuilder;
import org.apache.flink.runtime.io.network.TestingConnectionManager;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelBuilder;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGateBuilder;
import org.apache.flink.runtime.io.network.util.TestBufferFactory;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.streaming.runtime.io.CheckpointBarrierUnaligner.ThreadSafeUnaligner;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.TestSubtaskCheckpointCoordinator;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxDefaultAction;
import org.apache.flink.util.function.ThrowingRunnable;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter.NO_OP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the behaviors of the {@link CheckpointedInputGate}.
 */
public class CheckpointBarrierUnalignerTest {

	private int sizeCounter = 1;

	private CheckpointedInputGate inputGate;

	private RecordingChannelStateWriter channelStateWriter;

	private int[] sequenceNumbers;

	private List<BufferOrEvent> output;

	@Before
	public void setUp() {
		channelStateWriter = new RecordingChannelStateWriter();
	}

	@After
	public void ensureEmpty() throws Exception {
		if (inputGate != null) {
			assertFalse(inputGate.pollNext().isPresent());
			assertTrue(inputGate.isFinished());
			inputGate.close();
		}

		if (channelStateWriter != null) {
			channelStateWriter.close();
		}
	}

	// ------------------------------------------------------------------------
	//  Tests
	// ------------------------------------------------------------------------

	/**
	 * Validates that the buffer behaves correctly if no checkpoint barriers come, for a single input channel.
	 */
	@Test
	public void testSingleChannelNoBarriers() throws Exception {
		inputGate = createInputGate(1, new ValidatingCheckpointHandler(1));
		final BufferOrEvent[] sequence = addSequence(inputGate,
			createBuffer(0), createBuffer(0),
			createBuffer(0), createEndOfPartition(0));

		assertOutput(sequence);
		assertInflightData();
	}

	/**
	 * Validates that the buffer behaves correctly if no checkpoint barriers come, for an input with multiple input
	 * channels.
	 */
	@Test
	public void testMultiChannelNoBarriers() throws Exception {
		inputGate = createInputGate(4, new ValidatingCheckpointHandler(1));
		final BufferOrEvent[] sequence = addSequence(inputGate,
			createBuffer(2), createBuffer(2), createBuffer(0),
			createBuffer(1), createBuffer(0), createEndOfPartition(0),
			createBuffer(3), createBuffer(1), createEndOfPartition(3),
			createBuffer(1), createEndOfPartition(1), createBuffer(2), createEndOfPartition(2));

		assertOutput(sequence);
		assertInflightData();
	}

	/**
	 * Validates that the buffer preserved the order of elements for a input with a single input channel, and checkpoint
	 * events.
	 */
	@Test
	public void testSingleChannelWithBarriers() throws Exception {
		ValidatingCheckpointHandler handler = new ValidatingCheckpointHandler(1);
		inputGate = createInputGate(1, handler);
		final BufferOrEvent[] sequence = addSequence(inputGate,
			createBuffer(0), createBuffer(0), createBuffer(0),
			createBarrier(1, 0),
			createBuffer(0), createBuffer(0), createBuffer(0), createBuffer(0),
			createBarrier(2, 0), createBarrier(3, 0),
			createBuffer(0), createBuffer(0),
			createBarrier(4, 0), createBarrier(5, 0), createBarrier(6, 0),
			createBuffer(0), createEndOfPartition(0));

		assertOutput(sequence);
	}

	/**
	 * Validates that the buffer correctly aligns the streams for inputs with multiple input channels, by buffering and
	 * blocking certain inputs.
	 */
	@Test
	public void testMultiChannelWithBarriers() throws Exception {
		ValidatingCheckpointHandler handler = new ValidatingCheckpointHandler(1);
		inputGate = createInputGate(3, handler);

		// checkpoint with in-flight data
		BufferOrEvent[] sequence1 = addSequence(inputGate,
			createBuffer(0), createBuffer(2), createBuffer(0),
			createBarrier(1, 1), createBarrier(1, 2),
			createBuffer(2), createBuffer(1), createBuffer(0), // last buffer = in-flight
			createBarrier(1, 0));

		// checkpoint 1 triggered unaligned
		assertOutput(sequence1);
		assertEquals(1L, channelStateWriter.getLastStartedCheckpointId());
		assertInflightData(sequence1[7]);

		// checkpoint without in-flight data
		BufferOrEvent[] sequence2 = addSequence(inputGate,
			createBuffer(0), createBuffer(0), createBuffer(1), createBuffer(1), createBuffer(2),
			createBarrier(2, 0), createBarrier(2, 1), createBarrier(2, 2));

		assertOutput(sequence2);
		assertEquals(2L, channelStateWriter.getLastStartedCheckpointId());
		assertInflightData();

		// checkpoint with data only from one channel
		BufferOrEvent[] sequence3 = addSequence(inputGate,
			createBuffer(2), createBuffer(2),
			createBarrier(3, 2),
			createBuffer(2), createBuffer(2),
			createBarrier(3, 0), createBarrier(3, 1));

		assertOutput(sequence3);
		assertEquals(3L, channelStateWriter.getLastStartedCheckpointId());
		assertInflightData();

		// empty checkpoint
		addSequence(inputGate,
			createBarrier(4, 1), createBarrier(4, 2), createBarrier(4, 0));

		assertOutput();
		assertEquals(4L, channelStateWriter.getLastStartedCheckpointId());
		assertInflightData();

		// checkpoint with in-flight data in mixed order
		BufferOrEvent[] sequence5 = addSequence(inputGate,
			createBuffer(0), createBuffer(2), createBuffer(0),
			createBarrier(5, 1),
			createBuffer(2), createBuffer(0), createBuffer(2), createBuffer(1),
			createBarrier(5, 2),
			createBuffer(1), createBuffer(0), createBuffer(2), createBuffer(1),
			createBarrier(5, 0));

		assertOutput(sequence5);
		assertEquals(5L, channelStateWriter.getLastStartedCheckpointId());
		assertInflightData(sequence5[4], sequence5[5], sequence5[6], sequence5[10]);

		// some trailing data
		BufferOrEvent[] sequence6 = addSequence(inputGate,
			createBuffer(0),
			createEndOfPartition(0), createEndOfPartition(1), createEndOfPartition(2));

		assertOutput(sequence6);
		assertInflightData();
	}

	@Test
	public void testMultiChannelTrailingInflightData() throws Exception {
		ValidatingCheckpointHandler handler = new ValidatingCheckpointHandler(1);
		inputGate = createInputGate(3, handler);

		BufferOrEvent[] sequence = addSequence(inputGate,
			createBuffer(0), createBuffer(1), createBuffer(2),
			createBarrier(1, 1), createBarrier(1, 2), createBarrier(1, 0),

			createBuffer(2), createBuffer(1), createBuffer(0),
			createBarrier(2, 1),
			createBuffer(1), createBuffer(1), createEndOfPartition(1), createBuffer(0), createBuffer(2),
			createBarrier(2, 2),
			createBuffer(2), createEndOfPartition(2), createBuffer(0), createEndOfPartition(0));

		assertOutput(sequence);
		assertEquals(2L, channelStateWriter.getLastStartedCheckpointId());
		// TODO: treat EndOfPartitionEvent as a special CheckpointBarrier?
		assertInflightData();
	}

	@Test
	public void testMissingCancellationBarriers() throws Exception {
		ValidatingCheckpointHandler handler = new ValidatingCheckpointHandler(1);
		inputGate = createInputGate(2, handler);
		final BufferOrEvent[] sequence = addSequence(inputGate,
			createBarrier(1L, 0),
			createCancellationBarrier(2L, 0),
			createCancellationBarrier(3L, 0),
			createCancellationBarrier(3L, 1),
			createBuffer(0),
			createEndOfPartition(0), createEndOfPartition(1));

		assertOutput(sequence);
		assertEquals(1L, channelStateWriter.getLastStartedCheckpointId());
		assertEquals(3L, handler.getLastCanceledCheckpointId());
		assertInflightData();
	}

	@Test
	public void testEarlyCleanup() throws Exception {
		ValidatingCheckpointHandler handler = new ValidatingCheckpointHandler(1);
		inputGate = createInputGate(3, handler);

		// checkpoint 1
		final BufferOrEvent[] sequence1 = addSequence(inputGate,
			createBuffer(0), createBuffer(1), createBuffer(2),
			createBarrier(1, 1), createBarrier(1, 2), createBarrier(1, 0));
		assertOutput(sequence1);
		assertEquals(1L, channelStateWriter.getLastStartedCheckpointId());
		assertInflightData();

		// checkpoint 2
		final BufferOrEvent[] sequence2 = addSequence(inputGate,
			createBuffer(2), createBuffer(1), createBuffer(0),
			createBarrier(2, 1),
			createBuffer(1), createBuffer(1), createEndOfPartition(1), createBuffer(0), createBuffer(2),
			createBarrier(2, 2),
			createBuffer(2), createEndOfPartition(2), createBuffer(0), createEndOfPartition(0));
		assertOutput(sequence2);
		assertEquals(2L, channelStateWriter.getLastStartedCheckpointId());
		assertInflightData();
	}

	@Test
	public void testStartAlignmentWithClosedChannels() throws Exception {
		ValidatingCheckpointHandler handler = new ValidatingCheckpointHandler(2);
		inputGate = createInputGate(4, handler);

		final BufferOrEvent[] sequence1 = addSequence(inputGate,
			// close some channels immediately
			createEndOfPartition(2), createEndOfPartition(1),

			// checkpoint without in-flight data
			createBuffer(0), createBuffer(0), createBuffer(3),
			createBarrier(2, 3), createBarrier(2, 0));
		assertOutput(sequence1);
		assertEquals(2L, channelStateWriter.getLastStartedCheckpointId());
		assertInflightData();

		// checkpoint with in-flight data
		final BufferOrEvent[] sequence2 = addSequence(inputGate,
			createBuffer(3), createBuffer(0),
			createBarrier(3, 3),
			createBuffer(3), createBuffer(0),
			createBarrier(3, 0));
		assertOutput(sequence2);
		assertEquals(3L, channelStateWriter.getLastStartedCheckpointId());
		assertInflightData(sequence2[4]);

		// empty checkpoint
		final BufferOrEvent[] sequence3 = addSequence(inputGate,
			createBarrier(4, 0), createBarrier(4, 3));
		assertOutput(sequence3);
		assertEquals(4L, channelStateWriter.getLastStartedCheckpointId());
		assertInflightData();

		// some data, one channel closes
		final BufferOrEvent[] sequence4 = addSequence(inputGate,
			createBuffer(0), createBuffer(0), createBuffer(3),
			createEndOfPartition(0));
		assertOutput(sequence4);
		assertEquals(-1L, channelStateWriter.getLastStartedCheckpointId());
		assertInflightData();

		// checkpoint on last remaining channel
		final BufferOrEvent[] sequence5 = addSequence(
			inputGate,
			createBuffer(3),
			createBarrier(5, 3),
			createBuffer(3),
			createEndOfPartition(3));
		assertOutput(sequence5);
		assertEquals(5L, channelStateWriter.getLastStartedCheckpointId());
		assertInflightData();
	}

	@Test
	public void testEndOfStreamWhileCheckpoint() throws Exception {
		ValidatingCheckpointHandler handler = new ValidatingCheckpointHandler(1);
		inputGate = createInputGate(3, handler);

		// one checkpoint
		final BufferOrEvent[] sequence1 = addSequence(inputGate,
			createBarrier(1, 0), createBarrier(1, 1), createBarrier(1, 2));
		assertOutput(sequence1);
		assertEquals(1L, channelStateWriter.getLastStartedCheckpointId());
		assertInflightData();

		final BufferOrEvent[] sequence2 = addSequence(inputGate,
			// some buffers
			createBuffer(0), createBuffer(0), createBuffer(2),

			// start the checkpoint that will be incomplete
			createBarrier(2, 2), createBarrier(2, 0),
			createBuffer(0), createBuffer(2), createBuffer(1),

			// close one after the barrier one before the barrier
			createEndOfPartition(2), createEndOfPartition(1),
			createBuffer(0),

			// final end of stream
			createEndOfPartition(0));
		assertOutput(sequence2);
		assertEquals(2L, channelStateWriter.getLastStartedCheckpointId());
		assertInflightData(sequence2[7]);
	}

	@Test
	public void testSingleChannelAbortCheckpoint() throws Exception {
		ValidatingCheckpointHandler handler = new ValidatingCheckpointHandler(1);
		inputGate = createInputGate(1, handler);
		final BufferOrEvent[] sequence1 = addSequence(
			inputGate,
			createBuffer(0),
			createBarrier(1, 0),
			createBuffer(0),
			createBarrier(2, 0),
			createCancellationBarrier(4, 0));

		assertOutput(sequence1);
		assertEquals(2L, channelStateWriter.getLastStartedCheckpointId());
		assertEquals(4L, handler.getLastCanceledCheckpointId());
		assertInflightData();

		final BufferOrEvent[] sequence2 = addSequence(
			inputGate,
			createBarrier(5, 0),
			createBuffer(0),
			createCancellationBarrier(6, 0),
			createBuffer(0),
			createEndOfPartition(0));

		assertOutput(sequence2);
		assertEquals(5L, channelStateWriter.getLastStartedCheckpointId());
		assertEquals(6L, handler.getLastCanceledCheckpointId());
		assertInflightData();
	}

	@Test
	public void testMultiChannelAbortCheckpoint() throws Exception {
		ValidatingCheckpointHandler handler = new ValidatingCheckpointHandler(1);
		inputGate = createInputGate(3, handler);
		// some buffers and a successful checkpoint
		final BufferOrEvent[] sequence1 = addSequence(inputGate,
			createBuffer(0), createBuffer(2), createBuffer(0),
			createBarrier(1, 1), createBarrier(1, 2),
			createBuffer(2), createBuffer(1),
			createBarrier(1, 0),
			createBuffer(0), createBuffer(2));

		assertOutput(sequence1);
		assertEquals(1L, channelStateWriter.getLastStartedCheckpointId());
		assertInflightData();

		// canceled checkpoint on last barrier
		final BufferOrEvent[] sequence2 = addSequence(inputGate,
			createBarrier(2, 0), createBarrier(2, 2),
			createBuffer(0), createBuffer(2),
			createCancellationBarrier(2, 1));

		assertOutput(sequence2);
		assertEquals(2L, channelStateWriter.getLastStartedCheckpointId());
		assertEquals(2L, handler.getLastCanceledCheckpointId());
		assertInflightData();

		// one more successful checkpoint
		final BufferOrEvent[] sequence3 = addSequence(inputGate,
			createBuffer(2), createBuffer(1),
			createBarrier(3, 1), createBarrier(3, 2), createBarrier(3, 0));

		assertOutput(sequence3);
		assertEquals(3L, channelStateWriter.getLastStartedCheckpointId());
		assertInflightData();

		// this checkpoint gets immediately canceled, don't start a checkpoint at all
		final BufferOrEvent[] sequence4 = addSequence(inputGate,
			createBuffer(0), createBuffer(1),
			createCancellationBarrier(4, 1), createBarrier(4, 2),
			createBuffer(0),
			createBarrier(4, 0));

		assertOutput(sequence4);
		assertEquals(-1, channelStateWriter.getLastStartedCheckpointId());
		assertEquals(4L, handler.getLastCanceledCheckpointId());
		assertInflightData();

		// a simple successful checkpoint
		// another successful checkpoint
		final BufferOrEvent[] sequence5 = addSequence(inputGate,
			createBuffer(0), createBuffer(1), createBuffer(2),
			createBarrier(5, 2), createBarrier(5, 1), createBarrier(5, 0),
			createBuffer(0), createBuffer(1));

		assertOutput(sequence5);
		assertEquals(5L, channelStateWriter.getLastStartedCheckpointId());
		assertInflightData();

		// abort multiple cancellations and a barrier after the cancellations, don't start a checkpoint at all
		final BufferOrEvent[] sequence6 = addSequence(inputGate,
			createCancellationBarrier(6, 1), createCancellationBarrier(6, 2),
			createBarrier(6, 0),
			createBuffer(0),
			createEndOfPartition(0), createEndOfPartition(1), createEndOfPartition(2));

		assertOutput(sequence6);
		assertEquals(-1L, channelStateWriter.getLastStartedCheckpointId());
		assertEquals(6L, handler.getLastCanceledCheckpointId());
		assertInflightData();
	}

	/**
	 * Tests the race condition between {@link CheckpointBarrierUnaligner#processBarrier(CheckpointBarrier, int)}
	 * and {@link ThreadSafeUnaligner#notifyBarrierReceived(CheckpointBarrier, InputChannelInfo)}. The barrier
	 * notification will trigger an async checkpoint (ch1) via mailbox, and meanwhile the barrier processing will
	 * execute the next checkpoint (ch2) directly in advance. When the ch1 action is taken from mailbox to execute,
	 * it should be exit because it is smaller than the finished ch2.
	 */
	@Test
	public void testConcurrentProcessBarrierAndNotifyBarrierReceived() throws Exception {
		final ValidatingCheckpointInvokable invokable = new ValidatingCheckpointInvokable();
		final CheckpointBarrierUnaligner handler = new CheckpointBarrierUnaligner(new int[] { 1 }, TestSubtaskCheckpointCoordinator.INSTANCE, "test", invokable);
		final InputChannelInfo channelInfo = new InputChannelInfo(0, 0);
		final ExecutorService executor = Executors.newFixedThreadPool(1);

		try {
			// Enqueue the checkpoint (ch0) action into the mailbox of invokable because it is triggered by other thread.
			Callable<Void> notifyTask = () -> {
				handler.getThreadSafeUnaligner().notifyBarrierReceived(buildCheckpointBarrier(0), channelInfo);
				return null;
			};
			Future<Void> result = executor.submit(notifyTask);
			result.get();

			// Execute the checkpoint (ch1) directly because it is triggered by main thread.
			handler.processBarrier(buildCheckpointBarrier(1), 0);

			// Run the previous queued mailbox action to execute ch0.
			invokable.runMailboxStep();

			// ch0 will not be executed finally because it is smaller than the previously executed ch1.
			assertEquals(1, invokable.getTriggeredCheckpointId());
			assertEquals(1, invokable.getTotalTriggeredCheckpoints());
		} finally {
			executor.shutdown();
		}
	}

	// ------------------------------------------------------------------------
	//  Utils
	// ------------------------------------------------------------------------

	private BufferOrEvent createBarrier(long checkpointId, int channel) {
		sizeCounter++;
		return new BufferOrEvent(
			new CheckpointBarrier(
				checkpointId,
				System.currentTimeMillis(),
				CheckpointOptions.forCheckpointWithDefaultLocation()),
			channel);
	}

	private BufferOrEvent createCancellationBarrier(long checkpointId, int channel) {
		sizeCounter++;
		return new BufferOrEvent(new CancelCheckpointMarker(checkpointId), channel);
	}

	private BufferOrEvent createBuffer(int channel) {
		final int size = sizeCounter++;
		return new BufferOrEvent(TestBufferFactory.createBuffer(size), channel);
	}

	private static BufferOrEvent createEndOfPartition(int channel) {
		return new BufferOrEvent(EndOfPartitionEvent.INSTANCE, channel);
	}

	private CheckpointedInputGate createInputGate(
			int numberOfChannels,
			AbstractInvokable toNotify) throws IOException, InterruptedException {
		final NettyShuffleEnvironment environment = new NettyShuffleEnvironmentBuilder().build();
		SingleInputGate gate = new SingleInputGateBuilder()
			.setNumberOfChannels(numberOfChannels)
			.setupBufferPoolFactory(environment)
			.build();
		gate.setInputChannels(
			IntStream.range(0, numberOfChannels)
			.mapToObj(channelIndex ->
				InputChannelBuilder.newBuilder()
					.setChannelIndex(channelIndex)
					.setupFromNettyShuffleEnvironment(environment)
					.setConnectionManager(new TestingConnectionManager())
					.buildRemoteChannel(gate))
			.toArray(RemoteInputChannel[]::new));
		sequenceNumbers = new int[numberOfChannels];

		gate.setup();
		gate.requestPartitions();

		return createCheckpointedInputGate(gate, toNotify);
	}

	private BufferOrEvent[] addSequence(CheckpointedInputGate inputGate, BufferOrEvent... sequence) throws Exception {
		output = new ArrayList<>();
		for (BufferOrEvent bufferOrEvent : sequence) {
			if (bufferOrEvent.isEvent()) {
				bufferOrEvent = new BufferOrEvent(
					EventSerializer.toBuffer(bufferOrEvent.getEvent()),
					bufferOrEvent.getChannelIndex(),
					bufferOrEvent.moreAvailable());
			}
			((RemoteInputChannel) inputGate.getChannel(bufferOrEvent.getChannelIndex())).onBuffer(
				bufferOrEvent.getBuffer(),
				sequenceNumbers[bufferOrEvent.getChannelIndex()]++,
				0);

			while (inputGate.pollNext().map(output::add).isPresent()) {
			}
		}
		sizeCounter = 1;
		return sequence;
	}

	private CheckpointedInputGate createCheckpointedInputGate(InputGate gate, AbstractInvokable toNotify) {
		final CheckpointBarrierUnaligner barrierHandler = new CheckpointBarrierUnaligner(
			new int[]{ gate.getNumberOfInputChannels() },
			new TestSubtaskCheckpointCoordinator(channelStateWriter),
			"Test",
			toNotify);
		barrierHandler.getBufferReceivedListener().ifPresent(gate::registerBufferReceivedListener);
		return new CheckpointedInputGate(gate, barrierHandler);
	}

	private void assertInflightData(BufferOrEvent... expected) {
		assertEquals("Unexpected in-flight sequence", getIds(Arrays.asList(expected)),
			getIds(getAndResetInflightData()));
	}

	private Collection<BufferOrEvent> getAndResetInflightData() {
		final List<BufferOrEvent> inflightData = channelStateWriter.getAddedInput().entries().stream()
			.map(entry -> new BufferOrEvent(entry.getValue(), entry.getKey().getInputChannelIdx()))
			.collect(Collectors.toList());
		channelStateWriter.reset();
		return inflightData;
	}

	private void assertOutput(BufferOrEvent... expectedSequence) {
		assertEquals("Unexpected output sequence", getIds(Arrays.asList(expectedSequence)), getIds(output));
	}

	private List<Object> getIds(Collection<BufferOrEvent> buffers) {
		return buffers.stream()
			.filter(boe -> !boe.isEvent() || !(boe.getEvent() instanceof CheckpointBarrier || boe.getEvent() instanceof CancelCheckpointMarker))
			.map(boe -> boe.isBuffer() ? boe.getSize() - 1 : boe.getEvent())
			.collect(Collectors.toList());
	}

	private CheckpointBarrier buildCheckpointBarrier(int id) {
		return new CheckpointBarrier(id, 0, CheckpointOptions.forCheckpointWithDefaultLocation());
	}

	// ------------------------------------------------------------------------
	//  Testing Mocks
	// ------------------------------------------------------------------------

	/**
	 * The invokable handler used for triggering checkpoint and validation.
	 */
	private class ValidatingCheckpointHandler extends AbstractInvokable {

		private long nextExpectedCheckpointId;

		private long lastCanceledCheckpointId;

		public ValidatingCheckpointHandler(long nextExpectedCheckpointId) {
			super(new DummyEnvironment("test", 1, 0));
			this.nextExpectedCheckpointId = nextExpectedCheckpointId;
		}

		public void invoke() {
			throw new UnsupportedOperationException();
		}

		public Future<Boolean> triggerCheckpointAsync(
				CheckpointMetaData checkpointMetaData,
				CheckpointOptions checkpointOptions,
				boolean advanceToEndOfEventTime) {
			throw new UnsupportedOperationException("should never be called");
		}

		public void triggerCheckpointOnBarrier(
				CheckpointMetaData checkpointMetaData,
				CheckpointOptions checkpointOptions,
				CheckpointMetrics checkpointMetrics) throws IOException {
			if (nextExpectedCheckpointId != -1L) {
				assertEquals("wrong checkpoint id", nextExpectedCheckpointId, checkpointMetaData.getCheckpointId());
			}

			assertTrue(checkpointMetaData.getTimestamp() > 0);
			assertTrue(checkpointMetrics.getAlignmentDurationNanos() >= 0);

			nextExpectedCheckpointId = checkpointMetaData.getCheckpointId() + 1;

			for (int index = 0; index < inputGate.getNumberOfInputChannels(); index++) {
				inputGate.getChannel(index).spillInflightBuffers(checkpointMetaData.getCheckpointId(), NO_OP);
			}
		}

		@Override
		public <E extends Exception> void executeInTaskThread(
				ThrowingRunnable<E> runnable,
				String descriptionFormat,
				Object... descriptionArgs) throws E {
			runnable.run();
		}

		public void abortCheckpointOnBarrier(long checkpointId, Throwable cause) {
			lastCanceledCheckpointId = checkpointId;
			nextExpectedCheckpointId = -1;
		}

		public Future<Void> notifyCheckpointCompleteAsync(long checkpointId) {
			throw new UnsupportedOperationException("should never be called");
		}

		public long getLastCanceledCheckpointId() {
			return lastCanceledCheckpointId;
		}
	}

	/**
	 * Specific {@link AbstractInvokable} implementation to record and validate which checkpoint
	 * id is executed and how many checkpoints are executed.
	 */
	private static final class ValidatingCheckpointInvokable extends StreamTask {

		private long expectedCheckpointId;

		private int totalNumCheckpoints;

		ValidatingCheckpointInvokable() throws Exception {
			super(new DummyEnvironment("test", 1, 0));
		}

		@Override
		public void init() {
		}

		@Override
		protected void processInput(MailboxDefaultAction.Controller controller) {
		}

		public void triggerCheckpointOnBarrier(
				CheckpointMetaData checkpointMetaData,
				CheckpointOptions checkpointOptions,
				CheckpointMetrics checkpointMetrics) {
			expectedCheckpointId = checkpointMetaData.getCheckpointId();
			totalNumCheckpoints++;
		}

		long getTriggeredCheckpointId() {
			return expectedCheckpointId;
		}

		int getTotalTriggeredCheckpoints() {
			return totalNumCheckpoints;
		}
	}
}
