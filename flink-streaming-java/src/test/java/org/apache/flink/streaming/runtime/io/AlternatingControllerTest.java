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

import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.checkpoint.channel.RecordingChannelStateWriter;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.EventAnnouncement;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelBuilder;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGateBuilder;
import org.apache.flink.runtime.io.network.partition.consumer.TestInputChannel;
import org.apache.flink.runtime.io.network.util.TestBufferFactory;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.streaming.api.operators.SyncMailboxExecutor;
import org.apache.flink.streaming.runtime.tasks.TestSubtaskCheckpointCoordinator;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxProcessor;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static java.util.Collections.singletonList;
import static junit.framework.TestCase.assertTrue;
import static org.apache.flink.runtime.checkpoint.CheckpointType.CHECKPOINT;
import static org.apache.flink.runtime.checkpoint.CheckpointType.SAVEPOINT;
import static org.apache.flink.runtime.io.network.api.serialization.EventSerializer.toBuffer;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * {@link AlternatingController} test.
 */
public class AlternatingControllerTest {

	@Test
	public void testChannelUnblockedAfterDifferentBarriers() throws Exception {
		CheckpointedInputGate gate = buildGate(new ValidatingCheckpointHandler(), 3);
		long barrierId = 1L;
		long ts = System.currentTimeMillis();
		long timeout = 10;

		send(barrier(barrierId, ts, unaligned(getDefault())), 0, gate);

		TestInputChannel acChannel = (TestInputChannel) gate.getChannel(1);
		acChannel.setBlocked(true);
		send(barrier(barrierId, ts, alignedWithTimeout(getDefault(), Integer.MAX_VALUE)), acChannel.getChannelIndex(), gate);
		assertFalse(acChannel.isBlocked());

		Thread.sleep(timeout);
		TestInputChannel acChannelWithTimeout = (TestInputChannel) gate.getChannel(2);
		acChannelWithTimeout.setBlocked(true);
		send(barrier(barrierId, ts, alignedWithTimeout(getDefault(), timeout)), acChannelWithTimeout.getChannelIndex(), gate);
		assertFalse(acChannelWithTimeout.isBlocked());
	}

	/**
	 * Upon subsuming (or canceling) a checkpoint, channels should be notified regardless of whether UC controller is
	 * currently being used or not. Otherwise, channels may not capture in-flight buffers.
	 */
	@Test
	public void testChannelResetOnNewBarrier() throws Exception {
		RecordingChannelStateWriter stateWriter = new RecordingChannelStateWriter();
		CheckpointedInputGate gate = buildRemoteInputGate(new ValidatingCheckpointHandler(), 2, stateWriter);

		sendBarrier(0, System.currentTimeMillis(), SAVEPOINT, gate, 0); // using AC because UC would require ordering in gate while polling
		((RemoteInputChannel) gate.getChannel(0)).onBuffer(createBuffer(1024), 1, 0); // to be captured
		send(toBuffer(new CheckpointBarrier(1, System.currentTimeMillis(), unaligned(getDefault())), true), 1, gate);

		assertFalse(stateWriter.getAddedInput().isEmpty());
	}

	@Test
	public void testCheckpointHandling() throws Exception {
		testBarrierHandling(CHECKPOINT);
	}

	@Test
	public void testSavepointHandling() throws Exception {
		testBarrierHandling(SAVEPOINT);
	}

	@Test
	public void testAlternation() throws Exception {
		int numBarriers = 123;
		int numChannels = 123;
		ValidatingCheckpointHandler target = new ValidatingCheckpointHandler();
		CheckpointedInputGate gate = buildGate(target, numChannels);
		List<Long> barriers = new ArrayList<>();
		for (long barrier = 0; barrier < numBarriers; barrier++) {
			barriers.add(barrier);
			CheckpointType type = barrier % 2 == 0 ? CHECKPOINT : SAVEPOINT;
			for (int channel = 0; channel < numChannels; channel++) {
				sendBarrier(barrier, type, (TestInputChannel) gate.getChannel(channel), gate);
			}
		}
		assertEquals(barriers, target.triggeredCheckpoints);
	}

	@Test
	public void testAlignedNeverTimeoutableCheckpoint() throws Exception {
		int numChannels = 2;
		int bufferSize = 1000;
		ValidatingCheckpointHandler target = new ValidatingCheckpointHandler();
		CheckpointedInputGate gate = buildGate(target, numChannels);

		long checkpointCreationTime = System.currentTimeMillis();
		// Aligned checkpoint that never times out
		Buffer neverTimeoutableCheckpoint = barrier(1, CHECKPOINT, checkpointCreationTime, Long.MAX_VALUE);
		send(neverTimeoutableCheckpoint, gate, 0);
		sendBuffer(bufferSize, gate, 1);
		assertEquals(0, target.getTriggeredCheckpointCounter());

		send(neverTimeoutableCheckpoint, gate, 1);
		assertEquals(1, target.getTriggeredCheckpointCounter());
	}

	@Test
	public void testTimeoutAlignment() throws Exception {
		int numChannels = 2;
		ValidatingCheckpointHandler target = new ValidatingCheckpointHandler();
		CheckpointedInputGate gate = buildRemoteInputGate(target, numChannels);

		testTimeoutBarrierOnTwoChannels(target, gate);
	}

	@Test
	public void testTimeoutAlignmentAfterProcessingBarrier() throws Exception {
		int numChannels = 3;
		ValidatingCheckpointHandler target = new ValidatingCheckpointHandler();
		CheckpointedInputGate gate = buildRemoteInputGate(target, numChannels);

		long alignmentTimeout = 10;
		long checkpointCreationTime = System.currentTimeMillis() - 2 * alignmentTimeout;
		Buffer neverTimeoutableCheckpoint = barrier(1, CHECKPOINT, checkpointCreationTime, Long.MAX_VALUE);

		RemoteInputChannel channel2 = (RemoteInputChannel) gate.getChannel(2);

		channel2.onBuffer(neverTimeoutableCheckpoint.retainBuffer(), 0, 0);
		while (gate.pollNext().isPresent()) {
		}

		assertEquals(0, target.getTriggeredCheckpointCounter());

		testTimeoutBarrierOnTwoChannels(target, gate);
	}

	private void testTimeoutBarrierOnTwoChannels(ValidatingCheckpointHandler target, CheckpointedInputGate gate) throws Exception {
		int bufferSize = 1000;
		long alignmentTimeout = 10;
		long checkpointCreationTime = System.currentTimeMillis() - 2 * alignmentTimeout;
		Buffer checkpointBarrier = barrier(1, CHECKPOINT, checkpointCreationTime, alignmentTimeout);
		Buffer buffer = TestBufferFactory.createBuffer(bufferSize);

		RemoteInputChannel channel0 = (RemoteInputChannel) gate.getChannel(0);
		RemoteInputChannel channel1 = (RemoteInputChannel) gate.getChannel(1);
		channel0.onBuffer(buffer.retainBuffer(), 0, 0);
		channel0.onBuffer(buffer.retainBuffer(), 1, 0);
		channel0.onBuffer(checkpointBarrier.retainBuffer(), 2, 0);
		channel1.onBuffer(buffer.retainBuffer(), 0, 0);
		channel1.onBuffer(checkpointBarrier.retainBuffer(), 1, 0);

		assertEquals(0, target.getTriggeredCheckpointCounter());
		// First announcements and prioritsed barriers
		List<AbstractEvent> events = new ArrayList<>();
		events.add(gate.pollNext().get().getEvent());
		Thread.sleep(alignmentTimeout * 2);
		events.add(gate.pollNext().get().getEvent());
		events.add(gate.pollNext().get().getEvent());
		events.add(gate.pollNext().get().getEvent());
		assertThat(events, containsInAnyOrder(
			instanceOf(EventAnnouncement.class),
			instanceOf(EventAnnouncement.class),
			instanceOf(CheckpointBarrier.class),
			instanceOf(CheckpointBarrier.class)));
		assertEquals(1, target.getTriggeredCheckpointCounter());
		assertThat(
			target.getTriggeredCheckpointOptions(),
			contains(CheckpointOptions.create(
				CHECKPOINT,
				CheckpointStorageLocationReference.getDefault(),
				true,
				true,
				0)));
		// Followed by overtaken buffers
		assertFalse(gate.pollNext().get().isEvent());
		assertFalse(gate.pollNext().get().isEvent());
		assertFalse(gate.pollNext().get().isEvent());
	}

	/**
	 * This test tries to make sure that the first time out happens after processing
	 * {@link EventAnnouncement} but before/during processing the first {@link CheckpointBarrier}.
	 */
	@Test
	public void testTimeoutAlignmentOnFirstBarrier() throws Exception {
		int numChannels = 2;
		ValidatingCheckpointHandler target = new ValidatingCheckpointHandler();
		CheckpointedInputGate gate = buildRemoteInputGate(target, numChannels);

		long alignmentTimeout = 100;
		long checkpointCreationTime = System.currentTimeMillis();
		Buffer checkpointBarrier = barrier(1, CHECKPOINT, checkpointCreationTime, alignmentTimeout);

		RemoteInputChannel channel0 = (RemoteInputChannel) gate.getChannel(0);
		RemoteInputChannel channel1 = (RemoteInputChannel) gate.getChannel(1);
		channel0.onBuffer(checkpointBarrier.retainBuffer(), 0, 0);
		channel1.onBuffer(checkpointBarrier.retainBuffer(), 0, 0);

		assertEquals(0, target.getTriggeredCheckpointCounter());
		// First announcements and prioritsed barriers
		List<AbstractEvent> events = new ArrayList<>();
		events.add(gate.pollNext().get().getEvent());
		events.add(gate.pollNext().get().getEvent());

		Thread.sleep(alignmentTimeout * 2);

		events.add(gate.pollNext().get().getEvent());
		assertThat(events, contains(
			instanceOf(EventAnnouncement.class),
			instanceOf(EventAnnouncement.class),
			instanceOf(CheckpointBarrier.class)));
		assertEquals(1, target.getTriggeredCheckpointCounter());
	}

	/**
	 * First we process aligned {@link CheckpointBarrier} and after that we receive an already unaligned
	 * {@link CheckpointBarrier}, that has timed out on an upstream task.
	 */
	@Test
	public void testTimeoutAlignmentOnUnalignedCheckpoint() throws Exception {
		int numChannels = 2;
		ValidatingCheckpointHandler target = new ValidatingCheckpointHandler();
		RecordingChannelStateWriter channelStateWriter = new RecordingChannelStateWriter();
		CheckpointedInputGate gate = buildRemoteInputGate(target, numChannels, channelStateWriter);

		long checkpointCreationTime = System.currentTimeMillis();
		Buffer alignedCheckpointBarrier = barrier(1, CHECKPOINT, checkpointCreationTime, Integer.MAX_VALUE);
		Buffer unalignedCheckpointBarrier = barrier(1, CHECKPOINT, checkpointCreationTime, 0);
		Buffer buffer = TestBufferFactory.createBuffer(1000);

		RemoteInputChannel channel0 = (RemoteInputChannel) gate.getChannel(0);
		RemoteInputChannel channel1 = (RemoteInputChannel) gate.getChannel(1);
		channel0.onBuffer(alignedCheckpointBarrier.retainBuffer(), 0, 0);

		List<AbstractEvent> events = new ArrayList<>();
		events.add(gate.pollNext().get().getEvent());
		events.add(gate.pollNext().get().getEvent());

		assertThat(events, contains(
			instanceOf(EventAnnouncement.class),
			instanceOf(CheckpointBarrier.class)));

		channel1.onBuffer(buffer.retainBuffer(), 0, 0);
		channel1.onBuffer(buffer.retainBuffer(), 1, 0);
		channel1.onBuffer(unalignedCheckpointBarrier.retainBuffer(), 2, 0);

		events.add(gate.pollNext().get().getEvent());

		assertThat(events, contains(
			instanceOf(EventAnnouncement.class),
			instanceOf(CheckpointBarrier.class),
			instanceOf(CheckpointBarrier.class)));

		assertEquals(channelStateWriter.getAddedInput().get(channel1.getChannelInfo()).size(), 2);
		assertEquals(1, target.getTriggeredCheckpointCounter());
	}

	@Test
	public void testTimeoutAlignmentConsistencyOnPreProcessBarrier() throws Exception {
		testTimeoutAlignmentConsistency(true, false, false);
	}

	@Test
	public void testTimeoutAlignmentConsistencyOnProcessBarrier() throws Exception {
		testTimeoutAlignmentConsistency(false, true, false);
	}

	@Test
	public void testTimeoutAlignmentConsistencyOnPostProcessBarrier() throws Exception {
		testTimeoutAlignmentConsistency(false, false, true);
	}

	public void testTimeoutAlignmentConsistency(
			boolean sleepBeforePreProcess,
			boolean sleepBeforeProcess,
			boolean sleepBeforePostProcess) throws Exception {
		ValidatingCheckpointHandler target = new ValidatingCheckpointHandler();
		SingleInputGate gate = new SingleInputGateBuilder().setNumberOfChannels(1).build();
		TestInputChannel channel0 = new TestInputChannel(gate, 0, false, true);
		gate.setInputChannels(channel0);

		RecordingChannelStateWriter channelStateWriter = new RecordingChannelStateWriter();
		AlternatingController controller = new AlternatingController(
			new AlignedController(gate),
			new UnalignedController(
				new TestSubtaskCheckpointCoordinator(channelStateWriter),
				gate));

		long checkpointCreationTime = System.currentTimeMillis();
		long alignmentTimeout = 10;
		CheckpointBarrier barrier = checkpointBarrier(1, CHECKPOINT, checkpointCreationTime, alignmentTimeout);

		InputChannelInfo channelInfo = channel0.getChannelInfo();

		controller.preProcessFirstBarrierOrAnnouncement(barrier);
		controller.barrierAnnouncement(channelInfo, barrier, 1);

		if (sleepBeforePreProcess) {
			Thread.sleep(alignmentTimeout * 2);
		}
		Optional<CheckpointBarrier> preProcessTrigger = controller.preProcessFirstBarrier(channelInfo, barrier);
		if (sleepBeforeProcess) {
			Thread.sleep(alignmentTimeout * 2);
		}
		Optional<CheckpointBarrier> processTrigger = controller.barrierReceived(channelInfo, barrier);
		if (sleepBeforePostProcess) {
			Thread.sleep(alignmentTimeout * 2);
		}
		Optional<CheckpointBarrier> postProcessTrigger = controller.postProcessLastBarrier(channelInfo, barrier);

		int triggeredCount = 0;
		boolean unalignedCheckpoint = false;
		if (preProcessTrigger.isPresent()) {
			triggeredCount++;
			unalignedCheckpoint = preProcessTrigger.get().getCheckpointOptions().isUnalignedCheckpoint();
			assertTrue(unalignedCheckpoint);
		}
		if (processTrigger.isPresent()) {
			triggeredCount++;
			unalignedCheckpoint = processTrigger.get().getCheckpointOptions().isUnalignedCheckpoint();
			assertTrue(unalignedCheckpoint);
		}
		if (postProcessTrigger.isPresent()) {
			triggeredCount++;
			unalignedCheckpoint = postProcessTrigger.get().getCheckpointOptions().isUnalignedCheckpoint();
		}

		assertEquals(
			String.format(
				"Checkpoint should be triggered exactly once, but [%s, %s, %s] was found instead",
				preProcessTrigger.isPresent(),
				processTrigger.isPresent(),
				postProcessTrigger.isPresent()),
			1,
			triggeredCount);

		if (unalignedCheckpoint) {
			// check that we can add output data if we are in unaligned checkpoint mode. In other words
			// if the state writer has been initialised correctly.
			assertEquals(barrier.getId(), channelStateWriter.getLastStartedCheckpointId());
		}
	}

	@Test
	public void testMetricsAlternation() throws Exception {
		int numChannels = 2;
		int bufferSize = 1000;
		ValidatingCheckpointHandler target = new ValidatingCheckpointHandler();
		CheckpointedInputGate gate = buildGate(target, numChannels);

		long startNanos = System.nanoTime();
		long checkpoint1CreationTime = System.currentTimeMillis() - 10;
		sendBarrier(1, checkpoint1CreationTime, CHECKPOINT, gate, 0);
		sendBuffer(bufferSize, gate, 0);
		sendBuffer(bufferSize, gate, 1);

		Thread.sleep(6);
		sendBarrier(1, checkpoint1CreationTime, CHECKPOINT, gate, 1);
		sendBuffer(bufferSize, gate, 0);

		assertMetrics(
			target,
			gate.getCheckpointBarrierHandler(),
			1L,
			startNanos,
			6_000_000L,
			10_000_000L,
			bufferSize * 2);

		startNanos = System.nanoTime();
		long checkpoint2CreationTime = System.currentTimeMillis() - 5;
		sendBarrier(2, checkpoint2CreationTime, SAVEPOINT, gate, 0);
		sendBuffer(bufferSize, gate, 1);

		assertMetrics(
			target,
			gate.getCheckpointBarrierHandler(),
			2L,
			startNanos,
			0L,
			5_000_000L,
			bufferSize * 2);
		Thread.sleep(5);
		sendBarrier(2, checkpoint2CreationTime, SAVEPOINT, gate, 1);
		sendBuffer(bufferSize, gate, 0);

		assertMetrics(
			target,
			gate.getCheckpointBarrierHandler(),
			2L,
			startNanos,
			5_000_000L,
			5_000_000L,
			bufferSize);

		startNanos = System.nanoTime();
		long checkpoint3CreationTime = System.currentTimeMillis() - 7;
		sendBarrier(3, checkpoint3CreationTime, CHECKPOINT, gate, 0);
		sendBuffer(bufferSize, gate, 0);
		sendBuffer(bufferSize, gate, 1);
		assertMetrics(
			target,
			gate.getCheckpointBarrierHandler(),
			3L,
			startNanos,
			0L,
			7_000_000L,
			-1L);
		Thread.sleep(10);
		sendBarrier(3, checkpoint2CreationTime, CHECKPOINT, gate, 1);
		assertMetrics(
			target,
			gate.getCheckpointBarrierHandler(),
			3L,
			startNanos,
			10_000_000L,
			7_000_000L,
			bufferSize * 2);
	}

	@Test
	public void testMetricsSingleChannel() throws Exception {
		int numChannels = 1;
		int bufferSize = 1000;
		ValidatingCheckpointHandler target = new ValidatingCheckpointHandler();
		CheckpointedInputGate gate = buildGate(target, numChannels);

		long checkpoint1CreationTime = System.currentTimeMillis() - 10;
		long startNanos = System.nanoTime();

		sendBuffer(bufferSize, gate, 0);
		sendBarrier(1, checkpoint1CreationTime, CHECKPOINT, gate, 0);
		sendBuffer(bufferSize, gate, 0);
		Thread.sleep(6);
		assertMetrics(
			target,
			gate.getCheckpointBarrierHandler(),
			1L,
			startNanos,
			0L,
			10_000_000L,
			0);

		long checkpoint2CreationTime = System.currentTimeMillis() - 5;
		startNanos = System.nanoTime();
		sendBuffer(bufferSize, gate, 0);
		sendBarrier(2, checkpoint2CreationTime, SAVEPOINT, gate, 0);
		sendBuffer(bufferSize, gate, 0);
		Thread.sleep(5);
		assertMetrics(
			target,
			gate.getCheckpointBarrierHandler(),
			2L,
			startNanos,
			0L,
			5_000_000L,
			0);
	}

	private void assertMetrics(
			ValidatingCheckpointHandler target,
			CheckpointBarrierHandler checkpointBarrierHandler,
			long latestCheckpointId,
			long alignmentDurationStartNanos,
			long alignmentDurationNanosMin,
			long startDelayNanos,
			long bytesProcessedDuringAlignment) {
		assertThat(checkpointBarrierHandler.getLatestCheckpointId(), equalTo(latestCheckpointId));
		long alignmentDurationNanos = checkpointBarrierHandler.getAlignmentDurationNanos();
		long expectedAlignmentDurationNanosMax = System.nanoTime() - alignmentDurationStartNanos;
		assertThat(alignmentDurationNanos, greaterThanOrEqualTo(alignmentDurationNanosMin));
		assertThat(alignmentDurationNanos, lessThanOrEqualTo(expectedAlignmentDurationNanosMax));
		assertThat(checkpointBarrierHandler.getCheckpointStartDelayNanos(), greaterThanOrEqualTo(startDelayNanos));
		assertThat(
				FutureUtils.getOrDefault(target.getLastBytesProcessedDuringAlignment(), -1L),
				equalTo(bytesProcessedDuringAlignment));
	}

	@Test
	public void testPreviousHandlerReset() throws Exception {
		SingleInputGate inputGate = new SingleInputGateBuilder().setNumberOfChannels(2).build();
		TestInputChannel[] channels = {
				new TestInputChannel(inputGate, 0),
				new TestInputChannel(inputGate, 1)
		};
		inputGate.setInputChannels(channels);
		ValidatingCheckpointHandler target = new ValidatingCheckpointHandler();
		SingleCheckpointBarrierHandler barrierHandler = barrierHandler(inputGate, target);

		for (int i = 0; i < 4; i++) {
			int channel = i % 2;
			CheckpointType type = channel == 0 ? SAVEPOINT : CHECKPOINT;
			target.setNextExpectedCheckpointId(-1);

			if (type.isSavepoint()) {
				channels[channel].setBlocked(true);
			}
			barrierHandler.processBarrier(new CheckpointBarrier(i, System.currentTimeMillis(), new CheckpointOptions(type, CheckpointStorageLocationReference.getDefault())), new InputChannelInfo(0, channel));
			if (type.isSavepoint()) {
				assertTrue(channels[channel].isBlocked());
				assertFalse(channels[(channel + 1) % 2].isBlocked());
			}
			else {
				assertFalse(channels[0].isBlocked());
				assertFalse(channels[1].isBlocked());
			}

			assertTrue(barrierHandler.isCheckpointPending());
			assertFalse(barrierHandler.getAllBarriersReceivedFuture(i).isDone());

			channels[0].setBlocked(false);
			channels[1].setBlocked(false);
		}
	}

	@Test
	public void testHasInflightDataBeforeProcessBarrier() throws Exception {
		SingleInputGate inputGate = new SingleInputGateBuilder().setNumberOfChannels(2).build();
		inputGate.setInputChannels(new TestInputChannel(inputGate, 0), new TestInputChannel(inputGate, 1));
		ValidatingCheckpointHandler target = new ValidatingCheckpointHandler();
		SingleCheckpointBarrierHandler barrierHandler = barrierHandler(inputGate, target);

		final long id = 1;
		barrierHandler.processBarrier(new CheckpointBarrier(id, System.currentTimeMillis(), new CheckpointOptions(CHECKPOINT, CheckpointStorageLocationReference.getDefault())), new InputChannelInfo(0, 0));

		assertFalse(barrierHandler.getAllBarriersReceivedFuture(id).isDone());
	}

	@Test
	public void testOutOfOrderBarrier() throws Exception {
		SingleInputGate inputGate = new SingleInputGateBuilder().setNumberOfChannels(2).build();
		TestInputChannel firstChannel = new TestInputChannel(inputGate, 0);
		TestInputChannel secondChannel = new TestInputChannel(inputGate, 1);
		inputGate.setInputChannels(firstChannel, secondChannel);
		ValidatingCheckpointHandler target = new ValidatingCheckpointHandler();
		SingleCheckpointBarrierHandler barrierHandler = barrierHandler(inputGate, target);

		long checkpointId = 10;
		long outOfOrderSavepointId = 5;

		barrierHandler.processBarrier(new CheckpointBarrier(checkpointId, System.currentTimeMillis(), new CheckpointOptions(CHECKPOINT, CheckpointStorageLocationReference.getDefault())), new InputChannelInfo(0, 0));
		secondChannel.setBlocked(true);
		barrierHandler.processBarrier(new CheckpointBarrier(outOfOrderSavepointId, System.currentTimeMillis(), new CheckpointOptions(SAVEPOINT, CheckpointStorageLocationReference.getDefault())), new InputChannelInfo(0, 1));

		assertEquals(checkpointId, barrierHandler.getLatestCheckpointId());
		assertFalse(secondChannel.isBlocked());
	}

	private void testBarrierHandling(CheckpointType checkpointType) throws Exception {
		final long barrierId = 123L;
		ValidatingCheckpointHandler target = new ValidatingCheckpointHandler();
		SingleInputGate gate = new SingleInputGateBuilder().setNumberOfChannels(2).build();
		TestInputChannel fast = new TestInputChannel(gate, 0, false, true);
		TestInputChannel slow = new TestInputChannel(gate, 1, false, true);
		gate.setInputChannels(fast, slow);
		SingleCheckpointBarrierHandler barrierHandler = barrierHandler(gate, target);
		CheckpointedInputGate checkpointedGate = new CheckpointedInputGate(gate, barrierHandler, new SyncMailboxExecutor());

		if (checkpointType.isSavepoint()) {
			fast.setBlocked(true);
			slow.setBlocked(true);
		}

		sendBarrier(barrierId, checkpointType, fast, checkpointedGate);
		assertEquals(checkpointType.isSavepoint(), target.triggeredCheckpoints.isEmpty());
		sendBarrier(barrierId, checkpointType, slow, checkpointedGate);

		assertEquals(singletonList(barrierId), target.triggeredCheckpoints);
		if (checkpointType.isSavepoint()) {
			for (InputChannel channel : gate.getInputChannels().values()) {
				assertFalse(
					String.format("channel %d should be resumed", channel.getChannelIndex()),
					((TestInputChannel) channel).isBlocked());
			}
		}
	}

	private void sendBarrier(long barrierId, long barrierCreationTime, CheckpointType type, CheckpointedInputGate gate, int channelId) throws Exception {
		send(barrier(barrierId, type, barrierCreationTime), gate, channelId);
	}

	private void send(Buffer buffer, CheckpointedInputGate gate, int channelId) throws Exception {
		TestInputChannel channel = (TestInputChannel) gate.getChannel(channelId);
		channel.read(buffer.retainBuffer());
		while (gate.pollNext().isPresent()) {
		}
	}

	private void sendBarrier(long barrierId, CheckpointType type, TestInputChannel channel, CheckpointedInputGate gate) throws Exception {
		channel.read(barrier(barrierId, type).retainBuffer());
		while (gate.pollNext().isPresent()) {
		}
	}

	private void sendBuffer(int bufferSize, CheckpointedInputGate gate, int channelId) throws Exception {
		TestInputChannel channel = (TestInputChannel) gate.getChannel(channelId);
		channel.read(TestBufferFactory.createBuffer(bufferSize));
		while (gate.pollNext().isPresent()) {
		}
	}

	private static SingleCheckpointBarrierHandler barrierHandler(SingleInputGate inputGate, AbstractInvokable target) {
		return barrierHandler(inputGate, target, new RecordingChannelStateWriter());
	}

	private static SingleCheckpointBarrierHandler barrierHandler(
			SingleInputGate inputGate,
			AbstractInvokable target,
			ChannelStateWriter stateWriter) {
		String taskName = "test";
		return new SingleCheckpointBarrierHandler(
			taskName,
			target,
			inputGate.getNumberOfInputChannels(),
			new AlternatingController(
				new AlignedController(inputGate),
				new UnalignedController(new TestSubtaskCheckpointCoordinator(stateWriter), inputGate)));
	}

	private Buffer barrier(long barrierId, CheckpointType checkpointType) throws IOException {
		return barrier(barrierId, checkpointType, System.currentTimeMillis());
	}

	private Buffer barrier(long barrierId, CheckpointType checkpointType, long barrierTimestamp) throws IOException {
		return barrier(barrierId, checkpointType, barrierTimestamp, 0);
	}

	private Buffer barrier(long barrierId, CheckpointType checkpointType, long barrierTimestamp, long alignmentTimeout) throws IOException {
		CheckpointBarrier checkpointBarrier = checkpointBarrier(
			barrierId,
			checkpointType,
			barrierTimestamp,
			alignmentTimeout);
		return toBuffer(
			checkpointBarrier,
			checkpointBarrier.getCheckpointOptions().isUnalignedCheckpoint());
	}

	private CheckpointBarrier checkpointBarrier(long barrierId, CheckpointType checkpointType, long barrierTimestamp, long alignmentTimeout) {
		CheckpointOptions options = CheckpointOptions.create(
			checkpointType,
			CheckpointStorageLocationReference.getDefault(),
			true,
			true,
			alignmentTimeout);
		return new CheckpointBarrier(
			barrierId,
			barrierTimestamp,
			options);
	}

	private static CheckpointedInputGate buildGate(AbstractInvokable target, int numChannels) {
		SingleInputGate gate = new SingleInputGateBuilder().setNumberOfChannels(numChannels).build();
		TestInputChannel[] channels = new TestInputChannel[numChannels];
		for (int i = 0; i < numChannels; i++) {
			channels[i] = new TestInputChannel(gate, i, false, true);
		}
		gate.setInputChannels(channels);
		return new CheckpointedInputGate(gate, barrierHandler(gate, target), new SyncMailboxExecutor());
	}

	private static CheckpointedInputGate buildRemoteInputGate(
			AbstractInvokable target,
			int numChannels) throws IOException {
		return buildRemoteInputGate(target, numChannels, new RecordingChannelStateWriter());
	}

	private static CheckpointedInputGate buildRemoteInputGate(
			AbstractInvokable target,
			int numChannels,
			ChannelStateWriter channelStateWriter) throws IOException {
		int maxUsedBuffers = 10;
		NetworkBufferPool networkBufferPool = new NetworkBufferPool(numChannels * maxUsedBuffers, 4096);
		SingleInputGate gate = new SingleInputGateBuilder()
			.setChannelFactory(InputChannelBuilder::buildRemoteChannel)
			.setNumberOfChannels(numChannels)
			.setSegmentProvider(networkBufferPool)
			.setBufferPoolFactory(networkBufferPool.createBufferPool(numChannels, maxUsedBuffers))
			.setChannelStateWriter(channelStateWriter)
			.build();
		gate.setup();
		gate.requestPartitions();
		// do not fire events automatically. If you need events, you should expose mailboxProcessor and
		// execute it step by step
		MailboxProcessor mailboxProcessor = new MailboxProcessor();
		return new CheckpointedInputGate(gate, barrierHandler(gate, target, channelStateWriter), mailboxProcessor.getMainMailboxExecutor());
	}
}
