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
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelBuilder;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGateBuilder;
import org.apache.flink.runtime.io.network.partition.consumer.TestInputChannel;
import org.apache.flink.runtime.io.network.util.TestBufferFactory;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.streaming.api.operators.SyncMailboxExecutor;
import org.apache.flink.streaming.runtime.tasks.TestSubtaskCheckpointCoordinator;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.apache.flink.runtime.checkpoint.CheckpointType.CHECKPOINT;
import static org.apache.flink.runtime.checkpoint.CheckpointType.SAVEPOINT;
import static org.apache.flink.runtime.io.network.api.serialization.EventSerializer.toBuffer;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;

/**
 * {@link AlternatingCheckpointBarrierHandler} test.
 */
public class AlternatingCheckpointBarrierHandlerTest {

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
		inputGate.setInputChannels(new TestInputChannel(inputGate, 0), new TestInputChannel(inputGate, 1));
		ValidatingCheckpointHandler target = new ValidatingCheckpointHandler();
		CheckpointBarrierAligner alignedHandler = new CheckpointBarrierAligner("test", target, inputGate);
		CheckpointBarrierUnaligner unalignedHandler = new CheckpointBarrierUnaligner(TestSubtaskCheckpointCoordinator.INSTANCE, "test", target, inputGate);
		AlternatingCheckpointBarrierHandler barrierHandler = new AlternatingCheckpointBarrierHandler(alignedHandler, unalignedHandler, target);

		for (int i = 0; i < 4; i++) {
			int channel = i % 2;
			CheckpointType type = channel == 0 ? CHECKPOINT : SAVEPOINT;
			target.setNextExpectedCheckpointId(-1);
			barrierHandler.processBarrier(new CheckpointBarrier(i, System.currentTimeMillis(), new CheckpointOptions(type, CheckpointStorageLocationReference.getDefault())), new InputChannelInfo(0, channel));
			assertEquals(type.isSavepoint(), alignedHandler.isCheckpointPending());
			assertNotEquals(alignedHandler.isCheckpointPending(), unalignedHandler.isCheckpointPending());

			if (!type.isSavepoint()) {
				assertFalse(barrierHandler.getAllBarriersReceivedFuture(i).isDone());
			}
		}
	}

	@Test
	public void testHasInflightDataBeforeProcessBarrier() throws Exception {
		SingleInputGate inputGate = new SingleInputGateBuilder().setNumberOfChannels(2).build();
		inputGate.setInputChannels(new TestInputChannel(inputGate, 0), new TestInputChannel(inputGate, 1));
		ValidatingCheckpointHandler target = new ValidatingCheckpointHandler();
		CheckpointBarrierAligner alignedHandler = new CheckpointBarrierAligner("test", target, inputGate);
		CheckpointBarrierUnaligner unalignedHandler = new CheckpointBarrierUnaligner(TestSubtaskCheckpointCoordinator.INSTANCE, "test", target, inputGate);
		AlternatingCheckpointBarrierHandler barrierHandler = new AlternatingCheckpointBarrierHandler(alignedHandler, unalignedHandler, target);

		final long id = 1;
		unalignedHandler.processBarrier(new CheckpointBarrier(id, System.currentTimeMillis(), new CheckpointOptions(CHECKPOINT, CheckpointStorageLocationReference.getDefault())), new InputChannelInfo(0, 0));

		assertFalse(barrierHandler.getAllBarriersReceivedFuture(id).isDone());
	}

	@Test
	public void testOutOfOrderBarrier() throws Exception {
		SingleInputGate inputGate = new SingleInputGateBuilder().setNumberOfChannels(2).build();
		inputGate.setInputChannels(new TestInputChannel(inputGate, 0), new TestInputChannel(inputGate, 1));
		ValidatingCheckpointHandler target = new ValidatingCheckpointHandler();
		CheckpointBarrierAligner alignedHandler = new CheckpointBarrierAligner("test", target, inputGate);
		CheckpointBarrierUnaligner unalignedHandler = new CheckpointBarrierUnaligner(TestSubtaskCheckpointCoordinator.INSTANCE, "test", target, inputGate);
		AlternatingCheckpointBarrierHandler barrierHandler = new AlternatingCheckpointBarrierHandler(alignedHandler, unalignedHandler, target);

		long checkpointId = 10;
		long outOfOrderSavepointId = 5;
		long initialAlignedCheckpointId = alignedHandler.getLatestCheckpointId();

		barrierHandler.processBarrier(new CheckpointBarrier(checkpointId, System.currentTimeMillis(), new CheckpointOptions(CHECKPOINT, CheckpointStorageLocationReference.getDefault())), new InputChannelInfo(0, 0));
		barrierHandler.processBarrier(new CheckpointBarrier(outOfOrderSavepointId, System.currentTimeMillis(), new CheckpointOptions(SAVEPOINT, CheckpointStorageLocationReference.getDefault())), new InputChannelInfo(0, 1));

		assertEquals(checkpointId, barrierHandler.getLatestCheckpointId());
		assertEquals(initialAlignedCheckpointId, alignedHandler.getLatestCheckpointId());
	}

	@Test
	public void testEndOfPartition() throws Exception {
		int totalChannels = 5;
		int closedChannels = 2;
		SingleInputGate inputGate = new SingleInputGateBuilder()
			.setNumberOfChannels(totalChannels)
			.setChannelFactory(InputChannelBuilder::buildLocalChannel)
			.build();
		ValidatingCheckpointHandler target = new ValidatingCheckpointHandler();
		CheckpointBarrierAligner alignedHandler = new CheckpointBarrierAligner("test", target, inputGate);
		CheckpointBarrierUnaligner unalignedHandler = new CheckpointBarrierUnaligner(TestSubtaskCheckpointCoordinator.INSTANCE, "test", target, inputGate);
		AlternatingCheckpointBarrierHandler barrierHandler = new AlternatingCheckpointBarrierHandler(alignedHandler, unalignedHandler, target);
		for (int i = 0; i < closedChannels; i++) {
			barrierHandler.processEndOfPartition();
		}
		assertEquals(totalChannels - closedChannels, alignedHandler.getNumOpenChannels());
		assertEquals(totalChannels - closedChannels, unalignedHandler.getNumOpenChannels());
	}

	private void testBarrierHandling(CheckpointType checkpointType) throws Exception {
		final long barrierId = 123L;
		ValidatingCheckpointHandler target = new ValidatingCheckpointHandler();
		SingleInputGate gate = new SingleInputGateBuilder().setNumberOfChannels(2).build();
		TestInputChannel fast = new TestInputChannel(gate, 0, false, true);
		TestInputChannel slow = new TestInputChannel(gate, 1, false, true);
		gate.setInputChannels(fast, slow);
		AlternatingCheckpointBarrierHandler barrierHandler = barrierHandler(gate, target);
		CheckpointedInputGate checkpointedGate = new CheckpointedInputGate(gate, barrierHandler, new SyncMailboxExecutor());

		sendBarrier(barrierId, checkpointType, fast, checkpointedGate);

		assertEquals(checkpointType.isSavepoint(), target.triggeredCheckpoints.isEmpty());

		sendBarrier(barrierId, checkpointType, slow, checkpointedGate);

		assertEquals(singletonList(barrierId), target.triggeredCheckpoints);
		for (InputChannel channel : gate.getInputChannels().values()) {
			assertEquals(
				String.format("channel %d should be resumed", channel.getChannelIndex()),
				checkpointType.isSavepoint(),
				((TestInputChannel) channel).isResumed());
		}
	}

	private void sendBarrier(long barrierId, long barrierCreationTime, CheckpointType type, CheckpointedInputGate gate, int channelId) throws Exception {
		TestInputChannel channel = (TestInputChannel) gate.getChannel(channelId);
		channel.read(barrier(barrierId, type, barrierCreationTime).retainBuffer());
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

	private static AlternatingCheckpointBarrierHandler barrierHandler(SingleInputGate inputGate, AbstractInvokable target) {
		String taskName = "test";
		return new AlternatingCheckpointBarrierHandler(
			new CheckpointBarrierAligner(taskName, target, inputGate),
			new CheckpointBarrierUnaligner(TestSubtaskCheckpointCoordinator.INSTANCE, taskName, target, inputGate),
			target);
	}

	private Buffer barrier(long barrierId, CheckpointType checkpointType) throws IOException {
		return barrier(barrierId, checkpointType, System.currentTimeMillis());
	}

	private Buffer barrier(long barrierId, CheckpointType checkpointType, long barrierTimestamp) throws IOException {
		return toBuffer(
			new CheckpointBarrier(
				barrierId,
				barrierTimestamp,
				new CheckpointOptions(checkpointType, CheckpointStorageLocationReference.getDefault(), true, true)),
			true);
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

}
