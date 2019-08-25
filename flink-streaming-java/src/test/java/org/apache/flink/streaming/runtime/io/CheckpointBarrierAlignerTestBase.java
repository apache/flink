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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.After;
import org.junit.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

/**
 * Tests for the behavior of the {@link CheckpointBarrierAligner} with different {@link BufferStorage} implements.
 */
public abstract class CheckpointBarrierAlignerTestBase {

	protected static final int PAGE_SIZE = 512;

	private static final Random RND = new Random();

	private static int sizeCounter = 1;

	CheckpointedInputGate inputGate;

	protected CheckpointedInputGate createBarrierBuffer(
		int numberOfChannels,
		BufferOrEvent[] sequence,
		@Nullable AbstractInvokable toNotify) throws IOException {
		MockInputGate gate = new MockInputGate(numberOfChannels, Arrays.asList(sequence));
		return createBarrierBuffer(gate, toNotify);
	}

	protected CheckpointedInputGate createBarrierBuffer(int numberOfChannels, BufferOrEvent[] sequence) throws IOException {
		return createBarrierBuffer(numberOfChannels, sequence, null);
	}

	protected CheckpointedInputGate createBarrierBuffer(InputGate gate) throws IOException {
		return createBarrierBuffer(gate, null);
	}

	abstract CheckpointedInputGate createBarrierBuffer(InputGate gate, @Nullable AbstractInvokable toNotify) throws IOException;

	abstract void validateAlignmentBuffered(long actualBytesBuffered, BufferOrEvent... sequence);

	@After
	public void ensureEmpty() throws Exception {
		assertFalse(inputGate.pollNext().isPresent());
		assertTrue(inputGate.isFinished());
		assertTrue(inputGate.isEmpty());

		inputGate.cleanup();
	}

	// ------------------------------------------------------------------------
	//  Tests
	// ------------------------------------------------------------------------

	/**
	 * Validates that the buffer behaves correctly if no checkpoint barriers come,
	 * for a single input channel.
	 */
	@Test
	public void testSingleChannelNoBarriers() throws Exception {
		BufferOrEvent[] sequence = {
			createBuffer(0), createBuffer(0),
			createBuffer(0), createEndOfPartition(0)
		};
		inputGate = createBarrierBuffer(1, sequence);

		for (BufferOrEvent boe : sequence) {
			assertEquals(boe, inputGate.pollNext().get());
		}

		assertEquals(0L, inputGate.getAlignmentDurationNanos());
	}

	/**
	 * Validates that the buffer behaves correctly if no checkpoint barriers come,
	 * for an input with multiple input channels.
	 */
	@Test
	public void testMultiChannelNoBarriers() throws Exception {
		BufferOrEvent[] sequence = {
			createBuffer(2), createBuffer(2), createBuffer(0),
			createBuffer(1), createBuffer(0), createEndOfPartition(0),
			createBuffer(3), createBuffer(1), createEndOfPartition(3),
			createBuffer(1), createEndOfPartition(1), createBuffer(2), createEndOfPartition(2)
		};
		inputGate = createBarrierBuffer(4, sequence);

		for (BufferOrEvent boe : sequence) {
			assertEquals(boe, inputGate.pollNext().get());
		}

		assertEquals(0L, inputGate.getAlignmentDurationNanos());
	}

	/**
	 * Validates that the buffer preserved the order of elements for a
	 * input with a single input channel, and checkpoint events.
	 */
	@Test
	public void testSingleChannelWithBarriers() throws Exception {
		BufferOrEvent[] sequence = {
			createBuffer(0), createBuffer(0), createBuffer(0),
			createBarrier(1, 0),
			createBuffer(0), createBuffer(0), createBuffer(0), createBuffer(0),
			createBarrier(2, 0), createBarrier(3, 0),
			createBuffer(0), createBuffer(0),
			createBarrier(4, 0), createBarrier(5, 0), createBarrier(6, 0),
			createBuffer(0), createEndOfPartition(0)
		};
		ValidatingCheckpointHandler handler = new ValidatingCheckpointHandler();
		inputGate = createBarrierBuffer(1, sequence, handler);

		handler.setNextExpectedCheckpointId(1L);

		for (BufferOrEvent boe : sequence) {
			if (boe.isBuffer() || boe.getEvent().getClass() != CheckpointBarrier.class) {
				assertEquals(boe, inputGate.pollNext().get());
			}
		}
	}

	/**
	 * Validates that the buffer correctly aligns the streams for inputs with
	 * multiple input channels, by buffering and blocking certain inputs.
	 */
	@Test
	public void testMultiChannelWithBarriers() throws Exception {
		BufferOrEvent[] sequence = {
			// checkpoint with blocked data
			createBuffer(0), createBuffer(2), createBuffer(0),
			createBarrier(1, 1), createBarrier(1, 2),
			createBuffer(2), createBuffer(1), createBuffer(0),
			createBarrier(1, 0),

			// checkpoint without blocked data
			createBuffer(0), createBuffer(0), createBuffer(1), createBuffer(1), createBuffer(2),
			createBarrier(2, 0), createBarrier(2, 1), createBarrier(2, 2),

			// checkpoint with data only from one channel
			createBuffer(2), createBuffer(2),
			createBarrier(3, 2),
			createBuffer(2), createBuffer(2),
			createBarrier(3, 0), createBarrier(3, 1),

			// empty checkpoint
			createBarrier(4, 1), createBarrier(4, 2), createBarrier(4, 0),

			// checkpoint with blocked data in mixed order
			createBuffer(0), createBuffer(2), createBuffer(0),
			createBarrier(5, 1),
			createBuffer(2), createBuffer(0), createBuffer(2), createBuffer(1),
			createBarrier(5, 2),
			createBuffer(1), createBuffer(0), createBuffer(2), createBuffer(1),
			createBarrier(5, 0),

			// some trailing data
			createBuffer(0),
			createEndOfPartition(0), createEndOfPartition(1), createEndOfPartition(2)
		};
		ValidatingCheckpointHandler handler = new ValidatingCheckpointHandler();
		inputGate = createBarrierBuffer(3, sequence, handler);

		handler.setNextExpectedCheckpointId(1L);

		// pre checkpoint 1
		check(sequence[0], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[1], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[2], inputGate.pollNext().get(), PAGE_SIZE);
		assertEquals(1L, handler.getNextExpectedCheckpointId());

		long startTs = System.nanoTime();

		// blocking while aligning for checkpoint 1
		check(sequence[7], inputGate.pollNext().get(), PAGE_SIZE);
		assertEquals(1L, handler.getNextExpectedCheckpointId());

		// checkpoint 1 done, returning buffered data
		check(sequence[5], inputGate.pollNext().get(), PAGE_SIZE);
		assertEquals(2L, handler.getNextExpectedCheckpointId());
		validateAlignmentTime(startTs, inputGate.getAlignmentDurationNanos());
		validateAlignmentBuffered(handler.getLastReportedBytesBufferedInAlignment(), sequence[5], sequence[6]);

		check(sequence[6], inputGate.pollNext().get(), PAGE_SIZE);

		// pre checkpoint 2
		check(sequence[9], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[10], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[11], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[12], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[13], inputGate.pollNext().get(), PAGE_SIZE);
		assertEquals(2L, handler.getNextExpectedCheckpointId());

		// checkpoint 2 barriers come together
		startTs = System.nanoTime();
		check(sequence[17], inputGate.pollNext().get(), PAGE_SIZE);
		assertEquals(3L, handler.getNextExpectedCheckpointId());
		validateAlignmentTime(startTs, inputGate.getAlignmentDurationNanos());
		validateAlignmentBuffered(handler.getLastReportedBytesBufferedInAlignment());

		check(sequence[18], inputGate.pollNext().get(), PAGE_SIZE);

		// checkpoint 3 starts, data buffered
		check(sequence[20], inputGate.pollNext().get(), PAGE_SIZE);
		validateAlignmentBuffered(handler.getLastReportedBytesBufferedInAlignment(), sequence[20], sequence[21]);
		assertEquals(4L, handler.getNextExpectedCheckpointId());
		check(sequence[21], inputGate.pollNext().get(), PAGE_SIZE);

		// checkpoint 4 happens without extra data

		// pre checkpoint 5
		check(sequence[27], inputGate.pollNext().get(), PAGE_SIZE);

		validateAlignmentBuffered(handler.getLastReportedBytesBufferedInAlignment());
		assertEquals(5L, handler.getNextExpectedCheckpointId());

		check(sequence[28], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[29], inputGate.pollNext().get(), PAGE_SIZE);

		// checkpoint 5 aligning
		check(sequence[31], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[32], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[33], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[37], inputGate.pollNext().get(), PAGE_SIZE);

		// buffered data from checkpoint 5 alignment
		check(sequence[34], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[36], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[38], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[39], inputGate.pollNext().get(), PAGE_SIZE);

		// remaining data
		check(sequence[41], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[42], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[43], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[44], inputGate.pollNext().get(), PAGE_SIZE);

		validateAlignmentBuffered(handler.getLastReportedBytesBufferedInAlignment(),
			sequence[34], sequence[36], sequence[38], sequence[39]);
	}

	@Test
	public void testMultiChannelTrailingBlockedData() throws Exception {
		BufferOrEvent[] sequence = {
			createBuffer(0), createBuffer(1), createBuffer(2),
			createBarrier(1, 1), createBarrier(1, 2), createBarrier(1, 0),

			createBuffer(2), createBuffer(1), createBuffer(0),
			createBarrier(2, 1),
			createBuffer(1), createBuffer(1), createEndOfPartition(1), createBuffer(0), createBuffer(2),
			createBarrier(2, 2),
			createBuffer(2), createEndOfPartition(2), createBuffer(0), createEndOfPartition(0)
		};
		ValidatingCheckpointHandler handler = new ValidatingCheckpointHandler();
		inputGate = createBarrierBuffer(3, sequence, handler);

		handler.setNextExpectedCheckpointId(1L);

		// pre-checkpoint 1
		check(sequence[0], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[1], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[2], inputGate.pollNext().get(), PAGE_SIZE);
		assertEquals(1L, handler.getNextExpectedCheckpointId());

		// pre-checkpoint 2
		check(sequence[6], inputGate.pollNext().get(), PAGE_SIZE);
		assertEquals(2L, handler.getNextExpectedCheckpointId());
		check(sequence[7], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[8], inputGate.pollNext().get(), PAGE_SIZE);

		// checkpoint 2 alignment
		long startTs = System.nanoTime();
		check(sequence[13], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[14], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[18], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[19], inputGate.pollNext().get(), PAGE_SIZE);
		validateAlignmentTime(startTs, inputGate.getAlignmentDurationNanos());

		// end of stream: remaining buffered contents
		check(sequence[10], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[11], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[12], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[16], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[17], inputGate.pollNext().get(), PAGE_SIZE);
	}

	/**
	 * Validates that the buffer correctly aligns the streams in cases
	 * where some channels receive barriers from multiple successive checkpoints
	 * before the pending checkpoint is complete.
	 */
	@Test
	public void testMultiChannelWithQueuedFutureBarriers() throws Exception{
		BufferOrEvent[] sequence = {
			// checkpoint 1 - with blocked data
			createBuffer(0), createBuffer(2), createBuffer(0),
			createBarrier(1, 1), createBarrier(1, 2),
			createBuffer(2), createBuffer(1), createBuffer(0),
			createBarrier(1, 0),
			createBuffer(1), createBuffer(0),

			// checkpoint 2 - where future checkpoint barriers come before
			// the current checkpoint is complete
			createBarrier(2, 1),
			createBuffer(1), createBuffer(2), createBarrier(2, 0),
			createBarrier(3, 0), createBuffer(0),
			createBarrier(3, 1), createBuffer(0), createBuffer(1), createBuffer(2),
			createBarrier(4, 1), createBuffer(1), createBuffer(2),

			// complete checkpoint 2, send a barrier for checkpoints 4 and 5
			createBarrier(2, 2),
			createBuffer(2), createBuffer(1), createBuffer(2), createBuffer(0),
			createBarrier(4, 0),
			createBuffer(2), createBuffer(1), createBuffer(2), createBuffer(0),
			createBarrier(5, 1),

			// complete checkpoint 3
			createBarrier(3, 2),
			createBuffer(2), createBuffer(1), createBuffer(2), createBuffer(0),
			createBarrier(6, 1),

			// complete checkpoint 4, checkpoint 5 remains not fully triggered
			createBarrier(4, 2),
			createBuffer(2),
			createBuffer(1), createEndOfPartition(1),
			createBuffer(2), createEndOfPartition(2),
			createBuffer(0), createEndOfPartition(0)
		};
		ValidatingCheckpointHandler handler = new ValidatingCheckpointHandler();
		inputGate = createBarrierBuffer(3, sequence, handler);

		handler.setNextExpectedCheckpointId(1L);

		// around checkpoint 1
		check(sequence[0], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[1], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[2], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[7], inputGate.pollNext().get(), PAGE_SIZE);

		check(sequence[5], inputGate.pollNext().get(), PAGE_SIZE);
		assertEquals(2L, handler.getNextExpectedCheckpointId());
		check(sequence[6], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[9], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[10], inputGate.pollNext().get(), PAGE_SIZE);

		// alignment of checkpoint 2 - buffering also some barriers for
		// checkpoints 3 and 4
		long startTs = System.nanoTime();
		check(sequence[13], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[20], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[23], inputGate.pollNext().get(), PAGE_SIZE);

		// checkpoint 2 completed
		check(sequence[12], inputGate.pollNext().get(), PAGE_SIZE);
		validateAlignmentTime(startTs, inputGate.getAlignmentDurationNanos());
		check(sequence[25], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[27], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[30], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[32], inputGate.pollNext().get(), PAGE_SIZE);

		// checkpoint 3 completed (emit buffered)
		check(sequence[16], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[18], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[19], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[28], inputGate.pollNext().get(), PAGE_SIZE);

		// past checkpoint 3
		check(sequence[36], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[38], inputGate.pollNext().get(), PAGE_SIZE);

		// checkpoint 4 completed (emit buffered)
		check(sequence[22], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[26], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[31], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[33], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[39], inputGate.pollNext().get(), PAGE_SIZE);

		// past checkpoint 4, alignment for checkpoint 5
		check(sequence[42], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[45], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[46], inputGate.pollNext().get(), PAGE_SIZE);

		// abort checkpoint 5 (end of partition)
		check(sequence[37], inputGate.pollNext().get(), PAGE_SIZE);

		// start checkpoint 6 alignment
		check(sequence[47], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[48], inputGate.pollNext().get(), PAGE_SIZE);

		// end of input, emit remainder
		check(sequence[43], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[44], inputGate.pollNext().get(), PAGE_SIZE);
	}

	/**
	 * Validates that the buffer skips over the current checkpoint if it
	 * receives a barrier from a later checkpoint on a non-blocked input.
	 */
	@Test
	public void testMultiChannelSkippingCheckpoints() throws Exception {
		BufferOrEvent[] sequence = {
			// checkpoint 1 - with blocked data
			createBuffer(0), createBuffer(2), createBuffer(0),
			createBarrier(1, 1), createBarrier(1, 2),
			createBuffer(2), createBuffer(1), createBuffer(0),
			createBarrier(1, 0),
			createBuffer(1), createBuffer(0),

			// checkpoint 2 will not complete: pre-mature barrier from checkpoint 3
			createBarrier(2, 1),
			createBuffer(1), createBuffer(2),
			createBarrier(2, 0),
			createBuffer(2), createBuffer(0),
			createBarrier(3, 2),

			createBuffer(2),
			createBuffer(1), createEndOfPartition(1),
			createBuffer(2), createEndOfPartition(2),
			createBuffer(0), createEndOfPartition(0)
		};
		AbstractInvokable toNotify = mock(AbstractInvokable.class);
		inputGate = createBarrierBuffer(3, sequence, toNotify);

		long startTs;

		// initial data
		check(sequence[0], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[1], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[2], inputGate.pollNext().get(), PAGE_SIZE);

		// align checkpoint 1
		startTs = System.nanoTime();
		check(sequence[7], inputGate.pollNext().get(), PAGE_SIZE);
		assertEquals(1L, inputGate.getLatestCheckpointId());

		// checkpoint done - replay buffered
		check(sequence[5], inputGate.pollNext().get(), PAGE_SIZE);
		validateAlignmentTime(startTs, inputGate.getAlignmentDurationNanos());
		verify(toNotify).triggerCheckpointOnBarrier(argThat(new CheckpointMatcher(1L)), any(CheckpointOptions.class), any(CheckpointMetrics.class));
		check(sequence[6], inputGate.pollNext().get(), PAGE_SIZE);

		check(sequence[9], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[10], inputGate.pollNext().get(), PAGE_SIZE);

		// alignment of checkpoint 2
		startTs = System.nanoTime();
		check(sequence[13], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[15], inputGate.pollNext().get(), PAGE_SIZE);

		// checkpoint 2 aborted, checkpoint 3 started
		check(sequence[12], inputGate.pollNext().get(), PAGE_SIZE);
		assertEquals(3L, inputGate.getLatestCheckpointId());
		validateAlignmentTime(startTs, inputGate.getAlignmentDurationNanos());
		verify(toNotify).abortCheckpointOnBarrier(eq(2L),
			argThat(new CheckpointExceptionMatcher(CheckpointFailureReason.CHECKPOINT_DECLINED_SUBSUMED)));
		check(sequence[16], inputGate.pollNext().get(), PAGE_SIZE);

		// checkpoint 3 alignment in progress
		check(sequence[19], inputGate.pollNext().get(), PAGE_SIZE);

		// checkpoint 3 aborted (end of partition)
		check(sequence[20], inputGate.pollNext().get(), PAGE_SIZE);
		verify(toNotify).abortCheckpointOnBarrier(eq(3L),
			argThat(new CheckpointExceptionMatcher(CheckpointFailureReason.CHECKPOINT_DECLINED_INPUT_END_OF_STREAM)));

		// replay buffered data from checkpoint 3
		check(sequence[18], inputGate.pollNext().get(), PAGE_SIZE);

		// all the remaining messages
		check(sequence[21], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[22], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[23], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[24], inputGate.pollNext().get(), PAGE_SIZE);
	}

	/**
	 * Validates that the buffer skips over the current checkpoint if it
	 * receives a barrier from a later checkpoint on a non-blocked input.
	 */
	@Test
	public void testMultiChannelJumpingOverCheckpoint() throws Exception {
		BufferOrEvent[] sequence = {
			// checkpoint 1 - with blocked data
			createBuffer(0), createBuffer(2), createBuffer(0),
			createBarrier(1, 1), createBarrier(1, 2),
			createBuffer(2), createBuffer(1), createBuffer(0),
			createBarrier(1, 0),
			createBuffer(1), createBuffer(0),

			// checkpoint 2 will not complete: pre-mature barrier from checkpoint 3
			createBarrier(2, 1),
			createBuffer(1), createBuffer(2),
			createBarrier(2, 0),
			createBuffer(2), createBuffer(0),
			createBarrier(3, 1),
			createBuffer(1), createBuffer(2),
			createBarrier(3, 0),
			createBuffer(2), createBuffer(0),
			createBarrier(4, 2),

			createBuffer(2),
			createBuffer(1), createEndOfPartition(1),
			createBuffer(2), createEndOfPartition(2),
			createBuffer(0), createEndOfPartition(0)
		};
		ValidatingCheckpointHandler handler = new ValidatingCheckpointHandler();
		inputGate = createBarrierBuffer(3, sequence, handler);

		handler.setNextExpectedCheckpointId(1L);

		// checkpoint 1
		check(sequence[0], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[1], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[2], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[7], inputGate.pollNext().get(), PAGE_SIZE);
		assertEquals(1L, inputGate.getLatestCheckpointId());

		check(sequence[5], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[6], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[9], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[10], inputGate.pollNext().get(), PAGE_SIZE);

		// alignment of checkpoint 2
		check(sequence[13], inputGate.pollNext().get(), PAGE_SIZE);
		assertEquals(2L, inputGate.getLatestCheckpointId());
		check(sequence[15], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[19], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[21], inputGate.pollNext().get(), PAGE_SIZE);

		long startTs = System.nanoTime();

		// checkpoint 2 aborted, checkpoint 4 started. replay buffered
		check(sequence[12], inputGate.pollNext().get(), PAGE_SIZE);
		assertEquals(4L, inputGate.getLatestCheckpointId());
		check(sequence[16], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[18], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[22], inputGate.pollNext().get(), PAGE_SIZE);

		// align checkpoint 4 remainder
		check(sequence[25], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[26], inputGate.pollNext().get(), PAGE_SIZE);

		validateAlignmentTime(startTs, inputGate.getAlignmentDurationNanos());

		// checkpoint 4 aborted (due to end of partition)
		check(sequence[24], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[27], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[28], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[29], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[30], inputGate.pollNext().get(), PAGE_SIZE);
	}

	/**
	 * Validates that the buffer skips over a later checkpoint if it
	 * receives a barrier from an even later checkpoint on a blocked input.
	 */
	@Test
	public void testMultiChannelSkippingCheckpointsViaBlockedInputs() throws Exception {
		BufferOrEvent[] sequence = {
			// checkpoint 1 - with blocked data
			createBuffer(0), createBuffer(2), createBuffer(0),
			createBarrier(1, 1), createBarrier(1, 2),
			createBuffer(2), createBuffer(1), createBuffer(0),
			createBarrier(1, 0),
			createBuffer(1), createBuffer(0),

			// checkpoint 2 will not complete: pre-mature barrier from checkpoint 3
			createBarrier(2, 1),
			createBuffer(1), createBuffer(2),
			createBarrier(2, 0),
			createBuffer(1), createBuffer(0),

			createBarrier(3, 0), // queued barrier on blocked input
			createBuffer(0),

			createBarrier(4, 1), // pre-mature barrier on blocked input
			createBuffer(1),
			createBuffer(0),
			createBuffer(2),

			// complete checkpoint 2
			createBarrier(2, 2),
			createBuffer(0),

			createBarrier(3, 2), // should be ignored
			createBuffer(2),
			createBarrier(4, 0),
			createBuffer(0), createBuffer(1), createBuffer(2),
			createBarrier(4, 2),

			createBuffer(1), createEndOfPartition(1),
			createBuffer(2), createEndOfPartition(2),
			createBuffer(0), createEndOfPartition(0)
		};
		inputGate = createBarrierBuffer(3, sequence);

		// checkpoint 1
		check(sequence[0], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[1], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[2], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[7], inputGate.pollNext().get(), PAGE_SIZE);
		assertEquals(1L, inputGate.getLatestCheckpointId());
		check(sequence[5], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[6], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[9], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[10], inputGate.pollNext().get(), PAGE_SIZE);

		// alignment of checkpoint 2
		check(sequence[13], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[22], inputGate.pollNext().get(), PAGE_SIZE);
		assertEquals(2L, inputGate.getLatestCheckpointId());

		// checkpoint 2 completed
		check(sequence[12], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[15], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[16], inputGate.pollNext().get(), PAGE_SIZE);

		// checkpoint 3 skipped, alignment for 4 started
		check(sequence[18], inputGate.pollNext().get(), PAGE_SIZE);
		assertEquals(4L, inputGate.getLatestCheckpointId());
		check(sequence[21], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[24], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[26], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[30], inputGate.pollNext().get(), PAGE_SIZE);

		// checkpoint 4 completed
		check(sequence[20], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[28], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[29], inputGate.pollNext().get(), PAGE_SIZE);

		check(sequence[32], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[33], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[34], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[35], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[36], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[37], inputGate.pollNext().get(), PAGE_SIZE);
	}

	@Test
	public void testMissingCancellationBarriers() throws Exception {
		BufferOrEvent[] sequence = {
			createBarrier(1L, 0),
			createCancellationBarrier(2L, 0),
			createCancellationBarrier(3L, 0),
			createCancellationBarrier(3L, 1),
			createBuffer(0)
		};
		AbstractInvokable validator = new CheckpointSequenceValidator(-3);
		inputGate = createBarrierBuffer(2, sequence, validator);

		for (BufferOrEvent boe : sequence) {
			if (boe.isBuffer() || (boe.getEvent().getClass() != CheckpointBarrier.class && boe.getEvent().getClass() != CancelCheckpointMarker.class)) {
				assertEquals(boe, inputGate.pollNext().get());
			}
		}
	}

	@Test
	public void testEarlyCleanup() throws Exception {
		BufferOrEvent[] sequence = {
			createBuffer(0), createBuffer(1), createBuffer(2),
			createBarrier(1, 1), createBarrier(1, 2), createBarrier(1, 0),

			createBuffer(2), createBuffer(1), createBuffer(0),
			createBarrier(2, 1),
			createBuffer(1), createBuffer(1), createEndOfPartition(1), createBuffer(0), createBuffer(2),
			createBarrier(2, 2),
			createBuffer(2), createEndOfPartition(2), createBuffer(0), createEndOfPartition(0)
		};
		ValidatingCheckpointHandler handler = new ValidatingCheckpointHandler();
		inputGate = createBarrierBuffer(3, sequence, handler);

		handler.setNextExpectedCheckpointId(1L);

		// pre-checkpoint 1
		check(sequence[0], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[1], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[2], inputGate.pollNext().get(), PAGE_SIZE);
		assertEquals(1L, handler.getNextExpectedCheckpointId());

		// pre-checkpoint 2
		check(sequence[6], inputGate.pollNext().get(), PAGE_SIZE);
		assertEquals(2L, handler.getNextExpectedCheckpointId());
		check(sequence[7], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[8], inputGate.pollNext().get(), PAGE_SIZE);

		// checkpoint 2 alignment
		check(sequence[13], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[14], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[18], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[19], inputGate.pollNext().get(), PAGE_SIZE);

		// drain buffer
		inputGate.pollNext().get();
		inputGate.pollNext().get();
		inputGate.pollNext().get();
		inputGate.pollNext().get();
		inputGate.pollNext().get();
	}

	@Test
	public void testStartAlignmentWithClosedChannels() throws Exception {
		BufferOrEvent[] sequence = {
			// close some channels immediately
			createEndOfPartition(2), createEndOfPartition(1),

			// checkpoint without blocked data
			createBuffer(0), createBuffer(0), createBuffer(3),
			createBarrier(2, 3), createBarrier(2, 0),

			// checkpoint with blocked data
			createBuffer(3), createBuffer(0),
			createBarrier(3, 3),
			createBuffer(3), createBuffer(0),
			createBarrier(3, 0),

			// empty checkpoint
			createBarrier(4, 0), createBarrier(4, 3),

			// some data, one channel closes
			createBuffer(0), createBuffer(0), createBuffer(3),
			createEndOfPartition(0),

			// checkpoint on last remaining channel
			createBuffer(3),
			createBarrier(5, 3),
			createBuffer(3),
			createEndOfPartition(3)
		};
		inputGate = createBarrierBuffer(4, sequence);

		// pre checkpoint 2
		check(sequence[0], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[1], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[2], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[3], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[4], inputGate.pollNext().get(), PAGE_SIZE);

		// checkpoint 3 alignment
		check(sequence[7], inputGate.pollNext().get(), PAGE_SIZE);
		assertEquals(2L, inputGate.getLatestCheckpointId());
		check(sequence[8], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[11], inputGate.pollNext().get(), PAGE_SIZE);

		// checkpoint 3 buffered
		check(sequence[10], inputGate.pollNext().get(), PAGE_SIZE);
		assertEquals(3L, inputGate.getLatestCheckpointId());

		// after checkpoint 4
		check(sequence[15], inputGate.pollNext().get(), PAGE_SIZE);
		assertEquals(4L, inputGate.getLatestCheckpointId());
		check(sequence[16], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[17], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[18], inputGate.pollNext().get(), PAGE_SIZE);

		check(sequence[19], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[21], inputGate.pollNext().get(), PAGE_SIZE);
		assertEquals(5L, inputGate.getLatestCheckpointId());
		check(sequence[22], inputGate.pollNext().get(), PAGE_SIZE);
	}

	@Test
	public void testEndOfStreamWhileCheckpoint() throws Exception {
		BufferOrEvent[] sequence = {
			// one checkpoint
			createBarrier(1, 0), createBarrier(1, 1), createBarrier(1, 2),

			// some buffers
			createBuffer(0), createBuffer(0), createBuffer(2),

			// start the checkpoint that will be incomplete
			createBarrier(2, 2), createBarrier(2, 0),
			createBuffer(0), createBuffer(2), createBuffer(1),

			// close one after the barrier one before the barrier
			createEndOfPartition(2), createEndOfPartition(1),
			createBuffer(0),

			// final end of stream
			createEndOfPartition(0)
		};
		inputGate = createBarrierBuffer(3, sequence);

		// data after first checkpoint
		check(sequence[3], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[4], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[5], inputGate.pollNext().get(), PAGE_SIZE);
		assertEquals(1L, inputGate.getLatestCheckpointId());

		// alignment of second checkpoint
		check(sequence[10], inputGate.pollNext().get(), PAGE_SIZE);
		assertEquals(2L, inputGate.getLatestCheckpointId());

		// first end-of-partition encountered: checkpoint will not be completed
		check(sequence[12], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[8], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[9], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[11], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[13], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[14], inputGate.pollNext().get(), PAGE_SIZE);
	}

	@Test
	public void testSingleChannelAbortCheckpoint() throws Exception {
		BufferOrEvent[] sequence = {
			createBuffer(0),
			createBarrier(1, 0),
			createBuffer(0),
			createBarrier(2, 0),
			createCancellationBarrier(4, 0),
			createBarrier(5, 0),
			createBuffer(0),
			createCancellationBarrier(6, 0),
			createBuffer(0)
		};
		AbstractInvokable toNotify = mock(AbstractInvokable.class);
		inputGate = createBarrierBuffer(1, sequence, toNotify);

		check(sequence[0], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[2], inputGate.pollNext().get(), PAGE_SIZE);
		verify(toNotify, times(1)).triggerCheckpointOnBarrier(argThat(new CheckpointMatcher(1L)), any(CheckpointOptions.class), any(CheckpointMetrics.class));
		assertEquals(0L, inputGate.getAlignmentDurationNanos());

		check(sequence[6], inputGate.pollNext().get(), PAGE_SIZE);
		assertEquals(5L, inputGate.getLatestCheckpointId());
		verify(toNotify, times(1)).triggerCheckpointOnBarrier(argThat(new CheckpointMatcher(2L)), any(CheckpointOptions.class), any(CheckpointMetrics.class));
		verify(toNotify, times(1)).abortCheckpointOnBarrier(eq(4L),
			argThat(new CheckpointExceptionMatcher(CheckpointFailureReason.CHECKPOINT_DECLINED_ON_CANCELLATION_BARRIER)));
		verify(toNotify, times(1)).triggerCheckpointOnBarrier(argThat(new CheckpointMatcher(5L)), any(CheckpointOptions.class), any(CheckpointMetrics.class));
		assertEquals(0L, inputGate.getAlignmentDurationNanos());

		check(sequence[8], inputGate.pollNext().get(), PAGE_SIZE);
		assertEquals(6L, inputGate.getLatestCheckpointId());
		verify(toNotify, times(1)).abortCheckpointOnBarrier(eq(6L),
			argThat(new CheckpointExceptionMatcher(CheckpointFailureReason.CHECKPOINT_DECLINED_ON_CANCELLATION_BARRIER)));
		assertEquals(0L, inputGate.getAlignmentDurationNanos());
	}

	@Test
	public void testMultiChannelAbortCheckpoint() throws Exception {
		BufferOrEvent[] sequence = {
				// some buffers and a successful checkpoint
			/* 0 */ createBuffer(0), createBuffer(2), createBuffer(0),
			/* 3 */ createBarrier(1, 1), createBarrier(1, 2),
			/* 5 */ createBuffer(2), createBuffer(1),
			/* 7 */ createBarrier(1, 0),
			/* 8 */ createBuffer(0), createBuffer(2),

				// aborted on last barrier
			/* 10 */ createBarrier(2, 0), createBarrier(2, 2),
			/* 12 */ createBuffer(0), createBuffer(2),
			/* 14 */ createCancellationBarrier(2, 1),

				// successful checkpoint
			/* 15 */ createBuffer(2), createBuffer(1),
			/* 17 */ createBarrier(3, 1), createBarrier(3, 2), createBarrier(3, 0),

				// abort on first barrier
			/* 20 */ createBuffer(0), createBuffer(1),
			/* 22 */ createCancellationBarrier(4, 1), createBarrier(4, 2),
			/* 24 */ createBuffer(0),
			/* 25 */ createBarrier(4, 0),

				// another successful checkpoint
			/* 26 */ createBuffer(0), createBuffer(1), createBuffer(2),
			/* 29 */ createBarrier(5, 2), createBarrier(5, 1), createBarrier(5, 0),
			/* 32 */ createBuffer(0), createBuffer(1),

				// abort multiple cancellations and a barrier after the cancellations
			/* 34 */ createCancellationBarrier(6, 1), createCancellationBarrier(6, 2),
			/* 36 */ createBarrier(6, 0),

			/* 37 */ createBuffer(0)
		};
		AbstractInvokable toNotify = mock(AbstractInvokable.class);
		inputGate = createBarrierBuffer(3, sequence, toNotify);

		long startTs;

		// successful first checkpoint, with some aligned buffers
		check(sequence[0], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[1], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[2], inputGate.pollNext().get(), PAGE_SIZE);
		startTs = System.nanoTime();
		check(sequence[5], inputGate.pollNext().get(), PAGE_SIZE);
		verify(toNotify, times(1)).triggerCheckpointOnBarrier(argThat(new CheckpointMatcher(1L)), any(CheckpointOptions.class), any(CheckpointMetrics.class));
		validateAlignmentTime(startTs, inputGate.getAlignmentDurationNanos());

		check(sequence[6], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[8], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[9], inputGate.pollNext().get(), PAGE_SIZE);

		// canceled checkpoint on last barrier
		startTs = System.nanoTime();
		check(sequence[12], inputGate.pollNext().get(), PAGE_SIZE);
		verify(toNotify, times(1)).abortCheckpointOnBarrier(eq(2L),
			argThat(new CheckpointExceptionMatcher(CheckpointFailureReason.CHECKPOINT_DECLINED_ON_CANCELLATION_BARRIER)));
		validateAlignmentTime(startTs, inputGate.getAlignmentDurationNanos());
		check(sequence[13], inputGate.pollNext().get(), PAGE_SIZE);

		// one more successful checkpoint
		check(sequence[15], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[16], inputGate.pollNext().get(), PAGE_SIZE);
		startTs = System.nanoTime();
		check(sequence[20], inputGate.pollNext().get(), PAGE_SIZE);
		verify(toNotify, times(1)).triggerCheckpointOnBarrier(argThat(new CheckpointMatcher(3L)), any(CheckpointOptions.class), any(CheckpointMetrics.class));
		validateAlignmentTime(startTs, inputGate.getAlignmentDurationNanos());
		check(sequence[21], inputGate.pollNext().get(), PAGE_SIZE);

		// this checkpoint gets immediately canceled
		check(sequence[24], inputGate.pollNext().get(), PAGE_SIZE);
		verify(toNotify, times(1)).abortCheckpointOnBarrier(eq(4L),
			argThat(new CheckpointExceptionMatcher(CheckpointFailureReason.CHECKPOINT_DECLINED_ON_CANCELLATION_BARRIER)));
		assertEquals(0L, inputGate.getAlignmentDurationNanos());

		// some buffers
		check(sequence[26], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[27], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[28], inputGate.pollNext().get(), PAGE_SIZE);

		// a simple successful checkpoint
		startTs = System.nanoTime();
		check(sequence[32], inputGate.pollNext().get(), PAGE_SIZE);
		verify(toNotify, times(1)).triggerCheckpointOnBarrier(argThat(new CheckpointMatcher(5L)), any(CheckpointOptions.class), any(CheckpointMetrics.class));
		validateAlignmentTime(startTs, inputGate.getAlignmentDurationNanos());
		check(sequence[33], inputGate.pollNext().get(), PAGE_SIZE);

		check(sequence[37], inputGate.pollNext().get(), PAGE_SIZE);
		verify(toNotify, times(1)).abortCheckpointOnBarrier(eq(6L),
			argThat(new CheckpointExceptionMatcher(CheckpointFailureReason.CHECKPOINT_DECLINED_ON_CANCELLATION_BARRIER)));
		assertEquals(0L, inputGate.getAlignmentDurationNanos());
	}

	@Test
	public void testAbortViaQueuedBarriers() throws Exception {
		BufferOrEvent[] sequence = {
				// starting a checkpoint
			/* 0 */ createBuffer(1),
			/* 1 */ createBarrier(1, 1), createBarrier(1, 2),
			/* 3 */ createBuffer(2), createBuffer(0), createBuffer(1),

				// queued barrier and cancellation barrier
			/* 6 */ createCancellationBarrier(2, 2),
			/* 7 */ createBarrier(2, 1),

				// some intermediate buffers (some queued)
			/* 8 */ createBuffer(0), createBuffer(1), createBuffer(2),

				// complete initial checkpoint
			/* 11 */ createBarrier(1, 0),

				// some buffers (none queued, since checkpoint is aborted)
			/* 12 */ createBuffer(2), createBuffer(1), createBuffer(0),

				// final barrier of aborted checkpoint
			/* 15 */ createBarrier(2, 0),

				// some more buffers
			/* 16 */ createBuffer(0), createBuffer(1), createBuffer(2)
		};
		AbstractInvokable toNotify = mock(AbstractInvokable.class);
		inputGate = createBarrierBuffer(3, sequence, toNotify);

		long startTs;

		check(sequence[0], inputGate.pollNext().get(), PAGE_SIZE);

		// starting first checkpoint
		startTs = System.nanoTime();
		check(sequence[4], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[8], inputGate.pollNext().get(), PAGE_SIZE);

		// finished first checkpoint
		check(sequence[3], inputGate.pollNext().get(), PAGE_SIZE);
		verify(toNotify, times(1)).triggerCheckpointOnBarrier(argThat(new CheckpointMatcher(1L)), any(CheckpointOptions.class), any(CheckpointMetrics.class));
		validateAlignmentTime(startTs, inputGate.getAlignmentDurationNanos());

		check(sequence[5], inputGate.pollNext().get(), PAGE_SIZE);

		// re-read the queued cancellation barriers
		check(sequence[9], inputGate.pollNext().get(), PAGE_SIZE);
		verify(toNotify, times(1)).abortCheckpointOnBarrier(eq(2L),
			argThat(new CheckpointExceptionMatcher(CheckpointFailureReason.CHECKPOINT_DECLINED_ON_CANCELLATION_BARRIER)));
		assertEquals(0L, inputGate.getAlignmentDurationNanos());

		check(sequence[10], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[12], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[13], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[14], inputGate.pollNext().get(), PAGE_SIZE);

		check(sequence[16], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[17], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[18], inputGate.pollNext().get(), PAGE_SIZE);

		// no further alignment should have happened
		assertEquals(0L, inputGate.getAlignmentDurationNanos());

		// no further checkpoint (abort) notifications
		verify(toNotify, times(1)).triggerCheckpointOnBarrier(any(CheckpointMetaData.class), any(CheckpointOptions.class), any(CheckpointMetrics.class));
		verify(toNotify, times(1)).abortCheckpointOnBarrier(anyLong(),
			argThat(new CheckpointExceptionMatcher(CheckpointFailureReason.CHECKPOINT_DECLINED_ON_CANCELLATION_BARRIER)));
	}

	/**
	 * This tests the where a replay of queued checkpoint barriers meets
	 * a canceled checkpoint.
	 *
	 * <p>The replayed newer checkpoint barrier must not try to cancel the
	 * already canceled checkpoint.
	 */
	@Test
	public void testAbortWhileHavingQueuedBarriers() throws Exception {
		BufferOrEvent[] sequence = {
				// starting a checkpoint
			/*  0 */ createBuffer(1),
			/*  1 */ createBarrier(1, 1),
			/*  2 */ createBuffer(2), createBuffer(0), createBuffer(1),

				// queued barrier and cancellation barrier
			/*  5 */ createBarrier(2, 1),

				// some queued buffers
			/*  6 */ createBuffer(2), createBuffer(1),

				// cancel the initial checkpoint
			/*  8 */ createCancellationBarrier(1, 0),

				// some more buffers
			/*  9 */ createBuffer(2), createBuffer(1), createBuffer(0),

				// ignored barrier - already canceled and moved to next checkpoint
			/* 12 */ createBarrier(1, 2),

				// some more buffers
			/* 13 */ createBuffer(0), createBuffer(1), createBuffer(2),

				// complete next checkpoint regularly
			/* 16 */ createBarrier(2, 0), createBarrier(2, 2),

				// some more buffers
			/* 18 */ createBuffer(0), createBuffer(1), createBuffer(2)
		};
		AbstractInvokable toNotify = mock(AbstractInvokable.class);
		inputGate = createBarrierBuffer(3, sequence, toNotify);

		long startTs;

		check(sequence[0], inputGate.pollNext().get(), PAGE_SIZE);

		// starting first checkpoint
		startTs = System.nanoTime();
		check(sequence[2], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[3], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[6], inputGate.pollNext().get(), PAGE_SIZE);

		// cancelled by cancellation barrier
		check(sequence[4], inputGate.pollNext().get(), PAGE_SIZE);
		validateAlignmentTime(startTs, inputGate.getAlignmentDurationNanos());
		verify(toNotify).abortCheckpointOnBarrier(eq(1L),
			argThat(new CheckpointExceptionMatcher(CheckpointFailureReason.CHECKPOINT_DECLINED_ON_CANCELLATION_BARRIER)));

		// the next checkpoint alignment starts now
		startTs = System.nanoTime();
		check(sequence[9], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[11], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[13], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[15], inputGate.pollNext().get(), PAGE_SIZE);

		// checkpoint done
		check(sequence[7], inputGate.pollNext().get(), PAGE_SIZE);
		validateAlignmentTime(startTs, inputGate.getAlignmentDurationNanos());
		verify(toNotify).triggerCheckpointOnBarrier(argThat(new CheckpointMatcher(2L)), any(CheckpointOptions.class), any(CheckpointMetrics.class));

		// queued data
		check(sequence[10], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[14], inputGate.pollNext().get(), PAGE_SIZE);

		// trailing data
		check(sequence[18], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[19], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[20], inputGate.pollNext().get(), PAGE_SIZE);

		// check overall notifications
		verify(toNotify, times(1)).triggerCheckpointOnBarrier(any(CheckpointMetaData.class), any(CheckpointOptions.class), any(CheckpointMetrics.class));
		verify(toNotify, times(1)).abortCheckpointOnBarrier(anyLong(), any(Throwable.class));
	}

	/**
	 * This tests the where a cancellation barrier is received for a checkpoint already
	 * canceled due to receiving a newer checkpoint barrier.
	 */
	@Test
	public void testIgnoreCancelBarrierIfCheckpointSubsumed() throws Exception {
		BufferOrEvent[] sequence = {
				// starting a checkpoint
			/*  0 */ createBuffer(2),
			/*  1 */ createBarrier(3, 1), createBarrier(3, 0),
			/*  3 */ createBuffer(0), createBuffer(1), createBuffer(2),

				// newer checkpoint barrier cancels/subsumes pending checkpoint
			/*  6 */ createBarrier(5, 2),

				// some queued buffers
			/*  7 */ createBuffer(2), createBuffer(1), createBuffer(0),

				// cancel barrier the initial checkpoint /it is already canceled)
			/* 10 */ createCancellationBarrier(3, 2),

				// some more buffers
			/* 11 */ createBuffer(2), createBuffer(0), createBuffer(1),

				// complete next checkpoint regularly
			/* 14 */ createBarrier(5, 0), createBarrier(5, 1),

				// some more buffers
			/* 16 */ createBuffer(0), createBuffer(1), createBuffer(2)
		};
		AbstractInvokable toNotify = mock(AbstractInvokable.class);
		inputGate = createBarrierBuffer(3, sequence, toNotify);

		long startTs;

		// validate the sequence

		check(sequence[0], inputGate.pollNext().get(), PAGE_SIZE);

		// beginning of first checkpoint
		check(sequence[5], inputGate.pollNext().get(), PAGE_SIZE);

		// future barrier aborts checkpoint
		startTs = System.nanoTime();
		check(sequence[3], inputGate.pollNext().get(), PAGE_SIZE);
		verify(toNotify, times(1)).abortCheckpointOnBarrier(eq(3L),
			argThat(new CheckpointExceptionMatcher(CheckpointFailureReason.CHECKPOINT_DECLINED_SUBSUMED)));
		check(sequence[4], inputGate.pollNext().get(), PAGE_SIZE);

		// alignment of next checkpoint
		check(sequence[8], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[9], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[12], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[13], inputGate.pollNext().get(), PAGE_SIZE);

		// checkpoint finished
		check(sequence[7], inputGate.pollNext().get(), PAGE_SIZE);
		validateAlignmentTime(startTs, inputGate.getAlignmentDurationNanos());
		verify(toNotify, times(1)).triggerCheckpointOnBarrier(argThat(new CheckpointMatcher(5L)), any(CheckpointOptions.class), any(CheckpointMetrics.class));
		check(sequence[11], inputGate.pollNext().get(), PAGE_SIZE);

		// remaining data
		check(sequence[16], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[17], inputGate.pollNext().get(), PAGE_SIZE);
		check(sequence[18], inputGate.pollNext().get(), PAGE_SIZE);

		// check overall notifications
		verify(toNotify, times(1)).triggerCheckpointOnBarrier(any(CheckpointMetaData.class), any(CheckpointOptions.class), any(CheckpointMetrics.class));
		verify(toNotify, times(1)).abortCheckpointOnBarrier(anyLong(), any(Throwable.class));
	}

	// ------------------------------------------------------------------------
	//  Utils
	// ------------------------------------------------------------------------

	private static BufferOrEvent createBarrier(long checkpointId, int channel) {
		return new BufferOrEvent(new CheckpointBarrier(
			checkpointId, System.currentTimeMillis(), CheckpointOptions.forCheckpointWithDefaultLocation()), channel);
	}

	private static BufferOrEvent createCancellationBarrier(long checkpointId, int channel) {
		return new BufferOrEvent(new CancelCheckpointMarker(checkpointId), channel);
	}

	private static BufferOrEvent createBuffer(int channel) {
		final int size = sizeCounter++;
		byte[] bytes = new byte[size];
		RND.nextBytes(bytes);

		MemorySegment memory = MemorySegmentFactory.allocateUnpooledSegment(PAGE_SIZE);
		memory.put(0, bytes);

		Buffer buf = new NetworkBuffer(memory, FreeingBufferRecycler.INSTANCE);
		buf.setSize(size);

		// retain an additional time so it does not get disposed after being read by the input gate
		buf.retainBuffer();

		return new BufferOrEvent(buf, channel);
	}

	private static BufferOrEvent createEndOfPartition(int channel) {
		return new BufferOrEvent(EndOfPartitionEvent.INSTANCE, channel);
	}

	private static void check(BufferOrEvent expected, BufferOrEvent present, int pageSize) {
		assertNotNull(expected);
		assertNotNull(present);
		assertEquals(expected.isBuffer(), present.isBuffer());

		if (expected.isBuffer()) {
			assertEquals(expected.getBuffer().getMaxCapacity(), present.getBuffer().getMaxCapacity());
			assertEquals(expected.getBuffer().getSize(), present.getBuffer().getSize());
			MemorySegment expectedMem = expected.getBuffer().getMemorySegment();
			MemorySegment presentMem = present.getBuffer().getMemorySegment();
			assertTrue("memory contents differs", expectedMem.compare(presentMem, 0, 0, pageSize) == 0);
		} else {
			assertEquals(expected.getEvent(), present.getEvent());
		}
	}

	private static void validateAlignmentTime(long startTimestamp, long alignmentDuration) {
		final long elapsed = System.nanoTime() - startTimestamp;
		assertTrue("wrong alignment time", alignmentDuration <= elapsed);
	}

	// ------------------------------------------------------------------------
	//  Testing Mocks
	// ------------------------------------------------------------------------

	/**
	 * The invokable handler used for triggering checkpoint and validation.
	 */
	private static class ValidatingCheckpointHandler extends AbstractInvokable {

		private long nextExpectedCheckpointId = -1L;
		private long lastReportedBytesBufferedInAlignment = -1;

		public ValidatingCheckpointHandler() {
			super(new DummyEnvironment("test", 1, 0));
		}

		public void setNextExpectedCheckpointId(long nextExpectedCheckpointId) {
			this.nextExpectedCheckpointId = nextExpectedCheckpointId;
		}

		public long getNextExpectedCheckpointId() {
			return nextExpectedCheckpointId;
		}

		long getLastReportedBytesBufferedInAlignment() {
			return lastReportedBytesBufferedInAlignment;
		}

		@Override
		public void invoke() {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean triggerCheckpoint(
				CheckpointMetaData checkpointMetaData,
				CheckpointOptions checkpointOptions,
				boolean advanceToEndOfEventTime) throws Exception {
			throw new UnsupportedOperationException("should never be called");
		}

		@Override
		public void triggerCheckpointOnBarrier(
				CheckpointMetaData checkpointMetaData,
				CheckpointOptions checkpointOptions,
				CheckpointMetrics checkpointMetrics) throws Exception {
			assertTrue("wrong checkpoint id", nextExpectedCheckpointId == -1L ||
				nextExpectedCheckpointId == checkpointMetaData.getCheckpointId());

			assertTrue(checkpointMetaData.getTimestamp() > 0);
			assertTrue(checkpointMetrics.getBytesBufferedInAlignment() >= 0);
			assertTrue(checkpointMetrics.getAlignmentDurationNanos() >= 0);

			nextExpectedCheckpointId++;
			lastReportedBytesBufferedInAlignment = checkpointMetrics.getBytesBufferedInAlignment();
		}

		@Override
		public void abortCheckpointOnBarrier(long checkpointId, Throwable cause) {}

		@Override
		public void notifyCheckpointComplete(long checkpointId) throws Exception {
			throw new UnsupportedOperationException("should never be called");
		}
	}

	/**
	 * The matcher used for verifying checkpoint equality.
	 */
	private static class CheckpointMatcher extends BaseMatcher<CheckpointMetaData> {

		private final long checkpointId;

		CheckpointMatcher(long checkpointId) {
			this.checkpointId = checkpointId;
		}

		@Override
		public boolean matches(Object o) {
			return o != null &&
				o.getClass() == CheckpointMetaData.class &&
				((CheckpointMetaData) o).getCheckpointId() == checkpointId;
		}

		@Override
		public void describeTo(Description description) {
			description.appendText("CheckpointMetaData - id = " + checkpointId);
		}
	}

	/**
	 * A validation matcher for checkpoint exception against failure reason.
	 */
	public static class CheckpointExceptionMatcher extends BaseMatcher<CheckpointException> {

		private final CheckpointFailureReason failureReason;

		public CheckpointExceptionMatcher(CheckpointFailureReason failureReason) {
			this.failureReason = failureReason;
		}

		@Override
		public boolean matches(Object o) {
			return o != null &&
				o.getClass() == CheckpointException.class &&
				((CheckpointException) o).getCheckpointFailureReason().equals(failureReason);
		}

		@Override
		public void describeTo(Description description) {
			description.appendText("CheckpointException - reason = " + failureReason);
		}
	}
}
