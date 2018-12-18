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
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.decline.CheckpointDeclineOnCancellationBarrierException;
import org.apache.flink.runtime.checkpoint.decline.CheckpointDeclineSubsumedException;
import org.apache.flink.runtime.checkpoint.decline.InputEndOfStreamException;
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
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

/**
 * Tests for the behavior of the {@link BarrierBuffer} with different {@link BufferBlocker} implements.
 */
public abstract class BarrierBufferTestBase {

	protected static final int PAGE_SIZE = 512;

	private static final Random RND = new Random();

	private static int sizeCounter = 1;

	abstract BarrierBuffer createBarrierHandler(InputGate gate) throws IOException;

	abstract void validateAlignmentBuffered(long actualBytesBuffered, BufferOrEvent... sequence);

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
			createBuffer(0, PAGE_SIZE), createBuffer(0, PAGE_SIZE),
			createBuffer(0, PAGE_SIZE), createEndOfPartition(0)
		};

		MockInputGate gate = new MockInputGate(PAGE_SIZE, 1, Arrays.asList(sequence));
		BarrierBuffer buffer = createBarrierHandler(gate);

		for (BufferOrEvent boe : sequence) {
			assertEquals(boe, buffer.getNextNonBlocked());
		}

		assertEquals(0L, buffer.getAlignmentDurationNanos());

		assertNull(buffer.getNextNonBlocked());
		assertNull(buffer.getNextNonBlocked());

		buffer.cleanup();
	}

	/**
	 * Validates that the buffer behaves correctly if no checkpoint barriers come,
	 * for an input with multiple input channels.
	 */
	@Test
	public void testMultiChannelNoBarriers() throws Exception {
		BufferOrEvent[] sequence = {
			createBuffer(2, PAGE_SIZE), createBuffer(2, PAGE_SIZE), createBuffer(0, PAGE_SIZE),
			createBuffer(1, PAGE_SIZE), createBuffer(0, PAGE_SIZE), createEndOfPartition(0),
			createBuffer(3, PAGE_SIZE), createBuffer(1, PAGE_SIZE), createEndOfPartition(3),
			createBuffer(1, PAGE_SIZE), createEndOfPartition(1), createBuffer(2, PAGE_SIZE), createEndOfPartition(2)
		};

		MockInputGate gate = new MockInputGate(PAGE_SIZE, 4, Arrays.asList(sequence));
		BarrierBuffer buffer = createBarrierHandler(gate);

		for (BufferOrEvent boe : sequence) {
			assertEquals(boe, buffer.getNextNonBlocked());
		}

		assertEquals(0L, buffer.getAlignmentDurationNanos());

		assertNull(buffer.getNextNonBlocked());
		assertNull(buffer.getNextNonBlocked());

		buffer.cleanup();
	}

	/**
	 * Validates that the buffer preserved the order of elements for a
	 * input with a single input channel, and checkpoint events.
	 */
	@Test
	public void testSingleChannelWithBarriers() throws Exception {
		BufferOrEvent[] sequence = {
			createBuffer(0, PAGE_SIZE), createBuffer(0, PAGE_SIZE), createBuffer(0, PAGE_SIZE),
			createBarrier(1, 0),
			createBuffer(0, PAGE_SIZE), createBuffer(0, PAGE_SIZE), createBuffer(0, PAGE_SIZE), createBuffer(0, PAGE_SIZE),
			createBarrier(2, 0), createBarrier(3, 0),
			createBuffer(0, PAGE_SIZE), createBuffer(0, PAGE_SIZE),
			createBarrier(4, 0), createBarrier(5, 0), createBarrier(6, 0),
			createBuffer(0, PAGE_SIZE), createEndOfPartition(0)
		};

		MockInputGate gate = new MockInputGate(PAGE_SIZE, 1, Arrays.asList(sequence));
		BarrierBuffer buffer = createBarrierHandler(gate);

		ValidatingCheckpointHandler handler = new ValidatingCheckpointHandler();
		buffer.registerCheckpointEventHandler(handler);
		handler.setNextExpectedCheckpointId(1L);

		for (BufferOrEvent boe : sequence) {
			if (boe.isBuffer() || boe.getEvent().getClass() != CheckpointBarrier.class) {
				assertEquals(boe, buffer.getNextNonBlocked());
			}
		}

		assertNull(buffer.getNextNonBlocked());
		assertNull(buffer.getNextNonBlocked());

		buffer.cleanup();
	}

	/**
	 * Validates that the buffer correctly aligns the streams for inputs with
	 * multiple input channels, by buffering and blocking certain inputs.
	 */
	@Test
	public void testMultiChannelWithBarriers() throws Exception {
		BufferOrEvent[] sequence = {
			// checkpoint with blocked data
			createBuffer(0, PAGE_SIZE), createBuffer(2, PAGE_SIZE), createBuffer(0, PAGE_SIZE),
			createBarrier(1, 1), createBarrier(1, 2),
			createBuffer(2, PAGE_SIZE), createBuffer(1, PAGE_SIZE), createBuffer(0, PAGE_SIZE),
			createBarrier(1, 0),

			// checkpoint without blocked data
			createBuffer(0, PAGE_SIZE), createBuffer(0, PAGE_SIZE), createBuffer(1, PAGE_SIZE), createBuffer(1, PAGE_SIZE), createBuffer(2, PAGE_SIZE),
			createBarrier(2, 0), createBarrier(2, 1), createBarrier(2, 2),

			// checkpoint with data only from one channel
			createBuffer(2, PAGE_SIZE), createBuffer(2, PAGE_SIZE),
			createBarrier(3, 2),
			createBuffer(2, PAGE_SIZE), createBuffer(2, PAGE_SIZE),
			createBarrier(3, 0), createBarrier(3, 1),

			// empty checkpoint
			createBarrier(4, 1), createBarrier(4, 2), createBarrier(4, 0),

			// checkpoint with blocked data in mixed order
			createBuffer(0, PAGE_SIZE), createBuffer(2, PAGE_SIZE), createBuffer(0, PAGE_SIZE),
			createBarrier(5, 1),
			createBuffer(2, PAGE_SIZE), createBuffer(0, PAGE_SIZE), createBuffer(2, PAGE_SIZE), createBuffer(1, PAGE_SIZE),
			createBarrier(5, 2),
			createBuffer(1, PAGE_SIZE), createBuffer(0, PAGE_SIZE), createBuffer(2, PAGE_SIZE), createBuffer(1, PAGE_SIZE),
			createBarrier(5, 0),

			// some trailing data
			createBuffer(0, PAGE_SIZE),
			createEndOfPartition(0), createEndOfPartition(1), createEndOfPartition(2)
		};

		MockInputGate gate = new MockInputGate(PAGE_SIZE, 3, Arrays.asList(sequence));
		BarrierBuffer buffer = createBarrierHandler(gate);

		ValidatingCheckpointHandler handler = new ValidatingCheckpointHandler();
		buffer.registerCheckpointEventHandler(handler);
		handler.setNextExpectedCheckpointId(1L);

		// pre checkpoint 1
		check(sequence[0], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[1], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[2], buffer.getNextNonBlocked(), PAGE_SIZE);
		assertEquals(1L, handler.getNextExpectedCheckpointId());

		long startTs = System.nanoTime();

		// blocking while aligning for checkpoint 1
		check(sequence[7], buffer.getNextNonBlocked(), PAGE_SIZE);
		assertEquals(1L, handler.getNextExpectedCheckpointId());

		// checkpoint 1 done, returning buffered data
		check(sequence[5], buffer.getNextNonBlocked(), PAGE_SIZE);
		assertEquals(2L, handler.getNextExpectedCheckpointId());
		validateAlignmentTime(startTs, buffer.getAlignmentDurationNanos());
		validateAlignmentBuffered(handler.getLastReportedBytesBufferedInAlignment(), sequence[5], sequence[6]);

		check(sequence[6], buffer.getNextNonBlocked(), PAGE_SIZE);

		// pre checkpoint 2
		check(sequence[9], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[10], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[11], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[12], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[13], buffer.getNextNonBlocked(), PAGE_SIZE);
		assertEquals(2L, handler.getNextExpectedCheckpointId());

		// checkpoint 2 barriers come together
		startTs = System.nanoTime();
		check(sequence[17], buffer.getNextNonBlocked(), PAGE_SIZE);
		assertEquals(3L, handler.getNextExpectedCheckpointId());
		validateAlignmentTime(startTs, buffer.getAlignmentDurationNanos());
		validateAlignmentBuffered(handler.getLastReportedBytesBufferedInAlignment());

		check(sequence[18], buffer.getNextNonBlocked(), PAGE_SIZE);

		// checkpoint 3 starts, data buffered
		check(sequence[20], buffer.getNextNonBlocked(), PAGE_SIZE);
		validateAlignmentBuffered(handler.getLastReportedBytesBufferedInAlignment(), sequence[20], sequence[21]);
		assertEquals(4L, handler.getNextExpectedCheckpointId());
		check(sequence[21], buffer.getNextNonBlocked(), PAGE_SIZE);

		// checkpoint 4 happens without extra data

		// pre checkpoint 5
		check(sequence[27], buffer.getNextNonBlocked(), PAGE_SIZE);

		validateAlignmentBuffered(handler.getLastReportedBytesBufferedInAlignment());
		assertEquals(5L, handler.getNextExpectedCheckpointId());

		check(sequence[28], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[29], buffer.getNextNonBlocked(), PAGE_SIZE);

		// checkpoint 5 aligning
		check(sequence[31], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[32], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[33], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[37], buffer.getNextNonBlocked(), PAGE_SIZE);

		// buffered data from checkpoint 5 alignment
		check(sequence[34], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[36], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[38], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[39], buffer.getNextNonBlocked(), PAGE_SIZE);

		// remaining data
		check(sequence[41], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[42], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[43], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[44], buffer.getNextNonBlocked(), PAGE_SIZE);

		assertNull(buffer.getNextNonBlocked());
		assertNull(buffer.getNextNonBlocked());

		validateAlignmentBuffered(handler.getLastReportedBytesBufferedInAlignment(),
			sequence[34], sequence[36], sequence[38], sequence[39]);

		buffer.cleanup();
	}

	@Test
	public void testMultiChannelTrailingBlockedData() throws Exception {
		BufferOrEvent[] sequence = {
			createBuffer(0, PAGE_SIZE), createBuffer(1, PAGE_SIZE), createBuffer(2, PAGE_SIZE),
			createBarrier(1, 1), createBarrier(1, 2), createBarrier(1, 0),

			createBuffer(2, PAGE_SIZE), createBuffer(1, PAGE_SIZE), createBuffer(0, PAGE_SIZE),
			createBarrier(2, 1),
			createBuffer(1, PAGE_SIZE), createBuffer(1, PAGE_SIZE), createEndOfPartition(1), createBuffer(0, PAGE_SIZE), createBuffer(2, PAGE_SIZE),
			createBarrier(2, 2),
			createBuffer(2, PAGE_SIZE), createEndOfPartition(2), createBuffer(0, PAGE_SIZE), createEndOfPartition(0)
		};

		MockInputGate gate = new MockInputGate(PAGE_SIZE, 3, Arrays.asList(sequence));
		BarrierBuffer buffer = createBarrierHandler(gate);

		ValidatingCheckpointHandler handler = new ValidatingCheckpointHandler();
		buffer.registerCheckpointEventHandler(handler);
		handler.setNextExpectedCheckpointId(1L);

		// pre-checkpoint 1
		check(sequence[0], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[1], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[2], buffer.getNextNonBlocked(), PAGE_SIZE);
		assertEquals(1L, handler.getNextExpectedCheckpointId());

		// pre-checkpoint 2
		check(sequence[6], buffer.getNextNonBlocked(), PAGE_SIZE);
		assertEquals(2L, handler.getNextExpectedCheckpointId());
		check(sequence[7], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[8], buffer.getNextNonBlocked(), PAGE_SIZE);

		// checkpoint 2 alignment
		long startTs = System.nanoTime();
		check(sequence[13], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[14], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[18], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[19], buffer.getNextNonBlocked(), PAGE_SIZE);
		validateAlignmentTime(startTs, buffer.getAlignmentDurationNanos());

		// end of stream: remaining buffered contents
		check(sequence[10], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[11], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[12], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[16], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[17], buffer.getNextNonBlocked(), PAGE_SIZE);

		assertNull(buffer.getNextNonBlocked());
		assertNull(buffer.getNextNonBlocked());

		buffer.cleanup();
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
			createBuffer(0, PAGE_SIZE), createBuffer(2, PAGE_SIZE), createBuffer(0, PAGE_SIZE),
			createBarrier(1, 1), createBarrier(1, 2),
			createBuffer(2, PAGE_SIZE), createBuffer(1, PAGE_SIZE), createBuffer(0, PAGE_SIZE),
			createBarrier(1, 0),
			createBuffer(1, PAGE_SIZE), createBuffer(0, PAGE_SIZE),

			// checkpoint 2 - where future checkpoint barriers come before
			// the current checkpoint is complete
			createBarrier(2, 1),
			createBuffer(1, PAGE_SIZE), createBuffer(2, PAGE_SIZE), createBarrier(2, 0),
			createBarrier(3, 0), createBuffer(0, PAGE_SIZE),
			createBarrier(3, 1), createBuffer(0, PAGE_SIZE), createBuffer(1, PAGE_SIZE), createBuffer(2, PAGE_SIZE),
			createBarrier(4, 1), createBuffer(1, PAGE_SIZE), createBuffer(2, PAGE_SIZE),

			// complete checkpoint 2, send a barrier for checkpoints 4 and 5
			createBarrier(2, 2),
			createBuffer(2, PAGE_SIZE), createBuffer(1, PAGE_SIZE), createBuffer(2, PAGE_SIZE), createBuffer(0, PAGE_SIZE),
			createBarrier(4, 0),
			createBuffer(2, PAGE_SIZE), createBuffer(1, PAGE_SIZE), createBuffer(2, PAGE_SIZE), createBuffer(0, PAGE_SIZE),
			createBarrier(5, 1),

			// complete checkpoint 3
			createBarrier(3, 2),
			createBuffer(2, PAGE_SIZE), createBuffer(1, PAGE_SIZE), createBuffer(2, PAGE_SIZE), createBuffer(0, PAGE_SIZE),
			createBarrier(6, 1),

			// complete checkpoint 4, checkpoint 5 remains not fully triggered
			createBarrier(4, 2),
			createBuffer(2, PAGE_SIZE),
			createBuffer(1, PAGE_SIZE), createEndOfPartition(1),
			createBuffer(2, PAGE_SIZE), createEndOfPartition(2),
			createBuffer(0, PAGE_SIZE), createEndOfPartition(0)
		};

		MockInputGate gate = new MockInputGate(PAGE_SIZE, 3, Arrays.asList(sequence));
		BarrierBuffer buffer = createBarrierHandler(gate);

		ValidatingCheckpointHandler handler = new ValidatingCheckpointHandler();
		buffer.registerCheckpointEventHandler(handler);
		handler.setNextExpectedCheckpointId(1L);

		// around checkpoint 1
		check(sequence[0], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[1], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[2], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[7], buffer.getNextNonBlocked(), PAGE_SIZE);

		check(sequence[5], buffer.getNextNonBlocked(), PAGE_SIZE);
		assertEquals(2L, handler.getNextExpectedCheckpointId());
		check(sequence[6], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[9], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[10], buffer.getNextNonBlocked(), PAGE_SIZE);

		// alignment of checkpoint 2 - buffering also some barriers for
		// checkpoints 3 and 4
		long startTs = System.nanoTime();
		check(sequence[13], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[20], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[23], buffer.getNextNonBlocked(), PAGE_SIZE);

		// checkpoint 2 completed
		check(sequence[12], buffer.getNextNonBlocked(), PAGE_SIZE);
		validateAlignmentTime(startTs, buffer.getAlignmentDurationNanos());
		check(sequence[25], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[27], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[30], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[32], buffer.getNextNonBlocked(), PAGE_SIZE);

		// checkpoint 3 completed (emit buffered)
		check(sequence[16], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[18], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[19], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[28], buffer.getNextNonBlocked(), PAGE_SIZE);

		// past checkpoint 3
		check(sequence[36], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[38], buffer.getNextNonBlocked(), PAGE_SIZE);

		// checkpoint 4 completed (emit buffered)
		check(sequence[22], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[26], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[31], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[33], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[39], buffer.getNextNonBlocked(), PAGE_SIZE);

		// past checkpoint 4, alignment for checkpoint 5
		check(sequence[42], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[45], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[46], buffer.getNextNonBlocked(), PAGE_SIZE);

		// abort checkpoint 5 (end of partition)
		check(sequence[37], buffer.getNextNonBlocked(), PAGE_SIZE);

		// start checkpoint 6 alignment
		check(sequence[47], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[48], buffer.getNextNonBlocked(), PAGE_SIZE);

		// end of input, emit remainder
		check(sequence[43], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[44], buffer.getNextNonBlocked(), PAGE_SIZE);

		assertNull(buffer.getNextNonBlocked());
		assertNull(buffer.getNextNonBlocked());

		buffer.cleanup();
	}

	/**
	 * Validates that the buffer skips over the current checkpoint if it
	 * receives a barrier from a later checkpoint on a non-blocked input.
	 */
	@Test
	public void testMultiChannelSkippingCheckpoints() throws Exception {
		BufferOrEvent[] sequence = {
			// checkpoint 1 - with blocked data
			createBuffer(0, PAGE_SIZE), createBuffer(2, PAGE_SIZE), createBuffer(0, PAGE_SIZE),
			createBarrier(1, 1), createBarrier(1, 2),
			createBuffer(2, PAGE_SIZE), createBuffer(1, PAGE_SIZE), createBuffer(0, PAGE_SIZE),
			createBarrier(1, 0),
			createBuffer(1, PAGE_SIZE), createBuffer(0, PAGE_SIZE),

			// checkpoint 2 will not complete: pre-mature barrier from checkpoint 3
			createBarrier(2, 1),
			createBuffer(1, PAGE_SIZE), createBuffer(2, PAGE_SIZE),
			createBarrier(2, 0),
			createBuffer(2, PAGE_SIZE), createBuffer(0, PAGE_SIZE),
			createBarrier(3, 2),

			createBuffer(2, PAGE_SIZE),
			createBuffer(1, PAGE_SIZE), createEndOfPartition(1),
			createBuffer(2, PAGE_SIZE), createEndOfPartition(2),
			createBuffer(0, PAGE_SIZE), createEndOfPartition(0)
		};

		MockInputGate gate = new MockInputGate(PAGE_SIZE, 3, Arrays.asList(sequence));
		BarrierBuffer buffer = createBarrierHandler(gate);

		AbstractInvokable toNotify = mock(AbstractInvokable.class);
		buffer.registerCheckpointEventHandler(toNotify);

		long startTs;

		// initial data
		check(sequence[0], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[1], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[2], buffer.getNextNonBlocked(), PAGE_SIZE);

		// align checkpoint 1
		startTs = System.nanoTime();
		check(sequence[7], buffer.getNextNonBlocked(), PAGE_SIZE);
		assertEquals(1L, buffer.getCurrentCheckpointId());

		// checkpoint done - replay buffered
		check(sequence[5], buffer.getNextNonBlocked(), PAGE_SIZE);
		validateAlignmentTime(startTs, buffer.getAlignmentDurationNanos());
		verify(toNotify).triggerCheckpointOnBarrier(argThat(new CheckpointMatcher(1L)), any(CheckpointOptions.class), any(CheckpointMetrics.class));
		check(sequence[6], buffer.getNextNonBlocked(), PAGE_SIZE);

		check(sequence[9], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[10], buffer.getNextNonBlocked(), PAGE_SIZE);

		// alignment of checkpoint 2
		startTs = System.nanoTime();
		check(sequence[13], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[15], buffer.getNextNonBlocked(), PAGE_SIZE);

		// checkpoint 2 aborted, checkpoint 3 started
		check(sequence[12], buffer.getNextNonBlocked(), PAGE_SIZE);
		assertEquals(3L, buffer.getCurrentCheckpointId());
		validateAlignmentTime(startTs, buffer.getAlignmentDurationNanos());
		verify(toNotify).abortCheckpointOnBarrier(eq(2L), isA(CheckpointDeclineSubsumedException.class));
		check(sequence[16], buffer.getNextNonBlocked(), PAGE_SIZE);

		// checkpoint 3 alignment in progress
		check(sequence[19], buffer.getNextNonBlocked(), PAGE_SIZE);

		// checkpoint 3 aborted (end of partition)
		check(sequence[20], buffer.getNextNonBlocked(), PAGE_SIZE);
		verify(toNotify).abortCheckpointOnBarrier(eq(3L), isA(InputEndOfStreamException.class));

		// replay buffered data from checkpoint 3
		check(sequence[18], buffer.getNextNonBlocked(), PAGE_SIZE);

		// all the remaining messages
		check(sequence[21], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[22], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[23], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[24], buffer.getNextNonBlocked(), PAGE_SIZE);

		assertNull(buffer.getNextNonBlocked());
		assertNull(buffer.getNextNonBlocked());

		buffer.cleanup();
	}

	/**
	 * Validates that the buffer skips over the current checkpoint if it
	 * receives a barrier from a later checkpoint on a non-blocked input.
	 */
	@Test
	public void testMultiChannelJumpingOverCheckpoint() throws Exception {
		BufferOrEvent[] sequence = {
			// checkpoint 1 - with blocked data
			createBuffer(0, PAGE_SIZE), createBuffer(2, PAGE_SIZE), createBuffer(0, PAGE_SIZE),
			createBarrier(1, 1), createBarrier(1, 2),
			createBuffer(2, PAGE_SIZE), createBuffer(1, PAGE_SIZE), createBuffer(0, PAGE_SIZE),
			createBarrier(1, 0),
			createBuffer(1, PAGE_SIZE), createBuffer(0, PAGE_SIZE),

			// checkpoint 2 will not complete: pre-mature barrier from checkpoint 3
			createBarrier(2, 1),
			createBuffer(1, PAGE_SIZE), createBuffer(2, PAGE_SIZE),
			createBarrier(2, 0),
			createBuffer(2, PAGE_SIZE), createBuffer(0, PAGE_SIZE),
			createBarrier(3, 1),
			createBuffer(1, PAGE_SIZE), createBuffer(2, PAGE_SIZE),
			createBarrier(3, 0),
			createBuffer(2, PAGE_SIZE), createBuffer(0, PAGE_SIZE),
			createBarrier(4, 2),

			createBuffer(2, PAGE_SIZE),
			createBuffer(1, PAGE_SIZE), createEndOfPartition(1),
			createBuffer(2, PAGE_SIZE), createEndOfPartition(2),
			createBuffer(0, PAGE_SIZE), createEndOfPartition(0)
		};

		MockInputGate gate = new MockInputGate(PAGE_SIZE, 3, Arrays.asList(sequence));
		BarrierBuffer buffer = createBarrierHandler(gate);

		ValidatingCheckpointHandler handler = new ValidatingCheckpointHandler();
		buffer.registerCheckpointEventHandler(handler);
		handler.setNextExpectedCheckpointId(1L);

		// checkpoint 1
		check(sequence[0], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[1], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[2], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[7], buffer.getNextNonBlocked(), PAGE_SIZE);
		assertEquals(1L, buffer.getCurrentCheckpointId());

		check(sequence[5], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[6], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[9], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[10], buffer.getNextNonBlocked(), PAGE_SIZE);

		// alignment of checkpoint 2
		check(sequence[13], buffer.getNextNonBlocked(), PAGE_SIZE);
		assertEquals(2L, buffer.getCurrentCheckpointId());
		check(sequence[15], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[19], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[21], buffer.getNextNonBlocked(), PAGE_SIZE);

		long startTs = System.nanoTime();

		// checkpoint 2 aborted, checkpoint 4 started. replay buffered
		check(sequence[12], buffer.getNextNonBlocked(), PAGE_SIZE);
		assertEquals(4L, buffer.getCurrentCheckpointId());
		check(sequence[16], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[18], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[22], buffer.getNextNonBlocked(), PAGE_SIZE);

		// align checkpoint 4 remainder
		check(sequence[25], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[26], buffer.getNextNonBlocked(), PAGE_SIZE);

		validateAlignmentTime(startTs, buffer.getAlignmentDurationNanos());

		// checkpoint 4 aborted (due to end of partition)
		check(sequence[24], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[27], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[28], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[29], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[30], buffer.getNextNonBlocked(), PAGE_SIZE);

		assertNull(buffer.getNextNonBlocked());
		assertNull(buffer.getNextNonBlocked());

		buffer.cleanup();
	}

	/**
	 * Validates that the buffer skips over a later checkpoint if it
	 * receives a barrier from an even later checkpoint on a blocked input.
	 */
	@Test
	public void testMultiChannelSkippingCheckpointsViaBlockedInputs() throws Exception {
		BufferOrEvent[] sequence = {
			// checkpoint 1 - with blocked data
			createBuffer(0, PAGE_SIZE), createBuffer(2, PAGE_SIZE), createBuffer(0, PAGE_SIZE),
			createBarrier(1, 1), createBarrier(1, 2),
			createBuffer(2, PAGE_SIZE), createBuffer(1, PAGE_SIZE), createBuffer(0, PAGE_SIZE),
			createBarrier(1, 0),
			createBuffer(1, PAGE_SIZE), createBuffer(0, PAGE_SIZE),

			// checkpoint 2 will not complete: pre-mature barrier from checkpoint 3
			createBarrier(2, 1),
			createBuffer(1, PAGE_SIZE), createBuffer(2, PAGE_SIZE),
			createBarrier(2, 0),
			createBuffer(1, PAGE_SIZE), createBuffer(0, PAGE_SIZE),

			createBarrier(3, 0), // queued barrier on blocked input
			createBuffer(0, PAGE_SIZE),

			createBarrier(4, 1), // pre-mature barrier on blocked input
			createBuffer(1, PAGE_SIZE),
			createBuffer(0, PAGE_SIZE),
			createBuffer(2, PAGE_SIZE),

			// complete checkpoint 2
			createBarrier(2, 2),
			createBuffer(0, PAGE_SIZE),

			createBarrier(3, 2), // should be ignored
			createBuffer(2, PAGE_SIZE),
			createBarrier(4, 0),
			createBuffer(0, PAGE_SIZE), createBuffer(1, PAGE_SIZE), createBuffer(2, PAGE_SIZE),
			createBarrier(4, 2),

			createBuffer(1, PAGE_SIZE), createEndOfPartition(1),
			createBuffer(2, PAGE_SIZE), createEndOfPartition(2),
			createBuffer(0, PAGE_SIZE), createEndOfPartition(0)
		};

		MockInputGate gate = new MockInputGate(PAGE_SIZE, 3, Arrays.asList(sequence));
		BarrierBuffer buffer = createBarrierHandler(gate);

		// checkpoint 1
		check(sequence[0], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[1], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[2], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[7], buffer.getNextNonBlocked(), PAGE_SIZE);
		assertEquals(1L, buffer.getCurrentCheckpointId());
		check(sequence[5], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[6], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[9], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[10], buffer.getNextNonBlocked(), PAGE_SIZE);

		// alignment of checkpoint 2
		check(sequence[13], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[22], buffer.getNextNonBlocked(), PAGE_SIZE);
		assertEquals(2L, buffer.getCurrentCheckpointId());

		// checkpoint 2 completed
		check(sequence[12], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[15], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[16], buffer.getNextNonBlocked(), PAGE_SIZE);

		// checkpoint 3 skipped, alignment for 4 started
		check(sequence[18], buffer.getNextNonBlocked(), PAGE_SIZE);
		assertEquals(4L, buffer.getCurrentCheckpointId());
		check(sequence[21], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[24], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[26], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[30], buffer.getNextNonBlocked(), PAGE_SIZE);

		// checkpoint 4 completed
		check(sequence[20], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[28], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[29], buffer.getNextNonBlocked(), PAGE_SIZE);

		check(sequence[32], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[33], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[34], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[35], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[36], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[37], buffer.getNextNonBlocked(), PAGE_SIZE);

		assertNull(buffer.getNextNonBlocked());
		assertNull(buffer.getNextNonBlocked());

		buffer.cleanup();
	}

	@Test
	public void testEarlyCleanup() throws Exception {
		BufferOrEvent[] sequence = {
			createBuffer(0, PAGE_SIZE), createBuffer(1, PAGE_SIZE), createBuffer(2, PAGE_SIZE),
			createBarrier(1, 1), createBarrier(1, 2), createBarrier(1, 0),

			createBuffer(2, PAGE_SIZE), createBuffer(1, PAGE_SIZE), createBuffer(0, PAGE_SIZE),
			createBarrier(2, 1),
			createBuffer(1, PAGE_SIZE), createBuffer(1, PAGE_SIZE), createEndOfPartition(1), createBuffer(0, PAGE_SIZE), createBuffer(2, PAGE_SIZE),
			createBarrier(2, 2),
			createBuffer(2, PAGE_SIZE), createEndOfPartition(2), createBuffer(0, PAGE_SIZE), createEndOfPartition(0)
		};

		MockInputGate gate = new MockInputGate(PAGE_SIZE, 3, Arrays.asList(sequence));
		BarrierBuffer buffer = createBarrierHandler(gate);

		ValidatingCheckpointHandler handler = new ValidatingCheckpointHandler();
		buffer.registerCheckpointEventHandler(handler);
		handler.setNextExpectedCheckpointId(1L);

		// pre-checkpoint 1
		check(sequence[0], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[1], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[2], buffer.getNextNonBlocked(), PAGE_SIZE);
		assertEquals(1L, handler.getNextExpectedCheckpointId());

		// pre-checkpoint 2
		check(sequence[6], buffer.getNextNonBlocked(), PAGE_SIZE);
		assertEquals(2L, handler.getNextExpectedCheckpointId());
		check(sequence[7], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[8], buffer.getNextNonBlocked(), PAGE_SIZE);

		// checkpoint 2 alignment
		check(sequence[13], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[14], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[18], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[19], buffer.getNextNonBlocked(), PAGE_SIZE);

		// end of stream: remaining buffered contents
		buffer.getNextNonBlocked();
		buffer.cleanup();
	}

	@Test
	public void testStartAlignmentWithClosedChannels() throws Exception {
		BufferOrEvent[] sequence = {
			// close some channels immediately
			createEndOfPartition(2), createEndOfPartition(1),

			// checkpoint without blocked data
			createBuffer(0, PAGE_SIZE), createBuffer(0, PAGE_SIZE), createBuffer(3, PAGE_SIZE),
			createBarrier(2, 3), createBarrier(2, 0),

			// checkpoint with blocked data
			createBuffer(3, PAGE_SIZE), createBuffer(0, PAGE_SIZE),
			createBarrier(3, 3),
			createBuffer(3, PAGE_SIZE), createBuffer(0, PAGE_SIZE),
			createBarrier(3, 0),

			// empty checkpoint
			createBarrier(4, 0), createBarrier(4, 3),

			// some data, one channel closes
			createBuffer(0, PAGE_SIZE), createBuffer(0, PAGE_SIZE), createBuffer(3, PAGE_SIZE),
			createEndOfPartition(0),

			// checkpoint on last remaining channel
			createBuffer(3, PAGE_SIZE),
			createBarrier(5, 3),
			createBuffer(3, PAGE_SIZE),
			createEndOfPartition(3)
		};

		MockInputGate gate = new MockInputGate(PAGE_SIZE, 4, Arrays.asList(sequence));

		BarrierBuffer buffer = createBarrierHandler(gate);

		// pre checkpoint 2
		check(sequence[0], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[1], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[2], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[3], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[4], buffer.getNextNonBlocked(), PAGE_SIZE);

		// checkpoint 3 alignment
		check(sequence[7], buffer.getNextNonBlocked(), PAGE_SIZE);
		assertEquals(2L, buffer.getCurrentCheckpointId());
		check(sequence[8], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[11], buffer.getNextNonBlocked(), PAGE_SIZE);

		// checkpoint 3 buffered
		check(sequence[10], buffer.getNextNonBlocked(), PAGE_SIZE);
		assertEquals(3L, buffer.getCurrentCheckpointId());

		// after checkpoint 4
		check(sequence[15], buffer.getNextNonBlocked(), PAGE_SIZE);
		assertEquals(4L, buffer.getCurrentCheckpointId());
		check(sequence[16], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[17], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[18], buffer.getNextNonBlocked(), PAGE_SIZE);

		check(sequence[19], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[21], buffer.getNextNonBlocked(), PAGE_SIZE);
		assertEquals(5L, buffer.getCurrentCheckpointId());
		check(sequence[22], buffer.getNextNonBlocked(), PAGE_SIZE);

		assertNull(buffer.getNextNonBlocked());
		assertNull(buffer.getNextNonBlocked());

		buffer.cleanup();
	}

	@Test
	public void testEndOfStreamWhileCheckpoint() throws Exception {
		BufferOrEvent[] sequence = {
			// one checkpoint
			createBarrier(1, 0), createBarrier(1, 1), createBarrier(1, 2),

			// some buffers
			createBuffer(0, PAGE_SIZE), createBuffer(0, PAGE_SIZE), createBuffer(2, PAGE_SIZE),

			// start the checkpoint that will be incomplete
			createBarrier(2, 2), createBarrier(2, 0),
			createBuffer(0, PAGE_SIZE), createBuffer(2, PAGE_SIZE), createBuffer(1, PAGE_SIZE),

			// close one after the barrier one before the barrier
			createEndOfPartition(2), createEndOfPartition(1),
			createBuffer(0, PAGE_SIZE),

			// final end of stream
			createEndOfPartition(0)
		};

		MockInputGate gate = new MockInputGate(PAGE_SIZE, 3, Arrays.asList(sequence));
		BarrierBuffer buffer = createBarrierHandler(gate);

		// data after first checkpoint
		check(sequence[3], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[4], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[5], buffer.getNextNonBlocked(), PAGE_SIZE);
		assertEquals(1L, buffer.getCurrentCheckpointId());

		// alignment of second checkpoint
		check(sequence[10], buffer.getNextNonBlocked(), PAGE_SIZE);
		assertEquals(2L, buffer.getCurrentCheckpointId());

		// first end-of-partition encountered: checkpoint will not be completed
		check(sequence[12], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[8], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[9], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[11], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[13], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[14], buffer.getNextNonBlocked(), PAGE_SIZE);

		// all done
		assertNull(buffer.getNextNonBlocked());
		assertNull(buffer.getNextNonBlocked());

		buffer.cleanup();
	}

	@Test
	public void testSingleChannelAbortCheckpoint() throws Exception {
		BufferOrEvent[] sequence = {
			createBuffer(0, PAGE_SIZE),
			createBarrier(1, 0),
			createBuffer(0, PAGE_SIZE),
			createBarrier(2, 0),
			createCancellationBarrier(4, 0),
			createBarrier(5, 0),
			createBuffer(0, PAGE_SIZE),
			createCancellationBarrier(6, 0),
			createBuffer(0, PAGE_SIZE)
		};

		MockInputGate gate = new MockInputGate(PAGE_SIZE, 1, Arrays.asList(sequence));
		BarrierBuffer buffer = createBarrierHandler(gate);

		AbstractInvokable toNotify = mock(AbstractInvokable.class);
		buffer.registerCheckpointEventHandler(toNotify);

		check(sequence[0], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[2], buffer.getNextNonBlocked(), PAGE_SIZE);
		verify(toNotify, times(1)).triggerCheckpointOnBarrier(argThat(new CheckpointMatcher(1L)), any(CheckpointOptions.class), any(CheckpointMetrics.class));
		assertEquals(0L, buffer.getAlignmentDurationNanos());

		check(sequence[6], buffer.getNextNonBlocked(), PAGE_SIZE);
		assertEquals(5L, buffer.getCurrentCheckpointId());
		verify(toNotify, times(1)).triggerCheckpointOnBarrier(argThat(new CheckpointMatcher(2L)), any(CheckpointOptions.class), any(CheckpointMetrics.class));
		verify(toNotify, times(1)).abortCheckpointOnBarrier(eq(4L), any(CheckpointDeclineOnCancellationBarrierException.class));
		verify(toNotify, times(1)).triggerCheckpointOnBarrier(argThat(new CheckpointMatcher(5L)), any(CheckpointOptions.class), any(CheckpointMetrics.class));
		assertEquals(0L, buffer.getAlignmentDurationNanos());

		check(sequence[8], buffer.getNextNonBlocked(), PAGE_SIZE);
		assertEquals(6L, buffer.getCurrentCheckpointId());
		verify(toNotify, times(1)).abortCheckpointOnBarrier(eq(6L), any(CheckpointDeclineOnCancellationBarrierException.class));
		assertEquals(0L, buffer.getAlignmentDurationNanos());

		buffer.cleanup();
	}

	@Test
	public void testMultiChannelAbortCheckpoint() throws Exception {
		BufferOrEvent[] sequence = {
				// some buffers and a successful checkpoint
			/* 0 */ createBuffer(0, PAGE_SIZE), createBuffer(2, PAGE_SIZE), createBuffer(0, PAGE_SIZE),
			/* 3 */ createBarrier(1, 1), createBarrier(1, 2),
			/* 5 */ createBuffer(2, PAGE_SIZE), createBuffer(1, PAGE_SIZE),
			/* 7 */ createBarrier(1, 0),
			/* 8 */ createBuffer(0, PAGE_SIZE), createBuffer(2, PAGE_SIZE),

				// aborted on last barrier
			/* 10 */ createBarrier(2, 0), createBarrier(2, 2),
			/* 12 */ createBuffer(0, PAGE_SIZE), createBuffer(2, PAGE_SIZE),
			/* 14 */ createCancellationBarrier(2, 1),

				// successful checkpoint
			/* 15 */ createBuffer(2, PAGE_SIZE), createBuffer(1, PAGE_SIZE),
			/* 17 */ createBarrier(3, 1), createBarrier(3, 2), createBarrier(3, 0),

				// abort on first barrier
			/* 20 */ createBuffer(0, PAGE_SIZE), createBuffer(1, PAGE_SIZE),
			/* 22 */ createCancellationBarrier(4, 1), createBarrier(4, 2),
			/* 24 */ createBuffer(0, PAGE_SIZE),
			/* 25 */ createBarrier(4, 0),

				// another successful checkpoint
			/* 26 */ createBuffer(0, PAGE_SIZE), createBuffer(1, PAGE_SIZE), createBuffer(2, PAGE_SIZE),
			/* 29 */ createBarrier(5, 2), createBarrier(5, 1), createBarrier(5, 0),
			/* 32 */ createBuffer(0, PAGE_SIZE), createBuffer(1, PAGE_SIZE),

				// abort multiple cancellations and a barrier after the cancellations
			/* 34 */ createCancellationBarrier(6, 1), createCancellationBarrier(6, 2),
			/* 36 */ createBarrier(6, 0),

			/* 37 */ createBuffer(0, PAGE_SIZE)
		};

		MockInputGate gate = new MockInputGate(PAGE_SIZE, 3, Arrays.asList(sequence));
		BarrierBuffer buffer = createBarrierHandler(gate);

		AbstractInvokable toNotify = mock(AbstractInvokable.class);
		buffer.registerCheckpointEventHandler(toNotify);

		long startTs;

		// successful first checkpoint, with some aligned buffers
		check(sequence[0], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[1], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[2], buffer.getNextNonBlocked(), PAGE_SIZE);
		startTs = System.nanoTime();
		check(sequence[5], buffer.getNextNonBlocked(), PAGE_SIZE);
		verify(toNotify, times(1)).triggerCheckpointOnBarrier(argThat(new CheckpointMatcher(1L)), any(CheckpointOptions.class), any(CheckpointMetrics.class));
		validateAlignmentTime(startTs, buffer.getAlignmentDurationNanos());

		check(sequence[6], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[8], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[9], buffer.getNextNonBlocked(), PAGE_SIZE);

		// canceled checkpoint on last barrier
		startTs = System.nanoTime();
		check(sequence[12], buffer.getNextNonBlocked(), PAGE_SIZE);
		verify(toNotify, times(1)).abortCheckpointOnBarrier(eq(2L), any(CheckpointDeclineOnCancellationBarrierException.class));
		validateAlignmentTime(startTs, buffer.getAlignmentDurationNanos());
		check(sequence[13], buffer.getNextNonBlocked(), PAGE_SIZE);

		// one more successful checkpoint
		check(sequence[15], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[16], buffer.getNextNonBlocked(), PAGE_SIZE);
		startTs = System.nanoTime();
		check(sequence[20], buffer.getNextNonBlocked(), PAGE_SIZE);
		verify(toNotify, times(1)).triggerCheckpointOnBarrier(argThat(new CheckpointMatcher(3L)), any(CheckpointOptions.class), any(CheckpointMetrics.class));
		validateAlignmentTime(startTs, buffer.getAlignmentDurationNanos());
		check(sequence[21], buffer.getNextNonBlocked(), PAGE_SIZE);

		// this checkpoint gets immediately canceled
		check(sequence[24], buffer.getNextNonBlocked(), PAGE_SIZE);
		verify(toNotify, times(1)).abortCheckpointOnBarrier(eq(4L), any(CheckpointDeclineOnCancellationBarrierException.class));
		assertEquals(0L, buffer.getAlignmentDurationNanos());

		// some buffers
		check(sequence[26], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[27], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[28], buffer.getNextNonBlocked(), PAGE_SIZE);

		// a simple successful checkpoint
		startTs = System.nanoTime();
		check(sequence[32], buffer.getNextNonBlocked(), PAGE_SIZE);
		verify(toNotify, times(1)).triggerCheckpointOnBarrier(argThat(new CheckpointMatcher(5L)), any(CheckpointOptions.class), any(CheckpointMetrics.class));
		validateAlignmentTime(startTs, buffer.getAlignmentDurationNanos());
		check(sequence[33], buffer.getNextNonBlocked(), PAGE_SIZE);

		check(sequence[37], buffer.getNextNonBlocked(), PAGE_SIZE);
		verify(toNotify, times(1)).abortCheckpointOnBarrier(eq(6L), any(CheckpointDeclineOnCancellationBarrierException.class));
		assertEquals(0L, buffer.getAlignmentDurationNanos());

		// all done
		assertNull(buffer.getNextNonBlocked());
		assertNull(buffer.getNextNonBlocked());

		buffer.cleanup();
	}

	@Test
	public void testAbortViaQueuedBarriers() throws Exception {
		BufferOrEvent[] sequence = {
				// starting a checkpoint
			/* 0 */ createBuffer(1, PAGE_SIZE),
			/* 1 */ createBarrier(1, 1), createBarrier(1, 2),
			/* 3 */ createBuffer(2, PAGE_SIZE), createBuffer(0, PAGE_SIZE), createBuffer(1, PAGE_SIZE),

				// queued barrier and cancellation barrier
			/* 6 */ createCancellationBarrier(2, 2),
			/* 7 */ createBarrier(2, 1),

				// some intermediate buffers (some queued)
			/* 8 */ createBuffer(0, PAGE_SIZE), createBuffer(1, PAGE_SIZE), createBuffer(2, PAGE_SIZE),

				// complete initial checkpoint
			/* 11 */ createBarrier(1, 0),

				// some buffers (none queued, since checkpoint is aborted)
			/* 12 */ createBuffer(2, PAGE_SIZE), createBuffer(1, PAGE_SIZE), createBuffer(0, PAGE_SIZE),

				// final barrier of aborted checkpoint
			/* 15 */ createBarrier(2, 0),

				// some more buffers
			/* 16 */ createBuffer(0, PAGE_SIZE), createBuffer(1, PAGE_SIZE), createBuffer(2, PAGE_SIZE)
		};

		MockInputGate gate = new MockInputGate(PAGE_SIZE, 3, Arrays.asList(sequence));
		BarrierBuffer buffer = createBarrierHandler(gate);

		AbstractInvokable toNotify = mock(AbstractInvokable.class);
		buffer.registerCheckpointEventHandler(toNotify);

		long startTs;

		check(sequence[0], buffer.getNextNonBlocked(), PAGE_SIZE);

		// starting first checkpoint
		startTs = System.nanoTime();
		check(sequence[4], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[8], buffer.getNextNonBlocked(), PAGE_SIZE);

		// finished first checkpoint
		check(sequence[3], buffer.getNextNonBlocked(), PAGE_SIZE);
		verify(toNotify, times(1)).triggerCheckpointOnBarrier(argThat(new CheckpointMatcher(1L)), any(CheckpointOptions.class), any(CheckpointMetrics.class));
		validateAlignmentTime(startTs, buffer.getAlignmentDurationNanos());

		check(sequence[5], buffer.getNextNonBlocked(), PAGE_SIZE);

		// re-read the queued cancellation barriers
		check(sequence[9], buffer.getNextNonBlocked(), PAGE_SIZE);
		verify(toNotify, times(1)).abortCheckpointOnBarrier(eq(2L), any(CheckpointDeclineOnCancellationBarrierException.class));
		assertEquals(0L, buffer.getAlignmentDurationNanos());

		check(sequence[10], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[12], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[13], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[14], buffer.getNextNonBlocked(), PAGE_SIZE);

		check(sequence[16], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[17], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[18], buffer.getNextNonBlocked(), PAGE_SIZE);

		// no further alignment should have happened
		assertEquals(0L, buffer.getAlignmentDurationNanos());

		// no further checkpoint (abort) notifications
		verify(toNotify, times(1)).triggerCheckpointOnBarrier(any(CheckpointMetaData.class), any(CheckpointOptions.class), any(CheckpointMetrics.class));
		verify(toNotify, times(1)).abortCheckpointOnBarrier(anyLong(), any(CheckpointDeclineOnCancellationBarrierException.class));

		// all done
		assertNull(buffer.getNextNonBlocked());
		assertNull(buffer.getNextNonBlocked());

		buffer.cleanup();
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
			/*  0 */ createBuffer(1, PAGE_SIZE),
			/*  1 */ createBarrier(1, 1),
			/*  2 */ createBuffer(2, PAGE_SIZE), createBuffer(0, PAGE_SIZE), createBuffer(1, PAGE_SIZE),

				// queued barrier and cancellation barrier
			/*  5 */ createBarrier(2, 1),

				// some queued buffers
			/*  6 */ createBuffer(2, PAGE_SIZE), createBuffer(1, PAGE_SIZE),

				// cancel the initial checkpoint
			/*  8 */ createCancellationBarrier(1, 0),

				// some more buffers
			/*  9 */ createBuffer(2, PAGE_SIZE), createBuffer(1, PAGE_SIZE), createBuffer(0, PAGE_SIZE),

				// ignored barrier - already canceled and moved to next checkpoint
			/* 12 */ createBarrier(1, 2),

				// some more buffers
			/* 13 */ createBuffer(0, PAGE_SIZE), createBuffer(1, PAGE_SIZE), createBuffer(2, PAGE_SIZE),

				// complete next checkpoint regularly
			/* 16 */ createBarrier(2, 0), createBarrier(2, 2),

				// some more buffers
			/* 18 */ createBuffer(0, PAGE_SIZE), createBuffer(1, PAGE_SIZE), createBuffer(2, PAGE_SIZE)
		};

		MockInputGate gate = new MockInputGate(PAGE_SIZE, 3, Arrays.asList(sequence));
		BarrierBuffer buffer = createBarrierHandler(gate);

		AbstractInvokable toNotify = mock(AbstractInvokable.class);
		buffer.registerCheckpointEventHandler(toNotify);

		long startTs;

		check(sequence[0], buffer.getNextNonBlocked(), PAGE_SIZE);

		// starting first checkpoint
		startTs = System.nanoTime();
		check(sequence[2], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[3], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[6], buffer.getNextNonBlocked(), PAGE_SIZE);

		// cancelled by cancellation barrier
		check(sequence[4], buffer.getNextNonBlocked(), PAGE_SIZE);
		validateAlignmentTime(startTs, buffer.getAlignmentDurationNanos());
		verify(toNotify).abortCheckpointOnBarrier(eq(1L), any(CheckpointDeclineOnCancellationBarrierException.class));

		// the next checkpoint alignment starts now
		startTs = System.nanoTime();
		check(sequence[9], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[11], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[13], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[15], buffer.getNextNonBlocked(), PAGE_SIZE);

		// checkpoint done
		check(sequence[7], buffer.getNextNonBlocked(), PAGE_SIZE);
		validateAlignmentTime(startTs, buffer.getAlignmentDurationNanos());
		verify(toNotify).triggerCheckpointOnBarrier(argThat(new CheckpointMatcher(2L)), any(CheckpointOptions.class), any(CheckpointMetrics.class));

		// queued data
		check(sequence[10], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[14], buffer.getNextNonBlocked(), PAGE_SIZE);

		// trailing data
		check(sequence[18], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[19], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[20], buffer.getNextNonBlocked(), PAGE_SIZE);

		// all done
		assertNull(buffer.getNextNonBlocked());
		assertNull(buffer.getNextNonBlocked());

		buffer.cleanup();

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
			/*  0 */ createBuffer(2, PAGE_SIZE),
			/*  1 */ createBarrier(3, 1), createBarrier(3, 0),
			/*  3 */ createBuffer(0, PAGE_SIZE), createBuffer(1, PAGE_SIZE), createBuffer(2, PAGE_SIZE),

				// newer checkpoint barrier cancels/subsumes pending checkpoint
			/*  6 */ createBarrier(5, 2),

				// some queued buffers
			/*  7 */ createBuffer(2, PAGE_SIZE), createBuffer(1, PAGE_SIZE), createBuffer(0, PAGE_SIZE),

				// cancel barrier the initial checkpoint /it is already canceled)
			/* 10 */ createCancellationBarrier(3, 2),

				// some more buffers
			/* 11 */ createBuffer(2, PAGE_SIZE), createBuffer(0, PAGE_SIZE), createBuffer(1, PAGE_SIZE),

				// complete next checkpoint regularly
			/* 14 */ createBarrier(5, 0), createBarrier(5, 1),

				// some more buffers
			/* 16 */ createBuffer(0, PAGE_SIZE), createBuffer(1, PAGE_SIZE), createBuffer(2, PAGE_SIZE)
		};

		MockInputGate gate = new MockInputGate(PAGE_SIZE, 3, Arrays.asList(sequence));
		BarrierBuffer buffer = createBarrierHandler(gate);

		AbstractInvokable toNotify = mock(AbstractInvokable.class);
		buffer.registerCheckpointEventHandler(toNotify);

		long startTs;

		// validate the sequence

		check(sequence[0], buffer.getNextNonBlocked(), PAGE_SIZE);

		// beginning of first checkpoint
		check(sequence[5], buffer.getNextNonBlocked(), PAGE_SIZE);

		// future barrier aborts checkpoint
		startTs = System.nanoTime();
		check(sequence[3], buffer.getNextNonBlocked(), PAGE_SIZE);
		verify(toNotify, times(1)).abortCheckpointOnBarrier(eq(3L), any(CheckpointDeclineSubsumedException.class));
		check(sequence[4], buffer.getNextNonBlocked(), PAGE_SIZE);

		// alignment of next checkpoint
		check(sequence[8], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[9], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[12], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[13], buffer.getNextNonBlocked(), PAGE_SIZE);

		// checkpoint finished
		check(sequence[7], buffer.getNextNonBlocked(), PAGE_SIZE);
		validateAlignmentTime(startTs, buffer.getAlignmentDurationNanos());
		verify(toNotify, times(1)).triggerCheckpointOnBarrier(argThat(new CheckpointMatcher(5L)), any(CheckpointOptions.class), any(CheckpointMetrics.class));
		check(sequence[11], buffer.getNextNonBlocked(), PAGE_SIZE);

		// remaining data
		check(sequence[16], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[17], buffer.getNextNonBlocked(), PAGE_SIZE);
		check(sequence[18], buffer.getNextNonBlocked(), PAGE_SIZE);

		// all done
		assertNull(buffer.getNextNonBlocked());
		assertNull(buffer.getNextNonBlocked());

		buffer.cleanup();

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

	private static BufferOrEvent createBuffer(int channel, int pageSize) {
		final int size = sizeCounter++;
		byte[] bytes = new byte[size];
		RND.nextBytes(bytes);

		MemorySegment memory = MemorySegmentFactory.allocateUnpooledSegment(pageSize);
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
				CheckpointOptions checkpointOptions) throws Exception {
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
}
