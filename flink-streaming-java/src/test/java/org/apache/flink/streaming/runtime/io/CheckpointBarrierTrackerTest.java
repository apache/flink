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

import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.operators.testutils.DummyCheckpointInvokable;
import org.apache.flink.streaming.api.operators.SyncMailboxExecutor;

import org.junit.After;
import org.junit.Test;

import javax.annotation.Nullable;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


/**
 * Tests for the behavior of the barrier tracker.
 */
public class CheckpointBarrierTrackerTest {

	private static final int PAGE_SIZE = 512;

	private CheckpointedInputGate inputGate;

	@After
	public void ensureEmpty() throws Exception {
		assertFalse(inputGate.pollNext().isPresent());
		assertTrue(inputGate.isFinished());
	}

	@Test
	public void testSingleChannelNoBarriers() throws Exception {
		BufferOrEvent[] sequence = { createBuffer(0), createBuffer(0), createBuffer(0) };
		inputGate = createBarrierTracker(1, sequence);

		for (BufferOrEvent boe : sequence) {
			assertEquals(boe, inputGate.pollNext().get());
		}
	}

	@Test
	public void testMultiChannelNoBarriers() throws Exception {
		BufferOrEvent[] sequence = { createBuffer(2), createBuffer(2), createBuffer(0),
				createBuffer(1), createBuffer(0), createBuffer(3),
				createBuffer(1), createBuffer(1), createBuffer(2)
		};
		inputGate = createBarrierTracker(4, sequence);

		for (BufferOrEvent boe : sequence) {
			assertEquals(boe, inputGate.pollNext().get());
		}
	}

	@Test
	public void testSingleChannelWithBarriers() throws Exception {
		BufferOrEvent[] sequence = {
				createBuffer(0), createBuffer(0), createBuffer(0),
				createBarrier(1, 0),
				createBuffer(0), createBuffer(0), createBuffer(0), createBuffer(0),
				createBarrier(2, 0), createBarrier(3, 0),
				createBuffer(0), createBuffer(0),
				createBarrier(4, 0), createBarrier(5, 0), createBarrier(6, 0),
				createBuffer(0)
		};
		CheckpointSequenceValidator validator =
			new CheckpointSequenceValidator(1, 2, 3, 4, 5, 6);
		inputGate = createBarrierTracker(1, sequence, validator);

		for (BufferOrEvent boe : sequence) {
			assertEquals(boe, inputGate.pollNext().get());
		}
	}

	@Test
	public void testSingleChannelWithSkippedBarriers() throws Exception {
		BufferOrEvent[] sequence = {
				createBuffer(0),
				createBarrier(1, 0),
				createBuffer(0), createBuffer(0),
				createBarrier(3, 0), createBuffer(0),
				createBarrier(4, 0), createBarrier(6, 0), createBuffer(0),
				createBarrier(7, 0), createBuffer(0), createBarrier(10, 0),
				createBuffer(0)
		};
		CheckpointSequenceValidator validator =
			new CheckpointSequenceValidator(1, 3, 4, 6, 7, 10);
		inputGate = createBarrierTracker(1, sequence, validator);

		for (BufferOrEvent boe : sequence) {
			assertEquals(boe, inputGate.pollNext().get());
		}
	}

	@Test
	public void testMultiChannelWithBarriers() throws Exception {
		BufferOrEvent[] sequence = {
				createBuffer(0), createBuffer(2), createBuffer(0),
				createBarrier(1, 1), createBarrier(1, 2),
				createBuffer(2), createBuffer(1),
				createBarrier(1, 0),

				createBuffer(0), createBuffer(0), createBuffer(1), createBuffer(1), createBuffer(2),
				createBarrier(2, 0), createBarrier(2, 1), createBarrier(2, 2),

				createBuffer(2), createBuffer(2),
				createBarrier(3, 2),
				createBuffer(2), createBuffer(2),
				createBarrier(3, 0), createBarrier(3, 1),

				createBarrier(4, 1), createBarrier(4, 2), createBarrier(4, 0),

				createBuffer(0)
		};
		CheckpointSequenceValidator validator =
			new CheckpointSequenceValidator(1, 2, 3, 4);
		inputGate = createBarrierTracker(3, sequence, validator);

		for (BufferOrEvent boe : sequence) {
			assertEquals(boe, inputGate.pollNext().get());
		}
	}

	@Test
	public void testMultiChannelSkippingCheckpoints() throws Exception {
		BufferOrEvent[] sequence = {
				createBuffer(0), createBuffer(2), createBuffer(0),
				createBarrier(1, 1), createBarrier(1, 2),
				createBuffer(2), createBuffer(1),
				createBarrier(1, 0),

				createBuffer(0), createBuffer(0), createBuffer(1), createBuffer(1), createBuffer(2),
				createBarrier(2, 0), createBarrier(2, 1), createBarrier(2, 2),

				createBuffer(2), createBuffer(2),
				createBarrier(3, 2),
				createBuffer(2), createBuffer(2),

				// jump to checkpoint 4
				createBarrier(4, 0),
				createBuffer(0), createBuffer(1), createBuffer(2),
				createBarrier(4, 1),
				createBuffer(1),
				createBarrier(4, 2),

				createBuffer(0)
		};
		CheckpointSequenceValidator validator =
			new CheckpointSequenceValidator(1, 2, 4);
		inputGate = createBarrierTracker(3, sequence, validator);

		for (BufferOrEvent boe : sequence) {
			assertEquals(boe, inputGate.pollNext().get());
		}
	}

	/**
	 * This test validates that the barrier tracker does not immediately
	 * discard a pending checkpoint as soon as it sees a barrier from a
	 * later checkpoint from some channel.
	 *
	 * <p>This behavior is crucial, otherwise topologies where different inputs
	 * have different latency (and that latency is close to or higher than the
	 * checkpoint interval) may skip many checkpoints, or fail to complete a
	 * checkpoint all together.
	 */
	@Test
	public void testCompleteCheckpointsOnLateBarriers() throws Exception {
		BufferOrEvent[] sequence = {
				// checkpoint 2
				createBuffer(1), createBuffer(1), createBuffer(0), createBuffer(2),
				createBarrier(2, 1), createBarrier(2, 0), createBarrier(2, 2),

				// incomplete checkpoint 3
				createBuffer(1), createBuffer(0),
				createBarrier(3, 1), createBarrier(3, 2),

				// some barriers from checkpoint 4
				createBuffer(1), createBuffer(0),
				createBarrier(4, 2), createBarrier(4, 1),
				createBuffer(1), createBuffer(2),

				// last barrier from checkpoint 3
				createBarrier(3, 0),

				// complete checkpoint 4
				createBuffer(0), createBarrier(4, 0),

				// regular checkpoint 5
				createBuffer(1), createBuffer(2), createBarrier(5, 1),
				createBuffer(0), createBarrier(5, 0),
				createBuffer(1), createBarrier(5, 2),

				// checkpoint 6 (incomplete),
				createBuffer(1), createBarrier(6, 1),
				createBuffer(0), createBarrier(6, 0),

				// checkpoint 7, with early barriers for checkpoints 8 and 9
				createBuffer(1), createBarrier(7, 1),
				createBuffer(0), createBarrier(7, 2),
				createBuffer(2), createBarrier(8, 2),
				createBuffer(0), createBarrier(8, 1),
				createBuffer(1), createBarrier(9, 1),

				// complete checkpoint 7, first barriers from checkpoint 10
				createBarrier(7, 0),
				createBuffer(0), createBarrier(9, 2),
				createBuffer(2), createBarrier(10, 2),

				// complete checkpoint 8 and 9
				createBarrier(8, 0),
				createBuffer(1), createBuffer(2), createBarrier(9, 0),

				// trailing data
				createBuffer(1), createBuffer(0), createBuffer(2),

				// complete checkpoint 10
				createBarrier(10, 0), createBarrier(10, 1),
		};
		CheckpointSequenceValidator validator =
			new CheckpointSequenceValidator(2, 3, 4, 5, 7, 8, 9, 10);
		inputGate = createBarrierTracker(3, sequence, validator);

		for (BufferOrEvent boe : sequence) {
			assertEquals(boe, inputGate.pollNext().get());
		}
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
		// negative values mean an expected cancellation call!
		CheckpointSequenceValidator validator =
			new CheckpointSequenceValidator(1, 2, -4, 5, -6);
		inputGate = createBarrierTracker(1, sequence, validator);

		for (BufferOrEvent boe : sequence) {
			assertEquals(boe, inputGate.pollNext().get());
		}
	}

	@Test
	public void testMultiChannelAbortCheckpoint() throws Exception {
		BufferOrEvent[] sequence = {
				// some buffers and a successful checkpoint
				createBuffer(0), createBuffer(2), createBuffer(0),
				createBarrier(1, 1), createBarrier(1, 2),
				createBuffer(2), createBuffer(1),
				createBarrier(1, 0),

				// aborted on last barrier
				createBuffer(0), createBuffer(2),
				createBarrier(2, 0), createBarrier(2, 2),
				createBuffer(0), createBuffer(2),
				createCancellationBarrier(2, 1),

				// successful checkpoint
				createBuffer(2), createBuffer(1),
				createBarrier(3, 1), createBarrier(3, 2), createBarrier(3, 0),

				// abort on first barrier
				createBuffer(0), createBuffer(1),
				createCancellationBarrier(4, 1), createBarrier(4, 2),
				createBuffer(0),
				createBarrier(4, 0),

				// another successful checkpoint
				createBuffer(0), createBuffer(1), createBuffer(2),
				createBarrier(5, 2), createBarrier(5, 1), createBarrier(5, 0),

				// abort multiple cancellations and a barrier after the cancellations
				createBuffer(0), createBuffer(1),
				createCancellationBarrier(6, 1), createCancellationBarrier(6, 2),
				createBarrier(6, 0),

				createBuffer(0)
		};
		// negative values mean an expected cancellation call!
		CheckpointSequenceValidator validator =
			new CheckpointSequenceValidator(1, -2, 3, -4, 5, -6);
		inputGate = createBarrierTracker(3, sequence, validator);

		for (BufferOrEvent boe : sequence) {
			assertEquals(boe, inputGate.pollNext().get());
		}
	}

	/**
	 * Tests that each checkpoint is only aborted once in case of an interleaved cancellation
	 * barrier arrival of two consecutive checkpoints.
	 */
	@Test
	public void testInterleavedCancellationBarriers() throws Exception {
		BufferOrEvent[] sequence = {
			createBarrier(1L, 0),
			createCancellationBarrier(2L, 0),
			createCancellationBarrier(1L, 1),
			createCancellationBarrier(2L, 1),
			createCancellationBarrier(1L, 2),
			createCancellationBarrier(2L, 2),
			createBuffer(0)
		};
		CheckpointSequenceValidator validator =
			new CheckpointSequenceValidator(-1, -2);
		inputGate = createBarrierTracker(3, sequence, validator);

		for (BufferOrEvent boe : sequence) {
			assertEquals(boe, inputGate.pollNext().get());
		}
	}

	// ------------------------------------------------------------------------
	//  Utils
	// ------------------------------------------------------------------------
	private static CheckpointedInputGate createBarrierTracker(int numberOfChannels, BufferOrEvent[] sequence) {
		return createBarrierTracker(numberOfChannels, sequence, new DummyCheckpointInvokable());
	}

	private static CheckpointedInputGate createBarrierTracker(
			int numberOfChannels,
			BufferOrEvent[] sequence,
			@Nullable AbstractInvokable toNotifyOnCheckpoint) {
		MockInputGate gate = new MockInputGate(numberOfChannels, Arrays.asList(sequence));
		return new CheckpointedInputGate(
			gate,
			new CheckpointBarrierTracker(gate.getNumberOfInputChannels(), toNotifyOnCheckpoint),
			new SyncMailboxExecutor());
	}

	private static BufferOrEvent createBarrier(long id, int channel) {
		return new BufferOrEvent(new CheckpointBarrier(id, System.currentTimeMillis(), CheckpointOptions.forCheckpointWithDefaultLocation()), new InputChannelInfo(0, channel));
	}

	private static BufferOrEvent createCancellationBarrier(long id, int channel) {
		return new BufferOrEvent(new CancelCheckpointMarker(id), new InputChannelInfo(0, channel));
	}

	private static BufferOrEvent createBuffer(int channel) {
		return new BufferOrEvent(
				new NetworkBuffer(MemorySegmentFactory.wrap(new byte[]{1, 2}), FreeingBufferRecycler.INSTANCE), new InputChannelInfo(0, channel));
	}

	// ------------------------------------------------------------------------
	//  Testing Mocks
	// ------------------------------------------------------------------------

}
