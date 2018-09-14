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
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Tests for the behavior of the barrier tracker.
 */
public class BarrierTrackerTest {

	private static final int PAGE_SIZE = 512;

	@Test
	public void testSingleChannelNoBarriers() {
		try {
			BufferOrEvent[] sequence = { createBuffer(0), createBuffer(0), createBuffer(0) };

			MockInputGate gate = new MockInputGate(PAGE_SIZE, 1, Arrays.asList(sequence));
			BarrierTracker tracker = new BarrierTracker(gate);

			for (BufferOrEvent boe : sequence) {
				assertEquals(boe, tracker.getNextNonBlocked());
			}

			assertNull(tracker.getNextNonBlocked());
			assertNull(tracker.getNextNonBlocked());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testMultiChannelNoBarriers() {
		try {
			BufferOrEvent[] sequence = { createBuffer(2), createBuffer(2), createBuffer(0),
					createBuffer(1), createBuffer(0), createBuffer(3),
					createBuffer(1), createBuffer(1), createBuffer(2)
			};

			MockInputGate gate = new MockInputGate(PAGE_SIZE, 4, Arrays.asList(sequence));
			BarrierTracker tracker = new BarrierTracker(gate);

			for (BufferOrEvent boe : sequence) {
				assertEquals(boe, tracker.getNextNonBlocked());
			}

			assertNull(tracker.getNextNonBlocked());
			assertNull(tracker.getNextNonBlocked());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testSingleChannelWithBarriers() {
		try {
			BufferOrEvent[] sequence = {
					createBuffer(0), createBuffer(0), createBuffer(0),
					createBarrier(1, 0),
					createBuffer(0), createBuffer(0), createBuffer(0), createBuffer(0),
					createBarrier(2, 0), createBarrier(3, 0),
					createBuffer(0), createBuffer(0),
					createBarrier(4, 0), createBarrier(5, 0), createBarrier(6, 0),
					createBuffer(0)
			};

			MockInputGate gate = new MockInputGate(PAGE_SIZE, 1, Arrays.asList(sequence));
			BarrierTracker tracker = new BarrierTracker(gate);

			CheckpointSequenceValidator validator =
					new CheckpointSequenceValidator(1, 2, 3, 4, 5, 6);
			tracker.registerCheckpointEventHandler(validator);

			for (BufferOrEvent boe : sequence) {
				if (boe.isBuffer() || boe.getEvent().getClass() != CheckpointBarrier.class) {
					assertEquals(boe, tracker.getNextNonBlocked());
				}
			}

			assertNull(tracker.getNextNonBlocked());
			assertNull(tracker.getNextNonBlocked());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testSingleChannelWithSkippedBarriers() {
		try {
			BufferOrEvent[] sequence = {
					createBuffer(0),
					createBarrier(1, 0),
					createBuffer(0), createBuffer(0),
					createBarrier(3, 0), createBuffer(0),
					createBarrier(4, 0), createBarrier(6, 0), createBuffer(0),
					createBarrier(7, 0), createBuffer(0), createBarrier(10, 0),
					createBuffer(0)
			};

			MockInputGate gate = new MockInputGate(PAGE_SIZE, 1, Arrays.asList(sequence));
			BarrierTracker tracker = new BarrierTracker(gate);

			CheckpointSequenceValidator validator =
					new CheckpointSequenceValidator(1, 3, 4, 6, 7, 10);
			tracker.registerCheckpointEventHandler(validator);

			for (BufferOrEvent boe : sequence) {
				if (boe.isBuffer() || boe.getEvent().getClass() != CheckpointBarrier.class) {
					assertEquals(boe, tracker.getNextNonBlocked());
				}
			}

			assertNull(tracker.getNextNonBlocked());
			assertNull(tracker.getNextNonBlocked());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testMultiChannelWithBarriers() {
		try {
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

			MockInputGate gate = new MockInputGate(PAGE_SIZE, 3, Arrays.asList(sequence));
			BarrierTracker tracker = new BarrierTracker(gate);

			CheckpointSequenceValidator validator =
					new CheckpointSequenceValidator(1, 2, 3, 4);
			tracker.registerCheckpointEventHandler(validator);

			for (BufferOrEvent boe : sequence) {
				if (boe.isBuffer() || boe.getEvent().getClass() != CheckpointBarrier.class) {
					assertEquals(boe, tracker.getNextNonBlocked());
				}
			}

			assertNull(tracker.getNextNonBlocked());
			assertNull(tracker.getNextNonBlocked());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testMultiChannelSkippingCheckpoints() {
		try {
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

			MockInputGate gate = new MockInputGate(PAGE_SIZE, 3, Arrays.asList(sequence));
			BarrierTracker tracker = new BarrierTracker(gate);

			CheckpointSequenceValidator validator =
					new CheckpointSequenceValidator(1, 2, 4);
			tracker.registerCheckpointEventHandler(validator);

			for (BufferOrEvent boe : sequence) {
				if (boe.isBuffer() || boe.getEvent().getClass() != CheckpointBarrier.class) {
					assertEquals(boe, tracker.getNextNonBlocked());
				}
			}

			assertNull(tracker.getNextNonBlocked());
			assertNull(tracker.getNextNonBlocked());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
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
	public void testCompleteCheckpointsOnLateBarriers() {
		try {
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
					createBuffer(1), createBuffer(0), createBuffer(2)
			};

			MockInputGate gate = new MockInputGate(PAGE_SIZE, 3, Arrays.asList(sequence));
			BarrierTracker tracker = new BarrierTracker(gate);

			CheckpointSequenceValidator validator =
					new CheckpointSequenceValidator(2, 3, 4, 5, 7, 8, 9);
			tracker.registerCheckpointEventHandler(validator);

			for (BufferOrEvent boe : sequence) {
				if (boe.isBuffer() || boe.getEvent().getClass() != CheckpointBarrier.class) {
					assertEquals(boe, tracker.getNextNonBlocked());
				}
			}

			assertNull(tracker.getNextNonBlocked());
			assertNull(tracker.getNextNonBlocked());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
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

		MockInputGate gate = new MockInputGate(PAGE_SIZE, 1, Arrays.asList(sequence));
		BarrierTracker tracker = new BarrierTracker(gate);

		// negative values mean an expected cancellation call!
		CheckpointSequenceValidator validator =
				new CheckpointSequenceValidator(1, 2, -4, 5, -6);
		tracker.registerCheckpointEventHandler(validator);

		for (BufferOrEvent boe : sequence) {
			if (boe.isBuffer()) {
				assertEquals(boe, tracker.getNextNonBlocked());
			}
			assertTrue(tracker.isEmpty());
		}

		assertNull(tracker.getNextNonBlocked());
		assertNull(tracker.getNextNonBlocked());
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

		MockInputGate gate = new MockInputGate(PAGE_SIZE, 3, Arrays.asList(sequence));
		BarrierTracker tracker = new BarrierTracker(gate);

		// negative values mean an expected cancellation call!
		CheckpointSequenceValidator validator =
				new CheckpointSequenceValidator(1, -2, 3, -4, 5, -6);
		tracker.registerCheckpointEventHandler(validator);

		for (BufferOrEvent boe : sequence) {
			if (boe.isBuffer()) {
				assertEquals(boe, tracker.getNextNonBlocked());
			}
		}

		assertTrue(tracker.isEmpty());

		assertNull(tracker.getNextNonBlocked());
		assertNull(tracker.getNextNonBlocked());

		assertTrue(tracker.isEmpty());
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

		MockInputGate gate = new MockInputGate(PAGE_SIZE, 3, Arrays.asList(sequence));
		BarrierTracker tracker = new BarrierTracker(gate);
		AbstractInvokable statefulTask = mock(AbstractInvokable.class);

		tracker.registerCheckpointEventHandler(statefulTask);

		for (BufferOrEvent boe : sequence) {
			if (boe.isBuffer() || (boe.getEvent().getClass() != CheckpointBarrier.class && boe.getEvent().getClass() != CancelCheckpointMarker.class)) {
				assertEquals(boe, tracker.getNextNonBlocked());
			}
		}

		verify(statefulTask, times(1)).abortCheckpointOnBarrier(eq(1L), any(Throwable.class));
		verify(statefulTask, times(1)).abortCheckpointOnBarrier(eq(2L), any(Throwable.class));
	}

	// ------------------------------------------------------------------------
	//  Utils
	// ------------------------------------------------------------------------

	private static BufferOrEvent createBarrier(long id, int channel) {
		return new BufferOrEvent(new CheckpointBarrier(id, System.currentTimeMillis(), CheckpointOptions.forCheckpointWithDefaultLocation()), channel);
	}

	private static BufferOrEvent createCancellationBarrier(long id, int channel) {
		return new BufferOrEvent(new CancelCheckpointMarker(id), channel);
	}

	private static BufferOrEvent createBuffer(int channel) {
		return new BufferOrEvent(
				new NetworkBuffer(MemorySegmentFactory.wrap(new byte[]{1, 2}), FreeingBufferRecycler.INSTANCE), channel);
	}

	// ------------------------------------------------------------------------
	//  Testing Mocks
	// ------------------------------------------------------------------------

	private static class CheckpointSequenceValidator extends AbstractInvokable {

		private final long[] checkpointIDs;

		private int i = 0;

		private CheckpointSequenceValidator(long... checkpointIDs) {
			super(new DummyEnvironment("test", 1, 0));
			this.checkpointIDs = checkpointIDs;
		}

		@Override
		public void invoke() {
			throw new UnsupportedOperationException("should never be called");
		}

		@Override
		public boolean triggerCheckpoint(CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions) throws Exception {
			throw new UnsupportedOperationException("should never be called");
		}

		@Override
		public void triggerCheckpointOnBarrier(CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions, CheckpointMetrics checkpointMetrics) throws Exception {
			assertTrue("More checkpoints than expected", i < checkpointIDs.length);

			final long expectedId = checkpointIDs[i++];
			if (expectedId >= 0) {
				assertEquals("wrong checkpoint id", expectedId, checkpointMetaData.getCheckpointId());
				assertTrue(checkpointMetaData.getTimestamp() > 0);
			} else {
				fail("got 'triggerCheckpointOnBarrier()' when expecting an 'abortCheckpointOnBarrier()'");
			}
		}

		@Override
		public void abortCheckpointOnBarrier(long checkpointId, Throwable cause) {
			assertTrue("More checkpoints than expected", i < checkpointIDs.length);

			final long expectedId = checkpointIDs[i++];
			if (expectedId < 0) {
				assertEquals("wrong checkpoint id for checkpoint abort", -expectedId, checkpointId);
			} else {
				fail("got 'abortCheckpointOnBarrier()' when expecting an 'triggerCheckpointOnBarrier()'");
			}
		}

		@Override
		public void notifyCheckpointComplete(long checkpointId) throws Exception {
			throw new UnsupportedOperationException("should never be called");
		}
	}
}
