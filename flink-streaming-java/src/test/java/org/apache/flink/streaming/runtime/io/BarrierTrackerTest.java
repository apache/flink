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
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.util.event.EventListener;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

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
	 * This behavior is crucial, otherwise topologies where different inputs
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

	// ------------------------------------------------------------------------
	//  Utils
	// ------------------------------------------------------------------------

	private static BufferOrEvent createBarrier(long id, int channel) {
		return new BufferOrEvent(new CheckpointBarrier(id, System.currentTimeMillis()), channel);
	}

	private static BufferOrEvent createBuffer(int channel) {
		return new BufferOrEvent(
				new Buffer(MemorySegmentFactory.wrap(new byte[]{1, 2}), FreeingBufferRecycler.INSTANCE), channel);
	}
	
	// ------------------------------------------------------------------------
	//  Testing Mocks
	// ------------------------------------------------------------------------
	
	private static class CheckpointSequenceValidator implements EventListener<CheckpointBarrier> {

		private final long[] checkpointIDs;
		
		private int i = 0;

		private CheckpointSequenceValidator(long... checkpointIDs) {
			this.checkpointIDs = checkpointIDs;
		}
		
		@Override
		public void onEvent(CheckpointBarrier barrier) {
			assertTrue("More checkpoints than expected", i < checkpointIDs.length);
			assertNotNull(barrier);
			assertEquals("wrong checkpoint id", checkpointIDs[i++], barrier.getId());
			assertTrue(barrier.getTimestamp() > 0);
		}
	}
}
