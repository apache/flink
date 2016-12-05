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
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.jobgraph.tasks.StatefulTask;
import org.apache.flink.runtime.state.ChainedStateHandle;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the behavior of the {@link BarrierBuffer}.
 */
public class BarrierBufferTest {

	private static final int PAGE_SIZE = 512;
	
	private static int SIZE_COUNTER = 0;
	
	private static IOManager IO_MANAGER;

	@BeforeClass
	public static void setup() {
		IO_MANAGER = new IOManagerAsync();
		SIZE_COUNTER = 1;
	}

	@AfterClass
	public static void shutdownIOManager() {
		IO_MANAGER.shutdown();
	}

	// ------------------------------------------------------------------------
	//  Tests
	// ------------------------------------------------------------------------

	/**
	 * Validates that the buffer behaves correctly if no checkpoint barriers come,
	 * for a single input channel.
	 */
	@Test
	public void testSingleChannelNoBarriers() {
		try {
			BufferOrEvent[] sequence = { 
					createBuffer(0), createBuffer(0), createBuffer(0),
					createEndOfPartition(0)
			};

			MockInputGate gate = new MockInputGate(PAGE_SIZE, 1, Arrays.asList(sequence));
			BarrierBuffer buffer = new BarrierBuffer(gate, IO_MANAGER);

			for (BufferOrEvent boe : sequence) {
				assertEquals(boe, buffer.getNextNonBlocked());
			}

			assertEquals(0L, buffer.getAlignmentDurationNanos());

			assertNull(buffer.getNextNonBlocked());
			assertNull(buffer.getNextNonBlocked());
			
			buffer.cleanup();

			checkNoTempFilesRemain();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	/**
	 * Validates that the buffer behaves correctly if no checkpoint barriers come,
	 * for an input with multiple input channels.
	 */
	@Test
	public void testMultiChannelNoBarriers() {
		try {
			BufferOrEvent[] sequence = { createBuffer(2), createBuffer(2), createBuffer(0),
					createBuffer(1), createBuffer(0), createEndOfPartition(0),
					createBuffer(3), createBuffer(1), createEndOfPartition(3),
					createBuffer(1), createEndOfPartition(1), createBuffer(2), createEndOfPartition(2)
			};

			MockInputGate gate = new MockInputGate(PAGE_SIZE, 4, Arrays.asList(sequence));
			BarrierBuffer buffer = new BarrierBuffer(gate, IO_MANAGER);

			for (BufferOrEvent boe : sequence) {
				assertEquals(boe, buffer.getNextNonBlocked());
			}

			assertEquals(0L, buffer.getAlignmentDurationNanos());

			assertNull(buffer.getNextNonBlocked());
			assertNull(buffer.getNextNonBlocked());

			buffer.cleanup();

			checkNoTempFilesRemain();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	/**
	 * Validates that the buffer preserved the order of elements for a 
	 * input with a single input channel, and checkpoint events.
	 */
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
					createBuffer(0), createEndOfPartition(0)
			};

			MockInputGate gate = new MockInputGate(PAGE_SIZE, 1, Arrays.asList(sequence));
			BarrierBuffer buffer = new BarrierBuffer(gate, IO_MANAGER);

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

			checkNoTempFilesRemain();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	/**
	 * Validates that the buffer correctly aligns the streams for inputs with
	 * multiple input channels, by buffering and blocking certain inputs.
	 */
	@Test
	public void testMultiChannelWithBarriers() {
		try {
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

			MockInputGate gate = new MockInputGate(PAGE_SIZE, 3, Arrays.asList(sequence));
			BarrierBuffer buffer = new BarrierBuffer(gate, IO_MANAGER);

			ValidatingCheckpointHandler handler = new ValidatingCheckpointHandler();
			buffer.registerCheckpointEventHandler(handler);
			handler.setNextExpectedCheckpointId(1L);

			// pre checkpoint 1
			check(sequence[0], buffer.getNextNonBlocked());
			check(sequence[1], buffer.getNextNonBlocked());
			check(sequence[2], buffer.getNextNonBlocked());
			assertEquals(1L, handler.getNextExpectedCheckpointId());

			long startTs = System.nanoTime();

			// blocking while aligning for checkpoint 1
			check(sequence[7], buffer.getNextNonBlocked());
			assertEquals(1L, handler.getNextExpectedCheckpointId());

			// checkpoint 1 done, returning buffered data
			check(sequence[5], buffer.getNextNonBlocked());
			assertEquals(2L, handler.getNextExpectedCheckpointId());
			validateAlignmentTime(startTs, buffer);

			check(sequence[6], buffer.getNextNonBlocked());

			// pre checkpoint 2
			check(sequence[9], buffer.getNextNonBlocked());
			check(sequence[10], buffer.getNextNonBlocked());
			check(sequence[11], buffer.getNextNonBlocked());
			check(sequence[12], buffer.getNextNonBlocked());
			check(sequence[13], buffer.getNextNonBlocked());
			assertEquals(2L, handler.getNextExpectedCheckpointId());

			// checkpoint 2 barriers come together
			startTs = System.nanoTime();
			check(sequence[17], buffer.getNextNonBlocked());
			assertEquals(3L, handler.getNextExpectedCheckpointId());
			validateAlignmentTime(startTs, buffer);

			check(sequence[18], buffer.getNextNonBlocked());

			// checkpoint 3 starts, data buffered
			check(sequence[20], buffer.getNextNonBlocked());
			assertEquals(4L, handler.getNextExpectedCheckpointId());
			check(sequence[21], buffer.getNextNonBlocked());

			// checkpoint 4 happens without extra data

			// pre checkpoint 5
			check(sequence[27], buffer.getNextNonBlocked());
			assertEquals(5L, handler.getNextExpectedCheckpointId());
			check(sequence[28], buffer.getNextNonBlocked());
			check(sequence[29], buffer.getNextNonBlocked());
			
			// checkpoint 5 aligning
			check(sequence[31], buffer.getNextNonBlocked());
			check(sequence[32], buffer.getNextNonBlocked());
			check(sequence[33], buffer.getNextNonBlocked());
			check(sequence[37], buffer.getNextNonBlocked());
			
			// buffered data from checkpoint 5 alignment
			check(sequence[34], buffer.getNextNonBlocked());
			check(sequence[36], buffer.getNextNonBlocked());
			check(sequence[38], buffer.getNextNonBlocked());
			check(sequence[39], buffer.getNextNonBlocked());
			
			// remaining data
			check(sequence[41], buffer.getNextNonBlocked());
			check(sequence[42], buffer.getNextNonBlocked());
			check(sequence[43], buffer.getNextNonBlocked());
			check(sequence[44], buffer.getNextNonBlocked());
			
			assertNull(buffer.getNextNonBlocked());
			assertNull(buffer.getNextNonBlocked());

			buffer.cleanup();

			checkNoTempFilesRemain();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testMultiChannelTrailingBlockedData() {
		try {
			BufferOrEvent[] sequence = {
					createBuffer(0), createBuffer(1), createBuffer(2),
					createBarrier(1, 1), createBarrier(1, 2), createBarrier(1, 0),

					createBuffer(2), createBuffer(1), createBuffer(0),
					createBarrier(2, 1),
					createBuffer(1), createBuffer(1), createEndOfPartition(1), createBuffer(0), createBuffer(2),
					createBarrier(2, 2),
					createBuffer(2), createEndOfPartition(2), createBuffer(0), createEndOfPartition(0)
			};

			MockInputGate gate = new MockInputGate(PAGE_SIZE, 3, Arrays.asList(sequence));
			BarrierBuffer buffer = new BarrierBuffer(gate, IO_MANAGER);

			ValidatingCheckpointHandler handler = new ValidatingCheckpointHandler();
			buffer.registerCheckpointEventHandler(handler);
			handler.setNextExpectedCheckpointId(1L);

			// pre-checkpoint 1
			check(sequence[0], buffer.getNextNonBlocked());
			check(sequence[1], buffer.getNextNonBlocked());
			check(sequence[2], buffer.getNextNonBlocked());
			assertEquals(1L, handler.getNextExpectedCheckpointId());

			// pre-checkpoint 2
			check(sequence[6], buffer.getNextNonBlocked());
			assertEquals(2L, handler.getNextExpectedCheckpointId());
			check(sequence[7], buffer.getNextNonBlocked());
			check(sequence[8], buffer.getNextNonBlocked());

			// checkpoint 2 alignment
			long startTs = System.nanoTime();
			check(sequence[13], buffer.getNextNonBlocked());
			check(sequence[14], buffer.getNextNonBlocked());
			check(sequence[18], buffer.getNextNonBlocked());
			check(sequence[19], buffer.getNextNonBlocked());
			validateAlignmentTime(startTs, buffer);

			// end of stream: remaining buffered contents
			check(sequence[10], buffer.getNextNonBlocked());
			check(sequence[11], buffer.getNextNonBlocked());
			check(sequence[12], buffer.getNextNonBlocked());
			check(sequence[16], buffer.getNextNonBlocked());
			check(sequence[17], buffer.getNextNonBlocked());

			assertNull(buffer.getNextNonBlocked());
			assertNull(buffer.getNextNonBlocked());

			buffer.cleanup();

			checkNoTempFilesRemain();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	/**
	 * Validates that the buffer correctly aligns the streams in cases
	 * where some channels receive barriers from multiple successive checkpoints
	 * before the pending checkpoint is complete.
	 */
	@Test
	public void testMultiChannelWithQueuedFutureBarriers() {
		try {
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

			MockInputGate gate = new MockInputGate(PAGE_SIZE, 3, Arrays.asList(sequence));
			BarrierBuffer buffer = new BarrierBuffer(gate, IO_MANAGER);

			ValidatingCheckpointHandler handler = new ValidatingCheckpointHandler();
			buffer.registerCheckpointEventHandler(handler);
			handler.setNextExpectedCheckpointId(1L);

			// around checkpoint 1
			check(sequence[0], buffer.getNextNonBlocked());
			check(sequence[1], buffer.getNextNonBlocked());
			check(sequence[2], buffer.getNextNonBlocked());
			check(sequence[7], buffer.getNextNonBlocked());
			
			check(sequence[5], buffer.getNextNonBlocked());
			assertEquals(2L, handler.getNextExpectedCheckpointId());
			check(sequence[6], buffer.getNextNonBlocked());
			check(sequence[9], buffer.getNextNonBlocked());
			check(sequence[10], buffer.getNextNonBlocked());

			// alignment of checkpoint 2 - buffering also some barriers for
			// checkpoints 3 and 4
			long startTs = System.nanoTime();
			check(sequence[13], buffer.getNextNonBlocked());
			check(sequence[20], buffer.getNextNonBlocked());
			check(sequence[23], buffer.getNextNonBlocked());

			validateAlignmentTime(startTs, buffer);

			// checkpoint 2 completed
			check(sequence[12], buffer.getNextNonBlocked());
			check(sequence[25], buffer.getNextNonBlocked());
			check(sequence[27], buffer.getNextNonBlocked());
			check(sequence[30], buffer.getNextNonBlocked());
			check(sequence[32], buffer.getNextNonBlocked());

			// checkpoint 3 completed (emit buffered)
			check(sequence[16], buffer.getNextNonBlocked());
			check(sequence[18], buffer.getNextNonBlocked());
			check(sequence[19], buffer.getNextNonBlocked());
			check(sequence[28], buffer.getNextNonBlocked());
			
			// past checkpoint 3
			check(sequence[36], buffer.getNextNonBlocked());
			check(sequence[38], buffer.getNextNonBlocked());

			// checkpoint 4 completed (emit buffered)
			check(sequence[22], buffer.getNextNonBlocked());
			check(sequence[26], buffer.getNextNonBlocked());
			check(sequence[31], buffer.getNextNonBlocked());
			check(sequence[33], buffer.getNextNonBlocked());
			check(sequence[39], buffer.getNextNonBlocked());
			
			// past checkpoint 4, alignment for checkpoint 5
			check(sequence[42], buffer.getNextNonBlocked());
			check(sequence[45], buffer.getNextNonBlocked());
			check(sequence[46], buffer.getNextNonBlocked());
			
			// abort checkpoint 5 (end of partition)
			check(sequence[37], buffer.getNextNonBlocked());
			
			// start checkpoint 6 alignment
			check(sequence[47], buffer.getNextNonBlocked());
			check(sequence[48], buffer.getNextNonBlocked());
			
			// end of input, emit remainder
			check(sequence[43], buffer.getNextNonBlocked());
			check(sequence[44], buffer.getNextNonBlocked());
			
			assertNull(buffer.getNextNonBlocked());
			assertNull(buffer.getNextNonBlocked());

			buffer.cleanup();

			checkNoTempFilesRemain();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	/**
	 * Validates that the buffer skips over the current checkpoint if it
	 * receives a barrier from a later checkpoint on a non-blocked input.
	 */
	@Test
	public void testMultiChannelSkippingCheckpoints() {
		try {
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

			MockInputGate gate = new MockInputGate(PAGE_SIZE, 3, Arrays.asList(sequence));
			BarrierBuffer buffer = new BarrierBuffer(gate, IO_MANAGER);

			ValidatingCheckpointHandler handler = new ValidatingCheckpointHandler();
			buffer.registerCheckpointEventHandler(handler);
			handler.setNextExpectedCheckpointId(1L);

			// checkpoint 1
			check(sequence[0], buffer.getNextNonBlocked());
			check(sequence[1], buffer.getNextNonBlocked());
			check(sequence[2], buffer.getNextNonBlocked());
			check(sequence[7], buffer.getNextNonBlocked());
			assertEquals(1L, buffer.getCurrentCheckpointId());
			
			check(sequence[5], buffer.getNextNonBlocked());
			check(sequence[6], buffer.getNextNonBlocked());
			check(sequence[9], buffer.getNextNonBlocked());
			check(sequence[10], buffer.getNextNonBlocked());

			// alignment of checkpoint 2
			check(sequence[13], buffer.getNextNonBlocked());
			assertEquals(2L, buffer.getCurrentCheckpointId());
			check(sequence[15], buffer.getNextNonBlocked());

			// checkpoint 2 aborted, checkpoint 3 started
			check(sequence[12], buffer.getNextNonBlocked());
			assertEquals(3L, buffer.getCurrentCheckpointId());
			check(sequence[16], buffer.getNextNonBlocked());
			check(sequence[19], buffer.getNextNonBlocked());
			check(sequence[20], buffer.getNextNonBlocked());
			
			// checkpoint 3 aborted (end of partition)
			check(sequence[18], buffer.getNextNonBlocked());
			check(sequence[21], buffer.getNextNonBlocked());
			check(sequence[22], buffer.getNextNonBlocked());
			check(sequence[23], buffer.getNextNonBlocked());
			check(sequence[24], buffer.getNextNonBlocked());

			assertNull(buffer.getNextNonBlocked());
			assertNull(buffer.getNextNonBlocked());
			
			buffer.cleanup();

			checkNoTempFilesRemain();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	/**
	 * Validates that the buffer skips over the current checkpoint if it
	 * receives a barrier from a later checkpoint on a non-blocked input.
	 */
	@Test
	public void testMultiChannelJumpingOverCheckpoint() {
		try {
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

			MockInputGate gate = new MockInputGate(PAGE_SIZE, 3, Arrays.asList(sequence));
			BarrierBuffer buffer = new BarrierBuffer(gate, IO_MANAGER);

			ValidatingCheckpointHandler handler = new ValidatingCheckpointHandler();
			buffer.registerCheckpointEventHandler(handler);
			handler.setNextExpectedCheckpointId(1L);

			// checkpoint 1
			check(sequence[0], buffer.getNextNonBlocked());
			check(sequence[1], buffer.getNextNonBlocked());
			check(sequence[2], buffer.getNextNonBlocked());
			check(sequence[7], buffer.getNextNonBlocked());
			assertEquals(1L, buffer.getCurrentCheckpointId());

			check(sequence[5], buffer.getNextNonBlocked());
			check(sequence[6], buffer.getNextNonBlocked());
			check(sequence[9], buffer.getNextNonBlocked());
			check(sequence[10], buffer.getNextNonBlocked());

			// alignment of checkpoint 2
			check(sequence[13], buffer.getNextNonBlocked());
			assertEquals(2L, buffer.getCurrentCheckpointId());
			check(sequence[15], buffer.getNextNonBlocked());
			check(sequence[19], buffer.getNextNonBlocked());
			check(sequence[21], buffer.getNextNonBlocked());

			long startTs = System.nanoTime();

			// checkpoint 2 aborted, checkpoint 4 started. replay buffered
			check(sequence[12], buffer.getNextNonBlocked());
			assertEquals(4L, buffer.getCurrentCheckpointId());
			check(sequence[16], buffer.getNextNonBlocked());
			check(sequence[18], buffer.getNextNonBlocked());
			check(sequence[22], buffer.getNextNonBlocked());

			// align checkpoint 4 remainder
			check(sequence[25], buffer.getNextNonBlocked());
			check(sequence[26], buffer.getNextNonBlocked());

			validateAlignmentTime(startTs, buffer);

			// checkpoint 4 aborted (due to end of partition)
			check(sequence[24], buffer.getNextNonBlocked());
			check(sequence[27], buffer.getNextNonBlocked());
			check(sequence[28], buffer.getNextNonBlocked());
			check(sequence[29], buffer.getNextNonBlocked());
			check(sequence[30], buffer.getNextNonBlocked());

			assertNull(buffer.getNextNonBlocked());
			assertNull(buffer.getNextNonBlocked());

			buffer.cleanup();

			checkNoTempFilesRemain();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	/**
	 * Validates that the buffer skips over a later checkpoint if it
	 * receives a barrier from an even later checkpoint on a blocked input.
	 */
	@Test
	public void testMultiChannelSkippingCheckpointsViaBlockedInputs() {
		try {
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

			MockInputGate gate = new MockInputGate(PAGE_SIZE, 3, Arrays.asList(sequence));
			BarrierBuffer buffer = new BarrierBuffer(gate, IO_MANAGER);

			// checkpoint 1
			check(sequence[0], buffer.getNextNonBlocked());
			check(sequence[1], buffer.getNextNonBlocked());
			check(sequence[2], buffer.getNextNonBlocked());
			check(sequence[7], buffer.getNextNonBlocked());
			assertEquals(1L, buffer.getCurrentCheckpointId());
			check(sequence[5], buffer.getNextNonBlocked());
			check(sequence[6], buffer.getNextNonBlocked());
			check(sequence[9], buffer.getNextNonBlocked());
			check(sequence[10], buffer.getNextNonBlocked());

			// alignment of checkpoint 2
			check(sequence[13], buffer.getNextNonBlocked());
			check(sequence[22], buffer.getNextNonBlocked());
			assertEquals(2L, buffer.getCurrentCheckpointId());

			// checkpoint 2 completed
			check(sequence[12], buffer.getNextNonBlocked());
			check(sequence[15], buffer.getNextNonBlocked());
			check(sequence[16], buffer.getNextNonBlocked());
			
			// checkpoint 3 skipped, alignment for 4 started
			check(sequence[18], buffer.getNextNonBlocked());
			assertEquals(4L, buffer.getCurrentCheckpointId());
			check(sequence[21], buffer.getNextNonBlocked());
			check(sequence[24], buffer.getNextNonBlocked());
			check(sequence[26], buffer.getNextNonBlocked());
			check(sequence[30], buffer.getNextNonBlocked());
			
			// checkpoint 4 completed
			check(sequence[20], buffer.getNextNonBlocked());
			check(sequence[28], buffer.getNextNonBlocked());
			check(sequence[29], buffer.getNextNonBlocked());
			
			check(sequence[32], buffer.getNextNonBlocked());
			check(sequence[33], buffer.getNextNonBlocked());
			check(sequence[34], buffer.getNextNonBlocked());
			check(sequence[35], buffer.getNextNonBlocked());
			check(sequence[36], buffer.getNextNonBlocked());
			check(sequence[37], buffer.getNextNonBlocked());
			
			assertNull(buffer.getNextNonBlocked());
			assertNull(buffer.getNextNonBlocked());

			buffer.cleanup();

			checkNoTempFilesRemain();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testEarlyCleanup() {
		try {
			BufferOrEvent[] sequence = {
					createBuffer(0), createBuffer(1), createBuffer(2),
					createBarrier(1, 1), createBarrier(1, 2), createBarrier(1, 0),

					createBuffer(2), createBuffer(1), createBuffer(0),
					createBarrier(2, 1),
					createBuffer(1), createBuffer(1), createEndOfPartition(1), createBuffer(0), createBuffer(2),
					createBarrier(2, 2),
					createBuffer(2), createEndOfPartition(2), createBuffer(0), createEndOfPartition(0)
			};

			MockInputGate gate = new MockInputGate(PAGE_SIZE, 3, Arrays.asList(sequence));
			BarrierBuffer buffer = new BarrierBuffer(gate, IO_MANAGER);

			ValidatingCheckpointHandler handler = new ValidatingCheckpointHandler();
			buffer.registerCheckpointEventHandler(handler);
			handler.setNextExpectedCheckpointId(1L);

			// pre-checkpoint 1
			check(sequence[0], buffer.getNextNonBlocked());
			check(sequence[1], buffer.getNextNonBlocked());
			check(sequence[2], buffer.getNextNonBlocked());
			assertEquals(1L, handler.getNextExpectedCheckpointId());

			// pre-checkpoint 2
			check(sequence[6], buffer.getNextNonBlocked());
			assertEquals(2L, handler.getNextExpectedCheckpointId());
			check(sequence[7], buffer.getNextNonBlocked());
			check(sequence[8], buffer.getNextNonBlocked());

			// checkpoint 2 alignment
			check(sequence[13], buffer.getNextNonBlocked());
			check(sequence[14], buffer.getNextNonBlocked());
			check(sequence[18], buffer.getNextNonBlocked());
			check(sequence[19], buffer.getNextNonBlocked());

			// end of stream: remaining buffered contents
			buffer.getNextNonBlocked();
			buffer.cleanup();

			checkNoTempFilesRemain();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testStartAlignmentWithClosedChannels() {
		try {
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

			MockInputGate gate = new MockInputGate(PAGE_SIZE, 4, Arrays.asList(sequence));
			
			BarrierBuffer buffer = new BarrierBuffer(gate, IO_MANAGER);
			
			// pre checkpoint 2
			check(sequence[0], buffer.getNextNonBlocked());
			check(sequence[1], buffer.getNextNonBlocked());
			check(sequence[2], buffer.getNextNonBlocked());
			check(sequence[3], buffer.getNextNonBlocked());
			check(sequence[4], buffer.getNextNonBlocked());

			// checkpoint 3 alignment
			check(sequence[7], buffer.getNextNonBlocked());
			assertEquals(2L, buffer.getCurrentCheckpointId());
			check(sequence[8], buffer.getNextNonBlocked());
			check(sequence[11], buffer.getNextNonBlocked());

			// checkpoint 3 buffered
			check(sequence[10], buffer.getNextNonBlocked());
			assertEquals(3L, buffer.getCurrentCheckpointId());

			// after checkpoint 4
			check(sequence[15], buffer.getNextNonBlocked());
			assertEquals(4L, buffer.getCurrentCheckpointId());
			check(sequence[16], buffer.getNextNonBlocked());
			check(sequence[17], buffer.getNextNonBlocked());
			check(sequence[18], buffer.getNextNonBlocked());

			check(sequence[19], buffer.getNextNonBlocked());
			check(sequence[21], buffer.getNextNonBlocked());
			assertEquals(5L, buffer.getCurrentCheckpointId());
			check(sequence[22], buffer.getNextNonBlocked());

			assertNull(buffer.getNextNonBlocked());
			assertNull(buffer.getNextNonBlocked());
			
			buffer.cleanup();
			
			checkNoTempFilesRemain();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testEndOfStreamWhileCheckpoint() {
		
	}

	// ------------------------------------------------------------------------
	//  Utils
	// ------------------------------------------------------------------------

	private static BufferOrEvent createBarrier(long id, int channel) {
		return new BufferOrEvent(new CheckpointBarrier(id, System.currentTimeMillis()), channel);
	}

	private static BufferOrEvent createBuffer(int channel) {
		// since we have no access to the contents, we need to use the size as an
		// identifier to validate correctness here
		Buffer buf = new Buffer(
				MemorySegmentFactory.allocateUnpooledSegment(PAGE_SIZE),
				FreeingBufferRecycler.INSTANCE);
		
		buf.setSize(SIZE_COUNTER++);
		return new BufferOrEvent(buf, channel);
	}

	private static BufferOrEvent createEndOfPartition(int channel) {
		return new BufferOrEvent(EndOfPartitionEvent.INSTANCE, channel);
	}
	
	private static void check(BufferOrEvent expected, BufferOrEvent present) {
		assertNotNull(expected);
		assertNotNull(present);
		assertEquals(expected.isBuffer(), present.isBuffer());
		
		if (expected.isBuffer()) {
			// since we have no access to the contents, we need to use the size as an
			// identifier to validate correctness here
			assertEquals(expected.getBuffer().getSize(), present.getBuffer().getSize());
		}
		else {
			assertEquals(expected.getEvent(), present.getEvent());
		}
	}
	
	private static void checkNoTempFilesRemain() {
		// validate that all temp files have been removed
		for (File dir : IO_MANAGER.getSpillingDirectories()) {
			for (String file : dir.list()) {
				if (file != null && !(file.equals(".") || file.equals(".."))) {
					fail("barrier buffer did not clean up temp files. remaining file: " + file);
				}
			}
		}
	}

	private static void validateAlignmentTime(long startTimestamp, BarrierBuffer buffer) {
		final long elapsed = System.nanoTime() - startTimestamp;
		assertTrue("wrong alignment time", buffer.getAlignmentDurationNanos() <= elapsed);
	}

	// ------------------------------------------------------------------------
	//  Testing Mocks
	// ------------------------------------------------------------------------

	private static class ValidatingCheckpointHandler implements StatefulTask {
		
		private long nextExpectedCheckpointId = -1L;

		public void setNextExpectedCheckpointId(long nextExpectedCheckpointId) {
			this.nextExpectedCheckpointId = nextExpectedCheckpointId;
		}

		public long getNextExpectedCheckpointId() {
			return nextExpectedCheckpointId;
		}

		@Override
		public void setInitialState(
				ChainedStateHandle<StreamStateHandle> chainedState,
				List<KeyGroupsStateHandle> keyGroupsState,
				List<Collection<OperatorStateHandle>> partitionableOperatorState) throws Exception {
			throw new UnsupportedOperationException("should never be called");
		}

		@Override
		public boolean triggerCheckpoint(CheckpointMetaData checkpointMetaData) throws Exception {
			throw new UnsupportedOperationException("should never be called");
		}

		@Override
		public void triggerCheckpointOnBarrier(CheckpointMetaData checkpointMetaData) throws Exception {

			assertTrue("wrong checkpoint id", nextExpectedCheckpointId == -1L || nextExpectedCheckpointId == checkpointMetaData.getCheckpointId());
			assertTrue(checkpointMetaData.getTimestamp() > 0);
			assertTrue(checkpointMetaData.getBytesBufferedInAlignment() >= 0);
			assertTrue(checkpointMetaData.getAlignmentDurationNanos() >= 0);

			nextExpectedCheckpointId++;
		}

		@Override
		public void notifyCheckpointComplete(long checkpointId) throws Exception {
			throw new UnsupportedOperationException("should never be called");
		}
	}
}
