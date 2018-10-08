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
import org.apache.flink.runtime.checkpoint.decline.AlignmentLimitExceededException;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

/**
 * Tests for the barrier buffer's maximum limit of buffered/spilled bytes.
 */
public class BarrierBufferAlignmentLimitTest {

	private static final int PAGE_SIZE = 512;

	private static final Random RND = new Random();

	private static IOManager ioManager;

	// ------------------------------------------------------------------------
	//  Setup
	// ------------------------------------------------------------------------

	@BeforeClass
	public static void setup() {
		ioManager = new IOManagerAsync();
	}

	@AfterClass
	public static void shutdownIOManager() {
		ioManager.shutdown();
	}

	// ------------------------------------------------------------------------
	//  Tests
	// ------------------------------------------------------------------------

	/**
	 * This tests that a single alignment that buffers too much data cancels.
	 */
	@Test
	public void testBreakCheckpointAtAlignmentLimit() throws Exception {
		BufferOrEvent[] sequence = {
				// some initial buffers
				/*  0 */ createBuffer(1, 100), createBuffer(2, 70),
				/*  2 */ createBuffer(0, 42), createBuffer(2, 111),

				// starting a checkpoint
				/*  4 */ createBarrier(7, 1),
				/*  5 */ createBuffer(1, 100), createBuffer(2, 200), createBuffer(1, 300), createBuffer(0, 50),
				/*  9 */ createBarrier(7, 0),
				/* 10 */ createBuffer(2, 100), createBuffer(0, 100), createBuffer(1, 200), createBuffer(0, 200),

				// this buffer makes the alignment spill too large
				/* 14 */ createBuffer(0, 101),

				// additional data
				/* 15 */ createBuffer(0, 100), createBuffer(1, 100), createBuffer(2, 100),

				// checkpoint completes - this should not result in a "completion notification"
				/* 18 */ createBarrier(7, 2),

				// trailing buffers
				/* 19 */ createBuffer(0, 100), createBuffer(1, 100), createBuffer(2, 100)
		};

		// the barrier buffer has a limit that only 1000 bytes may be spilled in alignment
		MockInputGate gate = new MockInputGate(PAGE_SIZE, 3, Arrays.asList(sequence));
		BarrierBuffer buffer = new BarrierBuffer(gate, new BufferSpiller(ioManager, gate.getPageSize()), 1000);

		AbstractInvokable toNotify = mock(AbstractInvokable.class);
		buffer.registerCheckpointEventHandler(toNotify);

		// validating the sequence of buffers

		check(sequence[0], buffer.getNextNonBlocked());
		check(sequence[1], buffer.getNextNonBlocked());
		check(sequence[2], buffer.getNextNonBlocked());
		check(sequence[3], buffer.getNextNonBlocked());

		// start of checkpoint
		long startTs = System.nanoTime();
		check(sequence[6], buffer.getNextNonBlocked());
		check(sequence[8], buffer.getNextNonBlocked());
		check(sequence[10], buffer.getNextNonBlocked());

		// trying to pull the next makes the alignment overflow - so buffered buffers are replayed
		check(sequence[5], buffer.getNextNonBlocked());
		validateAlignmentTime(startTs, buffer);
		verify(toNotify, times(1)).abortCheckpointOnBarrier(eq(7L), any(AlignmentLimitExceededException.class));

		// playing back buffered events
		check(sequence[7], buffer.getNextNonBlocked());
		check(sequence[11], buffer.getNextNonBlocked());
		check(sequence[12], buffer.getNextNonBlocked());
		check(sequence[13], buffer.getNextNonBlocked());
		check(sequence[14], buffer.getNextNonBlocked());

		// the additional data
		check(sequence[15], buffer.getNextNonBlocked());
		check(sequence[16], buffer.getNextNonBlocked());
		check(sequence[17], buffer.getNextNonBlocked());

		check(sequence[19], buffer.getNextNonBlocked());
		check(sequence[20], buffer.getNextNonBlocked());
		check(sequence[21], buffer.getNextNonBlocked());

		// no call for a completed checkpoint must have happened
		verify(toNotify, times(0)).triggerCheckpointOnBarrier(
			any(CheckpointMetaData.class),
			any(CheckpointOptions.class),
			any(CheckpointMetrics.class));

		assertNull(buffer.getNextNonBlocked());
		assertNull(buffer.getNextNonBlocked());

		buffer.cleanup();
		checkNoTempFilesRemain();
	}

	/**
	 * This tests the following case:
	 *   - an alignment starts
	 *   - barriers from a second checkpoint queue before the first completes
	 *   - together they are larger than the threshold
	 *   - after the first checkpoint (with second checkpoint data queued) aborts, the second completes.
	 */
	@Test
	public void testAlignmentLimitWithQueuedAlignments() throws Exception {
		BufferOrEvent[] sequence = {
				// some initial buffers
				/*  0 */ createBuffer(1, 100), createBuffer(2, 70),

				// starting a checkpoint
				/*  2 */ createBarrier(3, 2),
				/*  3 */ createBuffer(1, 100), createBuffer(2, 100),
				/*  5 */ createBarrier(3, 0),
				/*  6 */ createBuffer(0, 100), createBuffer(1, 100),

				// queue some data from the next checkpoint
				/*  8 */ createBarrier(4, 0),
				/*  9 */ createBuffer(0, 100), createBuffer(0, 120), createBuffer(1, 100),

				// this one makes the alignment overflow
				/* 12 */ createBuffer(2, 100),

				// checkpoint completed
				/* 13 */ createBarrier(3, 1),

				// more for the next checkpoint
				/* 14 */ createBarrier(4, 1),
				/* 15 */ createBuffer(0, 100), createBuffer(1, 100), createBuffer(2, 100),

				// next checkpoint completes
				/* 18 */ createBarrier(4, 2),

				// trailing data
				/* 19 */ createBuffer(0, 100), createBuffer(1, 100), createBuffer(2, 100)
		};

		// the barrier buffer has a limit that only 1000 bytes may be spilled in alignment
		MockInputGate gate = new MockInputGate(PAGE_SIZE, 3, Arrays.asList(sequence));
		BarrierBuffer buffer = new BarrierBuffer(gate, new BufferSpiller(ioManager, gate.getPageSize()), 500);

		AbstractInvokable toNotify = mock(AbstractInvokable.class);
		buffer.registerCheckpointEventHandler(toNotify);

		// validating the sequence of buffers
		long startTs;

		check(sequence[0], buffer.getNextNonBlocked());
		check(sequence[1], buffer.getNextNonBlocked());

		// start of checkpoint
		startTs = System.nanoTime();
		check(sequence[3], buffer.getNextNonBlocked());
		check(sequence[7], buffer.getNextNonBlocked());

		// next checkpoint also in progress
		check(sequence[11], buffer.getNextNonBlocked());

		// checkpoint alignment aborted due to too much data
		check(sequence[4], buffer.getNextNonBlocked());
		validateAlignmentTime(startTs, buffer);
		verify(toNotify, times(1)).abortCheckpointOnBarrier(eq(3L), any(AlignmentLimitExceededException.class));

		// replay buffered data - in the middle, the alignment for checkpoint 4 starts
		check(sequence[6], buffer.getNextNonBlocked());
		startTs = System.nanoTime();
		check(sequence[12], buffer.getNextNonBlocked());

		// only checkpoint 4 is pending now - the last checkpoint 3 barrier will not trigger success
		check(sequence[17], buffer.getNextNonBlocked());

		// checkpoint 4 completed - check and validate buffered replay
		check(sequence[9], buffer.getNextNonBlocked());
		validateAlignmentTime(startTs, buffer);
		verify(toNotify, times(1)).triggerCheckpointOnBarrier(
			argThat(new CheckpointMatcher(4L)), any(CheckpointOptions.class), any(CheckpointMetrics.class));

		check(sequence[10], buffer.getNextNonBlocked());
		check(sequence[15], buffer.getNextNonBlocked());
		check(sequence[16], buffer.getNextNonBlocked());

		// trailing data
		check(sequence[19], buffer.getNextNonBlocked());
		check(sequence[20], buffer.getNextNonBlocked());
		check(sequence[21], buffer.getNextNonBlocked());

		// only checkpoint 4 was successfully completed, not checkpoint 3
		verify(toNotify, times(0)).triggerCheckpointOnBarrier(
			argThat(new CheckpointMatcher(3L)), any(CheckpointOptions.class), any(CheckpointMetrics.class));

		assertNull(buffer.getNextNonBlocked());
		assertNull(buffer.getNextNonBlocked());

		buffer.cleanup();
		checkNoTempFilesRemain();
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	private static BufferOrEvent createBuffer(int channel, int size) {
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

	private static BufferOrEvent createBarrier(long id, int channel) {
		return new BufferOrEvent(new CheckpointBarrier(id, System.currentTimeMillis(), CheckpointOptions.forCheckpointWithDefaultLocation()), channel);
	}

	private static void check(BufferOrEvent expected, BufferOrEvent present) {
		assertNotNull(expected);
		assertNotNull(present);
		assertEquals(expected.isBuffer(), present.isBuffer());

		if (expected.isBuffer()) {
			assertEquals(expected.getBuffer().getMaxCapacity(), present.getBuffer().getMaxCapacity());
			assertEquals(expected.getBuffer().getSize(), present.getBuffer().getSize());
			MemorySegment expectedMem = expected.getBuffer().getMemorySegment();
			MemorySegment presentMem = present.getBuffer().getMemorySegment();
			assertTrue("memory contents differs", expectedMem.compare(presentMem, 0, 0, PAGE_SIZE) == 0);
		}
		else {
			assertEquals(expected.getEvent(), present.getEvent());
		}
	}

	private static void validateAlignmentTime(long startTimestamp, BarrierBuffer buffer) {
		final long elapsed = System.nanoTime() - startTimestamp;
		assertTrue("wrong alignment time", buffer.getAlignmentDurationNanos() <= elapsed);
	}

	private static void checkNoTempFilesRemain() {
		// validate that all temp files have been removed
		for (File dir : ioManager.getSpillingDirectories()) {
			for (String file : dir.list()) {
				if (file != null && !(file.equals(".") || file.equals(".."))) {
					fail("barrier buffer did not clean up temp files. remaining file: " + file);
				}
			}
		}
	}

	/**
	 * A validation matcher for checkpoint metadata against checkpoint IDs.
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
