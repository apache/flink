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
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;

import org.hamcrest.collection.IsIterableContainingInOrder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import static org.apache.flink.util.ExceptionUtils.findThrowable;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the {@link CheckpointBarrierAligner}'s maximum limit of buffered/spilled bytes.
 */
public class CheckpointBarrierAlignerAlignmentLimitTest {

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
	public static void shutdownIOManager() throws Exception {
		ioManager.close();
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
		MockInputGate gate = new MockInputGate(3, Arrays.asList(sequence));
		CheckpointNotificationVerifier toNotify = new CheckpointNotificationVerifier();
		CheckpointedInputGate buffer = new CheckpointedInputGate(
			gate,
			new CachedBufferStorage(PAGE_SIZE, PAGE_SIZE * 2, "Testing"),
			"Testing",
			toNotify);

		// validating the sequence of buffers

		check(sequence[0], buffer.pollNext().get());
		check(sequence[1], buffer.pollNext().get());
		check(sequence[2], buffer.pollNext().get());
		check(sequence[3], buffer.pollNext().get());

		// start of checkpoint
		long startTs = System.nanoTime();
		check(sequence[6], buffer.pollNext().get());
		check(sequence[8], buffer.pollNext().get());
		check(sequence[10], buffer.pollNext().get());

		// trying to pull the next makes the alignment overflow - so buffered buffers are replayed
		try (AssertCheckpointWasAborted assertion =
				toNotify.expectAbortCheckpoint(
					7L,
					CheckpointFailureReason.CHECKPOINT_DECLINED_ALIGNMENT_LIMIT_EXCEEDED)) {
			check(sequence[5], buffer.pollNext().get());
			validateAlignmentTime(startTs, buffer);
		}

		// playing back buffered events
		check(sequence[7], buffer.pollNext().get());
		check(sequence[11], buffer.pollNext().get());
		check(sequence[12], buffer.pollNext().get());
		check(sequence[13], buffer.pollNext().get());
		check(sequence[14], buffer.pollNext().get());

		// the additional data
		check(sequence[15], buffer.pollNext().get());
		check(sequence[16], buffer.pollNext().get());
		check(sequence[17], buffer.pollNext().get());

		check(sequence[19], buffer.pollNext().get());
		check(sequence[20], buffer.pollNext().get());
		check(sequence[21], buffer.pollNext().get());

		// no call for a completed checkpoint must have happened
		assertTrue(toNotify.getAndResetTriggeredCheckpoints().isEmpty());

		assertFalse(buffer.pollNext().isPresent());
		assertTrue(buffer.isFinished());

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
		MockInputGate gate = new MockInputGate(3, Arrays.asList(sequence));

		CheckpointNotificationVerifier toNotify = new CheckpointNotificationVerifier();
		CheckpointedInputGate buffer = new CheckpointedInputGate(
			gate,
			new CachedBufferStorage(PAGE_SIZE, PAGE_SIZE * 5, "Testing"),
			"Testing",
			toNotify);

		// validating the sequence of buffers
		long startTs;

		check(sequence[0], buffer.pollNext().get());
		check(sequence[1], buffer.pollNext().get());

		// start of checkpoint
		startTs = System.nanoTime();
		check(sequence[3], buffer.pollNext().get());
		check(sequence[7], buffer.pollNext().get());

		// next checkpoint also in progress
		check(sequence[11], buffer.pollNext().get());

		// checkpoint alignment aborted due to too much data
		try (AssertCheckpointWasAborted assertion =
				toNotify.expectAbortCheckpoint(
					3L,
					CheckpointFailureReason.CHECKPOINT_DECLINED_ALIGNMENT_LIMIT_EXCEEDED)) {
			check(sequence[4], buffer.pollNext().get());
			validateAlignmentTime(startTs, buffer);
		}

		// replay buffered data - in the middle, the alignment for checkpoint 4 starts
		check(sequence[6], buffer.pollNext().get());
		startTs = System.nanoTime();
		check(sequence[12], buffer.pollNext().get());

		// only checkpoint 4 is pending now - the last checkpoint 3 barrier will not trigger success
		check(sequence[17], buffer.pollNext().get());

		// checkpoint 4 completed - check and validate buffered replay
		check(sequence[9], buffer.pollNext().get());
		validateAlignmentTime(startTs, buffer);
		assertThat(
			toNotify.getAndResetTriggeredCheckpoints(),
			IsIterableContainingInOrder.contains(4L));

		check(sequence[10], buffer.pollNext().get());
		check(sequence[15], buffer.pollNext().get());
		check(sequence[16], buffer.pollNext().get());

		// trailing data
		check(sequence[19], buffer.pollNext().get());
		check(sequence[20], buffer.pollNext().get());
		check(sequence[21], buffer.pollNext().get());

		// only checkpoint 4 was successfully completed, not checkpoint 3
		assertTrue(toNotify.getAndResetTriggeredCheckpoints().isEmpty());

		assertFalse(buffer.pollNext().isPresent());
		assertTrue(buffer.isFinished());

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

	private static void validateAlignmentTime(long startTimestamp, CheckpointedInputGate buffer) {
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

	private static class CheckpointNotificationVerifier extends AbstractInvokable {
		private final List<Long> triggeredCheckpoints = new ArrayList<>();
		private long expectedAbortCheckpointId = -1;
		@Nullable
		private CheckpointFailureReason expectedCheckpointFailureReason;

		public CheckpointNotificationVerifier() {
			super(new MockEnvironmentBuilder().build());
		}

		@Override
		public void invoke() throws Exception {
		}

		@Override
		public void abortCheckpointOnBarrier(long checkpointId, Throwable cause) throws Exception {
			try {
				if (expectedAbortCheckpointId != checkpointId || !matchesExpectedCause(cause)) {
					throw new Exception(cause);
				}
			}
			finally {
				expectedAbortCheckpointId = -1;
				expectedCheckpointFailureReason = null;
			}
		}

		private boolean matchesExpectedCause(Throwable cause) {
			if (expectedCheckpointFailureReason == null) {
				return true;
			}
			Optional<CheckpointException> checkpointException = findThrowable(cause, CheckpointException.class);
			if (!checkpointException.isPresent()) {
				return false;
			}
			return checkpointException.get().getCheckpointFailureReason() == expectedCheckpointFailureReason;
		}

		public AssertCheckpointWasAborted expectAbortCheckpoint(
				long expectedAbortCheckpointId,
				CheckpointFailureReason expectedCheckpointFailureReason) {
			this.expectedAbortCheckpointId = expectedAbortCheckpointId;
			this.expectedCheckpointFailureReason = expectedCheckpointFailureReason;
			return new AssertCheckpointWasAborted(this);
		}

		@Override
		public void triggerCheckpointOnBarrier(
				CheckpointMetaData checkpointMetaData,
				CheckpointOptions checkpointOptions,
				CheckpointMetrics checkpointMetrics) throws Exception {
			triggeredCheckpoints.add(checkpointMetaData.getCheckpointId());
		}

		public List<Long> getAndResetTriggeredCheckpoints() {
			List<Long> copy = new ArrayList<>(triggeredCheckpoints);
			triggeredCheckpoints.clear();
			return copy;
		}
	}

	private static class AssertCheckpointWasAborted implements Closeable {
		private final CheckpointNotificationVerifier checkpointNotificationVerifier;

		public AssertCheckpointWasAborted(CheckpointNotificationVerifier checkpointNotificationVerifier) {
			this.checkpointNotificationVerifier = checkpointNotificationVerifier;
		}

		@Override
		public void close() throws IOException {
			assertEquals(
				String.format(
					"AbstractInvokable::abortCheckpointOnBarrier(%d) has not fired",
					checkpointNotificationVerifier.expectedAbortCheckpointId),
				-1L,
				checkpointNotificationVerifier.expectedAbortCheckpointId);
		}
	}
}
