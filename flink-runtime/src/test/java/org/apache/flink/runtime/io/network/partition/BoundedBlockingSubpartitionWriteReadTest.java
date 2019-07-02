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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests that read the BoundedBlockingSubpartition with multiple threads in parallel.
 */
@RunWith(Parameterized.class)
public class BoundedBlockingSubpartitionWriteReadTest {

	@ClassRule
	public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

	private static final int BUFFER_SIZE = 1024 * 1024;

	// ------------------------------------------------------------------------
	//  parameters
	// ------------------------------------------------------------------------

	@Parameters(name = "type = {0}")
	public static Collection<Object[]> modes() {
		return Arrays.stream(BoundedBlockingSubpartitionType.values())
				.map((type) -> new Object[] { type })
				.collect(Collectors.toList());
	}

	@Parameter
	public BoundedBlockingSubpartitionType type;

	// ------------------------------------------------------------------------
	//  tests
	// ------------------------------------------------------------------------

	@Test
	public void testWriteAndReadData() throws Exception {
		final int numLongs = 15_000_000; // roughly 115 MiBytes

		// setup
		final BoundedBlockingSubpartition subpartition = createAndFillPartition(numLongs);

		// test & check
		final ResultSubpartitionView reader = subpartition.createReadView(() -> {});
		readLongs(reader, numLongs, subpartition.getBuffersInBacklog());

		// cleanup
		reader.releaseAllResources();
		subpartition.release();
	}

	@Test
	public void testRead10ConsumersSequential() throws Exception {
		final int numLongs = 10_000_000;

		// setup
		final BoundedBlockingSubpartition subpartition = createAndFillPartition(numLongs);

		// test & check
		for (int i = 0; i < 10; i++) {
			final ResultSubpartitionView reader = subpartition.createReadView(() -> {});
			readLongs(reader, numLongs, subpartition.getBuffersInBacklog());
			reader.releaseAllResources();
		}

		// cleanup
		subpartition.release();
	}

	@Test
	public void testRead10ConsumersConcurrent() throws Exception {
		final int numLongs = 15_000_000;

		// setup
		final BoundedBlockingSubpartition subpartition = createAndFillPartition(numLongs);

		// test
		final LongReader[] readerThreads = createSubpartitionLongReaders(
				subpartition, 10, numLongs, subpartition.getBuffersInBacklog());
		for (CheckedThread t : readerThreads) {
			t.start();
		}

		// check
		for (CheckedThread t : readerThreads) {
			t.sync(); // this propagates assertion errors out from the threads
		}

		// cleanup
		subpartition.release();
	}

	// ------------------------------------------------------------------------
	//  common test passes
	// ------------------------------------------------------------------------

	private static void readLongs(ResultSubpartitionView reader, long numLongs, int numBuffers) throws Exception {
		BufferAndBacklog next;
		long expectedNextLong = 0L;
		int nextExpectedBacklog = numBuffers - 1;

		while ((next = reader.getNextBuffer()) != null && next.buffer().isBuffer()) {
			assertTrue(next.isMoreAvailable());
			assertEquals(nextExpectedBacklog, next.buffersInBacklog());

			ByteBuffer buffer = next.buffer().getNioBufferReadable();
			while (buffer.hasRemaining()) {
				assertEquals(expectedNextLong++, buffer.getLong());
			}

			next.buffer().recycleBuffer();
			nextExpectedBacklog--;
		}

		assertEquals(numLongs, expectedNextLong);
		assertEquals(-1, nextExpectedBacklog);
	}

	// ------------------------------------------------------------------------
	//  utils
	// ------------------------------------------------------------------------

	private static void writeLongs(BoundedBlockingSubpartition partition, long nums) throws IOException {
		final MemorySegment memory = MemorySegmentFactory.allocateUnpooledSegment(BUFFER_SIZE);

		long l = 0;
		while (nums > 0) {
			int pos = 0;
			for (; nums > 0 && pos <= memory.size() - 8; pos += 8) {
				memory.putLongBigEndian(pos, l++);
				nums--;
			}

			partition.add(new BufferConsumer(memory, (ignored) -> {}, pos, true));

			// we need to flush after every buffer as long as the add() contract is that
			// buffer are immediately added and can be filled further after that (for low latency
			// streaming data exchanges)
			partition.flush();
		}
	}

	private BoundedBlockingSubpartition createAndFillPartition(long numLongs) throws IOException {
		BoundedBlockingSubpartition subpartition = createSubpartition();
		writeLongs(subpartition, numLongs);
		subpartition.finish();
		return subpartition;
	}

	private BoundedBlockingSubpartition createSubpartition() throws IOException {
		return type.create(
				0,
				PartitionTestUtils.createPartition(ResultPartitionType.BLOCKING),
				new File(TMP_FOLDER.newFolder(), "partitiondata"),
				BUFFER_SIZE);
	}

	private static LongReader[] createSubpartitionLongReaders(
			BoundedBlockingSubpartition subpartition,
			int numReaders,
			int numLongs,
			int numBuffers) throws IOException {

		final LongReader[] readerThreads = new LongReader[numReaders];
		for (int i = 0; i < numReaders; i++) {
			ResultSubpartitionView reader = subpartition.createReadView(() -> {});
			readerThreads[i] = new LongReader(reader, numLongs, numBuffers);
		}
		return readerThreads;
	}

	private static final class LongReader extends CheckedThread {

		private final ResultSubpartitionView reader;

		private final long numLongs;

		private final int numBuffers;

		LongReader(ResultSubpartitionView reader, long numLongs, int numBuffers) {
			this.reader = reader;
			this.numLongs = numLongs;
			this.numBuffers = numBuffers;
		}

		@Override
		public void go() throws Exception {
			readLongs(reader, numLongs, numBuffers);
		}
	}
}
