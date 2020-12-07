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
import org.apache.flink.runtime.io.disk.FileChannelManager;
import org.apache.flink.runtime.io.disk.FileChannelManagerImpl;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.BufferDecompressor;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;
import org.apache.flink.runtime.util.EnvironmentInformation;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
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

	private static final String tempDir = EnvironmentInformation.getTemporaryFileDirectory();

	private static FileChannelManager fileChannelManager;

	@ClassRule
	public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

	private static final int BUFFER_SIZE = 1024 * 1024;

	private static final String COMPRESSION_CODEC = "LZ4";

	private static final BufferDecompressor decompressor = new BufferDecompressor(BUFFER_SIZE, COMPRESSION_CODEC);

	// ------------------------------------------------------------------------
	//  parameters
	// ------------------------------------------------------------------------

	private final BoundedBlockingSubpartitionType type;

	private final boolean compressionEnabled;

	private final boolean sslEnabled;

	@Parameters(name = "type = {0}, compressionEnabled = {1}")
	public static Collection<Object[]> parameters() {
		return Arrays.stream(BoundedBlockingSubpartitionType.values())
				.map((type) -> new Object[][] { { type, true }, { type, false } })
				.flatMap(Arrays::stream)
				.collect(Collectors.toList());
	}

	public BoundedBlockingSubpartitionWriteReadTest(BoundedBlockingSubpartitionType type, boolean compressionEnabled) {
		this.type = type;
		this.compressionEnabled = compressionEnabled;
		// we can also make use of the same flag since they are completely irrelevant
		this.sslEnabled = compressionEnabled;
	}

	// ------------------------------------------------------------------------
	//  tests
	// ------------------------------------------------------------------------

	@BeforeClass
	public static void setUp() {
		fileChannelManager = new FileChannelManagerImpl(new String[] {tempDir}, "testing");
	}

	@AfterClass
	public static void shutdown() throws Exception {
		fileChannelManager.close();
	}

	@Test
	public void testWriteAndReadData() throws Exception {
		final int numLongs = 15_000_000; // roughly 115 MiBytes

		// setup
		final BoundedBlockingSubpartition subpartition = createAndFillPartition(numLongs);

		// test & check
		final ResultSubpartitionView reader = subpartition.createReadView(() -> {});
		readLongs(reader, numLongs, subpartition.getBuffersInBacklog(), compressionEnabled, decompressor);

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
			readLongs(reader, numLongs, subpartition.getBuffersInBacklog(), compressionEnabled, decompressor);
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
				subpartition, 10, numLongs, subpartition.getBuffersInBacklog(), compressionEnabled);
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

	private static void readLongs(
			ResultSubpartitionView reader,
			long numLongs,
			int numBuffers,
			boolean compressionEnabled,
			BufferDecompressor decompressor) throws Exception {
		BufferAndBacklog next;
		long expectedNextLong = 0L;
		int nextExpectedBacklog = numBuffers - 1;

		while ((next = reader.getNextBuffer()) != null && next.buffer().isBuffer()) {
			assertTrue(next.isDataAvailable());
			assertEquals(nextExpectedBacklog, next.buffersInBacklog());

			ByteBuffer buffer = next.buffer().getNioBufferReadable();
			if (compressionEnabled && next.buffer().isCompressed()) {
				Buffer uncompressedBuffer = decompressor.decompressToIntermediateBuffer(next.buffer());
				buffer = uncompressedBuffer.getNioBufferReadable();
				uncompressedBuffer.recycleBuffer();
			}
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

			partition.add(new BufferConsumer(memory, (ignored) -> {}, pos, Buffer.DataType.DATA_BUFFER));

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
					(BoundedBlockingResultPartition) PartitionTestUtils.createPartition(
					ResultPartitionType.BLOCKING,
					fileChannelManager,
					compressionEnabled,
					BUFFER_SIZE),
				new File(TMP_FOLDER.newFolder(), "partitiondata"),
				BUFFER_SIZE,
				sslEnabled);
	}

	private static LongReader[] createSubpartitionLongReaders(
			BoundedBlockingSubpartition subpartition,
			int numReaders,
			int numLongs,
			int numBuffers,
			boolean compressionEnabled) throws IOException {

		final LongReader[] readerThreads = new LongReader[numReaders];
		for (int i = 0; i < numReaders; i++) {
			ResultSubpartitionView reader = subpartition.createReadView(() -> {});
			readerThreads[i] = new LongReader(reader, numLongs, numBuffers, compressionEnabled);
		}
		return readerThreads;
	}

	private static final class LongReader extends CheckedThread {

		private final ResultSubpartitionView reader;

		private final long numLongs;

		private final int numBuffers;

		private final boolean compressionEnabled;

		private final BufferDecompressor decompressor;

		LongReader(ResultSubpartitionView reader, long numLongs, int numBuffers, boolean compressionEnabled) {
			this.reader = reader;
			this.numLongs = numLongs;
			this.numBuffers = numBuffers;
			this.compressionEnabled = compressionEnabled;
			this.decompressor = new BufferDecompressor(BUFFER_SIZE, COMPRESSION_CODEC);
		}

		@Override
		public void go() throws Exception {
			readLongs(reader, numLongs, numBuffers, compressionEnabled, decompressor);
		}
	}
}
