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

package org.apache.flink.connector.file.src.impl;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.reader.SimpleStreamFormat;
import org.apache.flink.connector.file.src.reader.StreamFormat;
import org.apache.flink.connector.file.src.testutils.TestingFileSystem;
import org.apache.flink.connector.file.src.util.CheckpointedPosition;
import org.apache.flink.connector.file.src.util.RecordAndPosition;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.Path;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit and behavior tests for the {@link StreamFormatAdapter}.
 */
@SuppressWarnings("serial")
public class StreamFormatAdapterTest {

	@ClassRule
	public static final TemporaryFolder TMP_DIR = new TemporaryFolder();

	private static final int NUM_NUMBERS = 100;
	private static final long FILE_LEN = 4 * NUM_NUMBERS;

	private static Path testPath;

	@BeforeClass
	public static void writeTestFile() throws IOException {
		final File testFile = new File(TMP_DIR.getRoot(), "testFile");
		testPath = Path.fromLocalFile(testFile);

		try (DataOutputStream out = new DataOutputStream(new FileOutputStream(testFile))) {
			for (int i = 0; i < NUM_NUMBERS; i++) {
				out.writeInt(i);
			}
		}
	}

	// ------------------------------------------------------------------------

	@Test
	public void testReadSmallBatchSize() throws IOException {
		simpleReadTest(1);
	}

	@Test
	public void testBatchSizeMatchesOneRecord() throws IOException {
		simpleReadTest(4);
	}

	@Test
	public void testBatchSizeIsRecordMultiple() throws IOException {
		simpleReadTest(20);
	}

	private void simpleReadTest(int batchSize) throws IOException {
		final Configuration config = new Configuration();
		config.set(StreamFormat.FETCH_IO_SIZE, new MemorySize(batchSize));
		final StreamFormatAdapter<Integer> format = new StreamFormatAdapter<>(new CheckpointedIntFormat());
		final BulkFormat.Reader<Integer> reader = format.createReader(config, testPath, 0L, FILE_LEN);

		final List<Integer> result = new ArrayList<>();
		readNumbers(reader, result, NUM_NUMBERS);

		verifyIntListResult(result);
	}

	// ------------------------------------------------------------------------

	@Test
	public void testRecoverCheckpointedFormatOneSplit() throws IOException {
		testReading(new CheckpointedIntFormat(), 1, 5, 44);
	}

	@Test
	public void testRecoverCheckpointedFormatMultipleSplits() throws IOException {
		testReading(new CheckpointedIntFormat(), 3, 11, 33, 56);
	}

	@Test
	public void testRecoverNonCheckpointedFormatOneSplit() throws IOException {
		testReading(new NonCheckpointedIntFormat(), 1, 5, 44);
	}

	private void testReading(StreamFormat<Integer> format, int numSplits, int... recoverAfterRecords) throws IOException {
		// add the end boundary for recovery
		final int[] boundaries = Arrays.copyOf(recoverAfterRecords, recoverAfterRecords.length + 1);
		boundaries[boundaries.length - 1] = NUM_NUMBERS;

		// set a fetch size so that we get three records per fetch
		final Configuration config = new Configuration();
		config.set(StreamFormat.FETCH_IO_SIZE, new MemorySize(10));

		final StreamFormatAdapter<Integer> adapter = new StreamFormatAdapter<>(format);
		final Queue<FileSourceSplit> splits = buildSplits(numSplits);
		final List<Integer> result = new ArrayList<>();

		FileSourceSplit currentSplit = null;
		BulkFormat.Reader<Integer> currentReader = null;

		for (int nextRecordToRecover : boundaries) {
			final Tuple2<FileSourceSplit, CheckpointedPosition> toRecoverFrom = readNumbers(
				currentReader, currentSplit,
				adapter, splits, config,
				result,
				nextRecordToRecover - result.size());

			currentSplit = toRecoverFrom.f0;
			currentReader = adapter.restoreReader(config, currentSplit.path(), currentSplit.offset(), currentSplit.length(), toRecoverFrom.f1);
		}

		verifyIntListResult(result);
	}

	// ------------------------------------------------------------------------

	@Test
	public void testClosesStreamIfReaderCreationFails() throws Exception {
		// setup
		final Path testPath1 = new Path("testFs:///testpath-1");
		final Path testPath2 = new Path("testFs:///testpath-2");

		final CloseTestingInputStream in1 = new CloseTestingInputStream();
		final CloseTestingInputStream in2 = new CloseTestingInputStream();

		final TestingFileSystem testFs = TestingFileSystem.createForFileStatus("testFs",
				TestingFileSystem.TestFileStatus.forFileWithStream(testPath1, 1024, in1),
				TestingFileSystem.TestFileStatus.forFileWithStream(testPath2, 1024, in2));
		testFs.register();

		// test
		final StreamFormatAdapter<Integer> adapter = new StreamFormatAdapter<>(new FailingInstantiationFormat());
		try {
			adapter.createReader(new Configuration(), testPath1, 0, 1024);
		} catch (IOException ignored) {}
		try {
			adapter.restoreReader(
					new Configuration(), testPath2, 0, 1024,
					new CheckpointedPosition(0L, 0L));
		} catch (IOException ignored) {}

		// assertions
		assertTrue(in1.closed);
		assertTrue(in2.closed);

		// cleanup
		testFs.unregister();
	}

	// ------------------------------------------------------------------------
	//  test helpers
	// ------------------------------------------------------------------------

	private static void verifyIntListResult(List<Integer> result) {
		assertEquals("wrong result size", NUM_NUMBERS, result.size());
		int nextExpected = 0;
		for (int next : result) {
			if (next != nextExpected++) {
				fail("Wrong result: " + result);
			}
		}
	}

	private static void readNumbers(BulkFormat.Reader<Integer> reader, List<Integer> result, int num) throws IOException {
		readNumbers(reader, null, null, null, null, result, num);
	}

	private static Tuple2<FileSourceSplit, CheckpointedPosition> readNumbers(
			BulkFormat.Reader<Integer> currentReader,
			FileSourceSplit currentSplit,
			BulkFormat<Integer> format,
			Queue<FileSourceSplit> moreSplits,
			Configuration config,
			List<Integer> result,
			int num) throws IOException {

		long offset = Long.MIN_VALUE;
		long skip = Long.MIN_VALUE;

		// loop across splits
		while (num > 0) {
			if (currentReader == null) {
				currentSplit = moreSplits.poll();
				assertNotNull(currentSplit);
				currentReader = format.createReader(config, currentSplit.path(), currentSplit.offset(), currentSplit.length());
			}

			// loop across batches
			BulkFormat.RecordIterator<Integer> nextBatch;
			while (num > 0 && (nextBatch = currentReader.readBatch()) != null) {

				// loop across record in batch
				RecordAndPosition<Integer> next;
				while (num > 0 && (next = nextBatch.next()) != null) {
					num--;
					result.add(next.getRecord());
					offset = next.getOffset();
					skip = next.getRecordSkipCount();
				}
			}

			currentReader.close();
			currentReader = null;
		}

		return new Tuple2<>(currentSplit, new CheckpointedPosition(offset, skip));
	}

	private static Queue<FileSourceSplit> buildSplits(int numSplits) {
		final Queue<FileSourceSplit>  splits = new ArrayDeque<>();
		final long rangeForSplit = FILE_LEN / numSplits;

		for (int i = 0; i < numSplits - 1; i++) {
			splits.add(new FileSourceSplit("ID-" + i, testPath, i * rangeForSplit, rangeForSplit));
		}
		final long startOfLast = (numSplits - 1) * rangeForSplit;
		splits.add(new FileSourceSplit("ID-" + (numSplits - 1), testPath, startOfLast, FILE_LEN - startOfLast));
		return splits;
	}

	// ------------------------------------------------------------------------
	//  test mocks
	// ------------------------------------------------------------------------

	private static class CheckpointedIntFormat implements StreamFormat<Integer> {

		@Override
		public Reader<Integer> createReader(
				Configuration config,
				FSDataInputStream stream,
				long fileLen,
				long splitEnd) throws IOException {

			assertEquals("invalid file length", 0, fileLen % 4);

			// round all positions to the next integer boundary
			// to simulate common split behavior, we round up to the next int boundary even when we
			// are at a perfect boundary. exceptions are if we are start or end.
			final long currPos = stream.getPos();
			final long start = currPos == 0L ? 0L : currPos + 4 - currPos % 4;
			final long end = splitEnd == fileLen ? fileLen : splitEnd + 4 - splitEnd % 4;
			stream.seek(start);

			return new CheckpointedIntReader(stream, end);
		}

		@Override
		public Reader<Integer> restoreReader(
				Configuration config,
				FSDataInputStream stream,
				long restoredOffset,
				long fileLen,
				long splitEnd) throws IOException {

			assertEquals("invalid file length", 0, fileLen % 4);

			// round end position to the next integer boundary
			final long end = splitEnd == fileLen ? fileLen : splitEnd + 4 - splitEnd % 4;
			// no rounding of checkpointed offset
			stream.seek(restoredOffset);
			return new CheckpointedIntReader(stream, end);
		}

		@Override
		public boolean isSplittable() {
			return true;
		}

		@Override
		public TypeInformation<Integer> getProducedType() {
			return Types.INT;
		}
	}

	private static class CheckpointedIntReader implements StreamFormat.Reader<Integer> {

		private static final int SKIPS_PER_OFFSET = 7;

		private final FSDataInputStream in;
		private final DataInputStream din;

		private final long endOffset;
		private long currentOffset;
		private long currentSkipCount;

		CheckpointedIntReader(FSDataInputStream in, long endOffset) throws IOException {
			this.in = in;
			this.endOffset = endOffset;
			this.currentOffset = in.getPos();
			this.din = new DataInputStream(in);
		}

		@Nullable
		@Override
		public Integer read() throws IOException {
			if (in.getPos() >= endOffset) {
				return null;
			}

			try {
				final int next = din.readInt();
				incrementPosition();
				return next;
			} catch (EOFException e) {
				return null;
			}
		}

		@Override
		public void close() throws IOException {
			in.close();
		}

		@Nullable
		@Override
		public CheckpointedPosition getCheckpointedPosition() {
			return new CheckpointedPosition(currentOffset, currentSkipCount);
		}

		private void incrementPosition() {
			currentSkipCount++;
			if (currentSkipCount >= SKIPS_PER_OFFSET) {
				currentOffset += 4 * currentSkipCount;
				currentSkipCount = 0;
			}
		}
	}

	private static class NonCheckpointedIntFormat extends SimpleStreamFormat<Integer> {

		@Override
		public Reader<Integer> createReader(Configuration config, FSDataInputStream stream) throws IOException {
			return new NonCheckpointedIntReader(stream);
		}

		@Override
		public TypeInformation<Integer> getProducedType() {
			return Types.INT;
		}
	}

	private static class NonCheckpointedIntReader extends CheckpointedIntReader {

		NonCheckpointedIntReader(FSDataInputStream in) throws IOException {
			super(in, Long.MAX_VALUE);
		}

		@Nullable
		@Override
		public CheckpointedPosition getCheckpointedPosition() {
			return null;
		}
	}

	// ------------------------------------------------------------------------

	private static class FailingInstantiationFormat implements StreamFormat<Integer> {
		@Override
		public Reader<Integer> createReader(
				Configuration config,
				FSDataInputStream stream,
				long fileLen,
				long splitEnd) throws IOException {
			throw new IOException("test exception");
		}

		@Override
		public Reader<Integer> restoreReader(
				Configuration config,
				FSDataInputStream stream,
				long restoredOffset,
				long fileLen,
				long splitEnd) throws IOException {
			throw new IOException("test exception");
		}

		@Override
		public boolean isSplittable() {
			return false;
		}

		@Override
		public TypeInformation<Integer> getProducedType() {
			return Types.INT;
		}
	}

	// ------------------------------------------------------------------------

	private static class CloseTestingInputStream extends FSDataInputStream {

		boolean closed;

		@Override
		public void seek(long desired) throws IOException {}

		@Override
		public long getPos() throws IOException {
			return 0;
		}

		@Override
		public int read() throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public void close() throws IOException {
			closed = true;
		}
	}
}
