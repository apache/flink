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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
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

import java.io.DataOutputStream;
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
 * Base class for adapters, as used by {@link StreamFormatAdapterTest} and {@link FileRecordFormatAdapterTest}.
 */
public abstract class AdapterTestBase<FormatT> {

	@ClassRule
	public static final TemporaryFolder TMP_DIR = new TemporaryFolder();

	protected static final int NUM_NUMBERS = 100;
	protected static final long FILE_LEN = 4 * NUM_NUMBERS;

	protected static Path testPath;

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
	//  format specific instantiation
	// ------------------------------------------------------------------------

	protected abstract FormatT createCheckpointedFormat();

	protected abstract FormatT createNonCheckpointedFormat();

	protected abstract FormatT createFormatFailingInInstantiation();

	protected abstract BulkFormat<Integer> wrapWithAdapter(FormatT format);

	// ------------------------------------------------------------------------
	//  shared tests
	// ------------------------------------------------------------------------

	@Test
	public void testRecoverCheckpointedFormatOneSplit() throws IOException {
		testReading(createCheckpointedFormat(), 1, 5, 44);
	}

	@Test
	public void testRecoverCheckpointedFormatMultipleSplits() throws IOException {
		testReading(createCheckpointedFormat(), 3, 11, 33, 56);
	}

	@Test
	public void testRecoverNonCheckpointedFormatOneSplit() throws IOException {
		testReading(createNonCheckpointedFormat(), 1, 5, 44);
	}

	private void testReading(FormatT format, int numSplits, int... recoverAfterRecords) throws IOException {
		// add the end boundary for recovery
		final int[] boundaries = Arrays.copyOf(recoverAfterRecords, recoverAfterRecords.length + 1);
		boundaries[boundaries.length - 1] = NUM_NUMBERS;

		// set a fetch size so that we get three records per fetch
		final Configuration config = new Configuration();
		config.set(StreamFormat.FETCH_IO_SIZE, new MemorySize(10));

		final BulkFormat<Integer> adapter = wrapWithAdapter(format);
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
		final Path testPath = new Path("testFs:///testpath-1");
		final CloseTestingInputStream in = new CloseTestingInputStream();
		final TestingFileSystem testFs = TestingFileSystem.createForFileStatus("testFs",
				TestingFileSystem.TestFileStatus.forFileWithStream(testPath, 1024, in));
		testFs.register();

		// test
		final BulkFormat<Integer> adapter = wrapWithAdapter(createFormatFailingInInstantiation());
		try {
			adapter.createReader(new Configuration(), testPath, 0, 1024);
		} catch (IOException ignored) {}

		// assertions
		assertTrue(in.closed);

		// cleanup
		testFs.unregister();
	}

	@Test
	public void testClosesStreamIfReaderRestoreFails() throws Exception {
		// setup
		final Path testPath = new Path("testFs:///testpath-1");
		final CloseTestingInputStream in = new CloseTestingInputStream();
		final TestingFileSystem testFs = TestingFileSystem.createForFileStatus("testFs",
			TestingFileSystem.TestFileStatus.forFileWithStream(testPath, 1024, in));
		testFs.register();

		// test
		final BulkFormat<Integer> adapter = wrapWithAdapter(createFormatFailingInInstantiation());
		try {
			adapter.restoreReader(
					new Configuration(), testPath, 0, 1024,
					new CheckpointedPosition(0L, 5L));
		} catch (IOException ignored) {}

		// assertions
		assertTrue(in.closed);

		// cleanup
		testFs.unregister();
	}

	// ------------------------------------------------------------------------
	//  test helpers
	// ------------------------------------------------------------------------

	protected static void verifyIntListResult(List<Integer> result) {
		assertEquals("wrong result size", NUM_NUMBERS, result.size());
		int nextExpected = 0;
		for (int next : result) {
			if (next != nextExpected++) {
				fail("Wrong result: " + result);
			}
		}
	}

	protected static void readNumbers(BulkFormat.Reader<Integer> reader, List<Integer> result, int num) throws IOException {
		readNumbers(reader, null, null, null, null, result, num);
	}

	protected static Tuple2<FileSourceSplit, CheckpointedPosition> readNumbers(
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

	static Queue<FileSourceSplit> buildSplits(int numSplits) {
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
	//  Test Mocks and Stubs
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
