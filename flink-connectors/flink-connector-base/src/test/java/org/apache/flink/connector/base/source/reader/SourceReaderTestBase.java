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

package org.apache.flink.connector.base.source.reader;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * An abstract test class for all the unit tests of {@link SourceReader} to inherit.
 *
 * @param <SplitT> the type of the splits.
 */
public abstract class SourceReaderTestBase<SplitT extends SourceSplit> extends TestLogger {

	protected static final int NUM_SPLITS = 10;
	protected static final int NUM_RECORDS_PER_SPLIT = 10;
	protected static final int TOTAL_NUM_RECORDS = NUM_RECORDS_PER_SPLIT * NUM_SPLITS;

	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	@After
	public void ensureNoDangling() {
		for (Thread t : Thread.getAllStackTraces().keySet()) {
			if (t.getName().equals("SourceFetcher")) {
				System.out.println("Dangling thread.");
			}
		}
	}

	/**
	 * Simply test the reader reads all the splits fine.
	 */
	@Test
	public void testRead() throws Exception {
		try (SourceReader<Integer, SplitT> reader = createReader()) {
			reader.addSplits(getSplits(NUM_SPLITS, NUM_RECORDS_PER_SPLIT, Boundedness.BOUNDED));
			ValidatingSourceOutput output = new ValidatingSourceOutput();
			while (output.count < TOTAL_NUM_RECORDS) {
				reader.pollNext(output);
			}
			output.validate();
		}
	}

	@Test
	public void testAddSplitToExistingFetcher() throws Exception {
		Thread.sleep(10);
		ValidatingSourceOutput output = new ValidatingSourceOutput();
		// Add a split to start the fetcher.
		List<SplitT> splits = Collections.singletonList(getSplit(0, NUM_RECORDS_PER_SPLIT, Boundedness.BOUNDED));
		// Poll 5 records and let it block on the element queue which only have capacity of 1;
		try (SourceReader<Integer, SplitT> reader = consumeRecords(splits, output, 5)) {
			List<SplitT> newSplits = new ArrayList<>();
			for (int i = 1; i < NUM_SPLITS; i++) {
				newSplits.add(getSplit(i, NUM_RECORDS_PER_SPLIT, Boundedness.BOUNDED));
			}
			reader.addSplits(newSplits);

			while (output.count() < NUM_RECORDS_PER_SPLIT * NUM_SPLITS) {
				reader.pollNext(output);
			}
			output.validate();
		}
	}

	@Test (timeout = 30000L)
	public void testPollingFromEmptyQueue() throws Exception {
		ValidatingSourceOutput output = new ValidatingSourceOutput();
		List<SplitT> splits = Collections.singletonList(getSplit(0, NUM_RECORDS_PER_SPLIT, Boundedness.BOUNDED));
		// Consumer all the records in the s;oit.
		try (SourceReader<Integer, SplitT> reader = consumeRecords(splits, output, NUM_RECORDS_PER_SPLIT)) {
			// Now let the main thread poll again.
			assertEquals("The status should be ", InputStatus.NOTHING_AVAILABLE, reader.pollNext(output));
		}
	}

	@Test (timeout = 30000L)
	public void testAvailableOnEmptyQueue() throws Exception {
		// Consumer all the records in the split.
		try (SourceReader<Integer, SplitT> reader = createReader()) {
			CompletableFuture<?> future = reader.isAvailable();
			assertFalse("There should be no records ready for poll.", future.isDone());
			// Add a split to the reader so there are more records to be read.
			reader.addSplits(Collections.singletonList(getSplit(0, NUM_RECORDS_PER_SPLIT, Boundedness.BOUNDED)));
			// The future should be completed fairly soon. Otherwise the test will hit timeout and fail.
			future.get();
		}
	}

	@Test (timeout = 30000L)
	public void testSnapshot() throws Exception {
		ValidatingSourceOutput output = new ValidatingSourceOutput();
		// Add a split to start the fetcher.
		List<SplitT> splits = getSplits(NUM_SPLITS, NUM_RECORDS_PER_SPLIT, Boundedness.CONTINUOUS_UNBOUNDED);
		try (SourceReader<Integer, SplitT> reader =
				consumeRecords(splits, output, NUM_SPLITS * NUM_RECORDS_PER_SPLIT)) {
			List<SplitT> state = reader.snapshotState();
			assertEquals("The snapshot should only have 10 splits. ", NUM_SPLITS, state.size());
			for (int i = 0; i < NUM_SPLITS; i++) {
				assertEquals("The first four splits should have been fully consumed.",
					NUM_RECORDS_PER_SPLIT, getNextRecordIndex(state.get(i)));
			}
		}
	}

	// ---------------- helper methods -----------------

	protected abstract SourceReader<Integer, SplitT> createReader();

	protected abstract List<SplitT> getSplits(int numSplits, int numRecordsPerSplit, Boundedness boundedness);

	protected abstract SplitT getSplit(int splitId, int numRecords, Boundedness boundedness);

	protected abstract long getNextRecordIndex(SplitT split);

	private SourceReader<Integer, SplitT> consumeRecords(
		List<SplitT> splits,
		ValidatingSourceOutput output,
		int n) throws Exception {
		SourceReader<Integer, SplitT> reader = createReader();
		// Add splits to start the fetcher.
		reader.addSplits(splits);
		// Poll all the n records of the single split.
		while (output.count() < n) {
			reader.pollNext(output);
		}
		return reader;
	}

	// ---------------- helper classes -----------------

	/**
	 * A source output that validates the output.
	 */
	protected static class ValidatingSourceOutput implements ReaderOutput<Integer> {
		private Set<Integer> consumedValues = new HashSet<>();
		private int max = Integer.MIN_VALUE;
		private int min = Integer.MAX_VALUE;

		private int count = 0;

		@Override
		public void collect(Integer element) {
			max = Math.max(element, max);
			min = Math.min(element, min);
			count++;
			consumedValues.add(element);
		}

		@Override
		public void collect(Integer element, long timestamp) {
			collect(element);
		}

		public void validate() {

			assertEquals(String.format("Should be %d distinct elements in total", TOTAL_NUM_RECORDS),
				TOTAL_NUM_RECORDS, consumedValues.size());
			assertEquals(String.format("Should be %d elements in total", TOTAL_NUM_RECORDS), TOTAL_NUM_RECORDS, count);
			assertEquals("The min value should be 0", 0, min);
			assertEquals("The max value should be " + (TOTAL_NUM_RECORDS - 1), TOTAL_NUM_RECORDS - 1, max);
		}

		public int count() {
			return count;
		}

		@Override
		public void emitWatermark(Watermark watermark) {}

		@Override
		public void markIdle() {}

		@Override
		public SourceOutput<Integer> createOutputForSplit(String splitId) {
			return this;
		}

		@Override
		public void releaseOutputForSplit(String splitId) {}
	}
}
