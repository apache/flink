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

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.mocks.MockSourceReader;
import org.apache.flink.connector.base.source.reader.mocks.MockSplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.base.source.reader.synchronization.FutureNotifier;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;

/**
 * A unit test class for {@link SourceReaderBase}.
 */
public class SourceReaderBaseTest extends SourceReaderTestBase<MockSourceSplit> {

	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	@Test
	public void testExceptionInSplitReader() throws Exception {
		expectedException.expect(RuntimeException.class);
		expectedException.expectMessage("One or more fetchers have encountered exception");
		final String errMsg = "Testing Exception";

		FutureNotifier futureNotifier = new FutureNotifier();
		FutureCompletingBlockingQueue<RecordsWithSplitIds<int[]>> elementsQueue =
				new FutureCompletingBlockingQueue<>(futureNotifier);
		// We have to handle split changes first, otherwise fetch will not be called.
		try (MockSourceReader reader = new MockSourceReader(
				futureNotifier,
				elementsQueue,
				() -> new SplitReader<int[], MockSourceSplit>() {
					@Override
					public RecordsWithSplitIds<int[]> fetch() {
						throw new RuntimeException(errMsg);
					}

					@Override
					public void handleSplitsChanges(Queue<SplitsChange<MockSourceSplit>> splitsChanges) {
						// We have to handle split changes first, otherwise fetch will not be called.
						splitsChanges.clear();
					}

					@Override
					public void wakeUp() {
					}
				},
				getConfig(),
				null)) {
			ValidatingSourceOutput output = new ValidatingSourceOutput();
			reader.addSplits(Collections.singletonList(getSplit(0,
					NUM_RECORDS_PER_SPLIT,
					Boundedness.CONTINUOUS_UNBOUNDED)));
			// This is not a real infinite loop, it is supposed to throw exception after two polls.
			while (true) {
				reader.pollNext(output);
				// Add a sleep to avoid tight loop.
				Thread.sleep(1);
			}
		}
	}

	// ---------------- helper methods -----------------

	@Override
	protected MockSourceReader createReader() {
		FutureNotifier futureNotifier = new FutureNotifier();
		FutureCompletingBlockingQueue<RecordsWithSplitIds<int[]>> elementsQueue =
				new FutureCompletingBlockingQueue<>(futureNotifier);
		MockSplitReader mockSplitReader =
				new MockSplitReader(2, true, true);
		return new MockSourceReader(
				futureNotifier,
				elementsQueue,
				() -> mockSplitReader,
				getConfig(),
				null);
	}

	@Override
	protected List<MockSourceSplit> getSplits(int numSplits, int numRecordsPerSplit, Boundedness boundedness) {
		List<MockSourceSplit> mockSplits = new ArrayList<>();
		for (int i = 0; i < numSplits; i++) {
			mockSplits.add(getSplit(i, numRecordsPerSplit, boundedness));
		}
		return mockSplits;
	}

	@Override
	protected MockSourceSplit getSplit(int splitId, int numRecords, Boundedness boundedness) {
		MockSourceSplit mockSplit;
		if (boundedness == Boundedness.BOUNDED) {
			mockSplit = new MockSourceSplit(splitId, 0, numRecords);
		} else {
			mockSplit = new MockSourceSplit(splitId);
		}
		for (int j = 0; j < numRecords; j++) {
			mockSplit.addRecord(splitId * 10 + j);
		}
		return mockSplit;
	}

	@Override
	protected long getIndex(MockSourceSplit split) {
		return split.index();
	}

	private Configuration getConfig() {
		Configuration config = new Configuration();
		config.setInteger(SourceReaderOptions.ELEMENT_QUEUE_CAPACITY, 1);
		config.setLong(SourceReaderOptions.SOURCE_READER_CLOSE_TIMEOUT, 30000L);
		return config;
	}
}
