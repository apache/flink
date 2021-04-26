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

package org.apache.flink.connector.base.source.reader.fetcher;

import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.mocks.MockSplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.base.source.reader.synchronization.FutureNotifier;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for {@link SplitFetcher}.
 */
public class SplitFetcherTest {

	@Test
	public void testWakeup() throws InterruptedException {
		final int numSplits = 3;
		final int numRecordsPerSplit = 10_000;
		final int interruptRecordsInterval = 10;
		final int numTotalRecords = numRecordsPerSplit * numSplits;

		FutureCompletingBlockingQueue<RecordsWithSplitIds<int[]>> elementQueue =
			new FutureCompletingBlockingQueue<>(new FutureNotifier(), 1);
		SplitFetcher<int[], MockSourceSplit> fetcher =
				new SplitFetcher<>(
						0,
						elementQueue,
						new MockSplitReader(2, true, true),
						() -> {});

		// Prepare the splits.
		List<MockSourceSplit> splits = new ArrayList<>();
		for (int i = 0; i < numSplits; i++) {
			splits.add(new MockSourceSplit(i, 0, numRecordsPerSplit));
			int base = i * numRecordsPerSplit;
			for (int j = base; j < base + numRecordsPerSplit; j++) {
				splits.get(splits.size() - 1).addRecord(j);
			}
		}
		// Add splits to the fetcher.
		fetcher.addSplits(splits);

		// A thread drives the fetcher.
		Thread fetcherThread = new Thread(fetcher, "FetcherThread");

		SortedSet<Integer> recordsRead = Collections.synchronizedSortedSet(new TreeSet<>());

		// A thread waking up the split fetcher frequently.
		AtomicInteger wakeupTimes = new AtomicInteger(0);
		AtomicBoolean stop = new AtomicBoolean(false);
		Thread interrupter = new Thread("Interrupter") {
			@Override
			public void run() {
				int lastInterrupt = 0;
				while (recordsRead.size() < numTotalRecords && !stop.get()) {
					int numRecordsRead = recordsRead.size();
					if (numRecordsRead >= lastInterrupt + interruptRecordsInterval) {
						fetcher.wakeUp(false);
						wakeupTimes.incrementAndGet();
						lastInterrupt = numRecordsRead;
					}
				}
			}
		};

		try {
			fetcherThread.start();
			interrupter.start();

			while (recordsRead.size() < numSplits * numRecordsPerSplit) {
				final RecordsWithSplitIds<int[]> nextBatch = elementQueue.take();
				while (nextBatch.nextSplit() != null) {
					int[] arr;
					while ((arr = nextBatch.nextRecordFromSplit()) != null) {
						assertTrue(recordsRead.add(arr[0]));
					}
				}
			}

			assertEquals(numTotalRecords, recordsRead.size());
			assertEquals(0, (int) recordsRead.first());
			assertEquals(numTotalRecords - 1, (int) recordsRead.last());
			assertTrue(wakeupTimes.get() > 0);
		} finally {
			stop.set(true);
			fetcher.shutdown();
			fetcherThread.join();
			interrupter.join();
		}
	}
}
