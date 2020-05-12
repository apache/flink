/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package org.apache.flink.connector.base.source.reader.fetcher;

import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.mocks.MockSplitReader;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for {@link SplitFetcher}.
 */
public class SplitFetcherTest {
	private static final int NUM_SPLITS = 3;
	private static final int NUM_RECORDS_PER_SPLIT = 10_000;
	private static final int INTERRUPT_RECORDS_INTERVAL = 10;
	private static final int NUM_TOTAL_RECORDS = NUM_RECORDS_PER_SPLIT * NUM_SPLITS;
	@Test
	public void testWakeup() throws InterruptedException {
		BlockingQueue<RecordsWithSplitIds<int[]>> elementQueue = new ArrayBlockingQueue<>(1);
		SplitFetcher<int[], MockSourceSplit> fetcher =
				new SplitFetcher<>(
						0,
						elementQueue,
						new MockSplitReader(2, true, true),
						() -> {});

		// Prepare the splits.
		List<MockSourceSplit> splits = new ArrayList<>();
		for (int i = 0; i < NUM_SPLITS; i++) {
			splits.add(new MockSourceSplit(i, 0, NUM_RECORDS_PER_SPLIT));
			int base = i * NUM_RECORDS_PER_SPLIT;
			for (int j = base; j < base + NUM_RECORDS_PER_SPLIT; j++) {
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
				while (recordsRead.size() < NUM_TOTAL_RECORDS && !stop.get()) {
					int numRecordsRead = recordsRead.size();
					if (numRecordsRead >= lastInterrupt + INTERRUPT_RECORDS_INTERVAL) {
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

			while (recordsRead.size() < NUM_SPLITS * NUM_RECORDS_PER_SPLIT) {
				elementQueue.take().recordsBySplits().values().forEach(records ->
						// Ensure there is no duplicate records.
						records.forEach(arr -> assertTrue(recordsRead.add(arr[0]))));
			}

			assertEquals(NUM_TOTAL_RECORDS, recordsRead.size());
			assertEquals(0, (int) recordsRead.first());
			assertEquals(NUM_TOTAL_RECORDS - 1, (int) recordsRead.last());
			assertTrue(wakeupTimes.get() > 0);
		} finally {
			stop.set(true);
			fetcher.shutdown();
			fetcherThread.join();
			interrupter.join();
		}
	}
}
