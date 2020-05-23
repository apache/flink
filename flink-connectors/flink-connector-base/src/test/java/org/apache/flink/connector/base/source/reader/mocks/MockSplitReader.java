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

package org.apache.flink.connector.base.source.reader.mocks;

import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Queue;

/**
 * A mock split reader for unit tests. The mock split reader provides configurable behaviours.
 * 1. Blocking fetch or non blocking fetch.
 *    - A blocking fetch can only be waken up by an interruption.
 *    - A non-blocking fetch do not expect to be interrupted.
 * 2. handle splits changes in one handleSplitsChanges call or handle one change in each call
 *    of handleSplitsChanges.
 */
public class MockSplitReader implements SplitReader<int[], MockSourceSplit> {
	// Use LinkedHashMap for determinism.
	private final Map<String, MockSourceSplit> splits = new LinkedHashMap<>();
	private final int numRecordsPerSplitPerFetch;
	private final boolean blockingFetch;
	private final boolean handleSplitsInOneShot;
	private volatile Thread runningThread;

	public MockSplitReader(
			int numRecordsPerSplitPerFetch,
			boolean blockingFetch,
			boolean handleSplitsInOneShot) {
		this.numRecordsPerSplitPerFetch = numRecordsPerSplitPerFetch;
		this.blockingFetch = blockingFetch;
		this.handleSplitsInOneShot = handleSplitsInOneShot;
		this.runningThread = null;
	}

	@Override
	public RecordsWithSplitIds<int[]> fetch() throws InterruptedException {
		if (runningThread == null) {
			runningThread = Thread.currentThread();
		}
		return getRecords();
	}

	@Override
	public void handleSplitsChanges(Queue<SplitsChange<MockSourceSplit>> splitsChanges) {
		do {
			SplitsChange<MockSourceSplit> splitsChange = splitsChanges.poll();
			if (splitsChange instanceof SplitsAddition) {
				splitsChange.splits().forEach(s -> splits.put(s.splitId(), s));
			}
		} while (handleSplitsInOneShot && !splitsChanges.isEmpty());
	}

	@Override
	public void wakeUp() {
		if (blockingFetch && runningThread != null) {
			runningThread.interrupt();
		}
	}

	private RecordsBySplits<int[]> getRecords() {
		RecordsBySplits<int[]> records = new RecordsBySplits<>();
		try {
			for (Map.Entry<String, MockSourceSplit> entry : splits.entrySet()) {
				MockSourceSplit split = entry.getValue();
				for (int i = 0; i < numRecordsPerSplitPerFetch && !split.isFinished(); i++) {
					// This call may throw InterruptedException.
					int[] record = split.getNext(blockingFetch);
					if (record != null) {
						records.add(entry.getKey(), record);
					}
				}
				if (split.isFinished()) {
					records.addFinishedSplit(entry.getKey());
				}
			}
		} catch (InterruptedException ie) {
			// Catch the exception and return the records that are already read.
			if (!blockingFetch) {
				throw new RuntimeException("Caught unexpected interrupted exception.");
			}
		}
		return records;
	}
}
