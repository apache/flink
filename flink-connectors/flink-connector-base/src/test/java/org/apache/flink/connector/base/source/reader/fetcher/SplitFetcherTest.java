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
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.mocks.MockSplitReader;
import org.apache.flink.connector.base.source.reader.mocks.TestingSourceSplit;
import org.apache.flink.connector.base.source.reader.mocks.TestingSplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.util.ExceptionUtils;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for {@link SplitFetcher}.
 */
public class SplitFetcherTest {

	@Test
	public void testNewFetcherIsIdle() {
		final SplitFetcher<Object, TestingSourceSplit> fetcher = createFetcher(new TestingSplitReader<>());
		assertTrue(fetcher.isIdle());
	}

	@Test
	public void testFetcherNotIdleAfterSplitAdded() {
		final SplitFetcher<Object, TestingSourceSplit> fetcher = createFetcher(new TestingSplitReader<>());
		final TestingSourceSplit split = new TestingSourceSplit("test-split");

		fetcher.addSplits(Collections.singletonList(split));

		assertFalse(fetcher.isIdle());

		// need to loop here because the internal wakeup flag handling means we need multiple loops
		while (fetcher.assignedSplits().isEmpty()) {
			fetcher.runOnce();
			assertFalse(fetcher.isIdle());
		}
	}

	@Test
	public void testIdleAfterFinishedSplitsEnqueued() {
		final SplitFetcher<Object, TestingSourceSplit> fetcher = createFetcherWithSplit(
			"test-split", new TestingSplitReader<>(finishedSplitFetch("test-split")));

		fetcher.runOnce();

		assertTrue(fetcher.assignedSplits().isEmpty());
		assertTrue(fetcher.isIdle());
	}

	@Test
	public void testNotifiesWhenGoingIdle() {
		final FutureCompletingBlockingQueue<RecordsWithSplitIds<Object>> queue = new FutureCompletingBlockingQueue<>();
		final SplitFetcher<Object, TestingSourceSplit> fetcher = createFetcherWithSplit(
			"test-split",
			queue,
			new TestingSplitReader<>(finishedSplitFetch("test-split")));

		fetcher.runOnce();

		assertTrue(fetcher.assignedSplits().isEmpty());
		assertTrue(fetcher.isIdle());
		assertTrue(queue.getAvailabilityFuture().isDone());
	}

	@Test
	public void testNotifiesOlderFutureWhenGoingIdle() {
		final FutureCompletingBlockingQueue<RecordsWithSplitIds<Object>> queue = new FutureCompletingBlockingQueue<>();
		final SplitFetcher<Object, TestingSourceSplit> fetcher = createFetcherWithSplit(
				"test-split",
				queue,
				new TestingSplitReader<>(finishedSplitFetch("test-split")));

		final CompletableFuture<?> future = queue.getAvailabilityFuture();

		fetcher.runOnce();

		assertTrue(fetcher.assignedSplits().isEmpty());
		assertTrue(fetcher.isIdle());
		assertTrue(future.isDone());
	}

	@Test
	public void testNotifiesWhenGoingIdleConcurrent() throws Exception {
		final FutureCompletingBlockingQueue<RecordsWithSplitIds<Object>> queue =
				new FutureCompletingBlockingQueue<>();
		final SplitFetcher<Object, TestingSourceSplit> fetcher = createFetcherWithSplit(
			"test-split", queue, new TestingSplitReader<>(finishedSplitFetch("test-split")));

		final QueueDrainerThread queueDrainer = new QueueDrainerThread(queue, fetcher, 1);
		queueDrainer.start();

		fetcher.runOnce();

		queueDrainer.sync();

		// either we got the notification that the fetcher went idle after the queue was drained (thread finished)
		// or the fetcher was already idle when the thread drained the queue (then we need no additional notification)
		assertTrue(queue.getAvailabilityFuture().isDone() || queueDrainer.wasIdleWhenFinished());
	}

	@Test
	public void testNotifiesOlderFutureWhenGoingIdleConcurrent() throws Exception {
		final FutureCompletingBlockingQueue<RecordsWithSplitIds<Object>> queue =
				new FutureCompletingBlockingQueue<>();
		final SplitFetcher<Object, TestingSourceSplit> fetcher = createFetcherWithSplit(
				"test-split", queue, new TestingSplitReader<>(finishedSplitFetch("test-split")));

		final QueueDrainerThread queueDrainer = new QueueDrainerThread(queue, fetcher, 1);
		queueDrainer.start();

		final CompletableFuture<?> future = queue.getAvailabilityFuture();

		fetcher.runOnce();
		assertTrue(future.isDone());

		queueDrainer.sync();
	}

	@Test
	public void testWakeup() throws InterruptedException {
		final int numSplits = 3;
		final int numRecordsPerSplit = 10_000;
		final int wakeupRecordsInterval = 10;
		final int numTotalRecords = numRecordsPerSplit * numSplits;

		FutureCompletingBlockingQueue<RecordsWithSplitIds<int[]>> elementQueue =
			new FutureCompletingBlockingQueue<>(1);
		SplitFetcher<int[], MockSourceSplit> fetcher =
				new SplitFetcher<>(
						0,
						elementQueue,
						new MockSplitReader(2, true),
						ExceptionUtils::rethrow,
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
		Thread wakeUpCaller = new Thread("Wakeup Caller") {
			@Override
			public void run() {
				int lastWakeup = 0;
				while (recordsRead.size() < numTotalRecords && !stop.get()) {
					int numRecordsRead = recordsRead.size();
					if (numRecordsRead >= lastWakeup + wakeupRecordsInterval) {
						fetcher.wakeUp(false);
						wakeupTimes.incrementAndGet();
						lastWakeup = numRecordsRead;
					}
				}
			}
		};

		try {
			fetcherThread.start();
			wakeUpCaller.start();

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
			wakeUpCaller.join();
		}
	}

	@Test
	public void testClose() {
		TestingSplitReader<Object, TestingSourceSplit> splitReader = new TestingSplitReader<>();
		final SplitFetcher<Object, TestingSourceSplit> fetcher = createFetcher(splitReader);
		fetcher.shutdown();
		fetcher.run();
		assertTrue(splitReader.isClosed());
	}

	// ------------------------------------------------------------------------
	//  testing utils
	// ------------------------------------------------------------------------

	private static <E> RecordsBySplits<E> finishedSplitFetch(String splitId) {
		return new RecordsBySplits<>(Collections.emptyMap(), Collections.singleton(splitId));
	}

	private static <E> SplitFetcher<E, TestingSourceSplit> createFetcher(
			final SplitReader<E, TestingSourceSplit> reader) {
		return createFetcher(reader, new FutureCompletingBlockingQueue<>());
	}

	private static <E> SplitFetcher<E, TestingSourceSplit> createFetcher(
			final SplitReader<E, TestingSourceSplit> reader,
			final FutureCompletingBlockingQueue<RecordsWithSplitIds<E>> queue) {
		return new SplitFetcher<>(0, queue, reader, ExceptionUtils::rethrow, () -> {});
	}

	private static <E> SplitFetcher<E, TestingSourceSplit> createFetcherWithSplit(
			final String splitId,
			final SplitReader<E, TestingSourceSplit> reader) {
		return createFetcherWithSplit(splitId, new FutureCompletingBlockingQueue<>(), reader);
	}

	private static <E> SplitFetcher<E, TestingSourceSplit> createFetcherWithSplit(
			final String splitId,
			final FutureCompletingBlockingQueue<RecordsWithSplitIds<E>> queue,
			final SplitReader<E, TestingSourceSplit> reader) {

		final SplitFetcher<E, TestingSourceSplit> fetcher = createFetcher(reader, queue);

		fetcher.addSplits(Collections.singletonList(new TestingSourceSplit(splitId)));
		while (fetcher.assignedSplits().isEmpty()) {
			fetcher.runOnce();
		}
		return fetcher;
	}

	// ------------------------------------------------------------------------

	private static final class QueueDrainerThread extends CheckedThread {

		private final FutureCompletingBlockingQueue<?> queue;
		private final SplitFetcher<?, ?> fetcher;
		private final int numFetchesToTake;

		private volatile boolean wasIdleWhenFinished;

		QueueDrainerThread(FutureCompletingBlockingQueue<?> queue, SplitFetcher<?, ?> fetcher, int numFetchesToTake) {
			super("Queue Drainer");
			setPriority(Thread.MAX_PRIORITY);
			this.queue = queue;
			this.fetcher = fetcher;
			this.numFetchesToTake = numFetchesToTake;
		}

		@Override
		public void go() throws Exception {
			int remaining = numFetchesToTake;
			while (remaining > 0) {
				remaining--;
				queue.take();
			}

			wasIdleWhenFinished = fetcher.isIdle();
		}

		public boolean wasIdleWhenFinished() {
			return wasIdleWhenFinished;
		}
	}
}
