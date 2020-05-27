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

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SourceReaderBase;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.base.source.reader.synchronization.FutureNotifier;
import org.apache.flink.util.ThrowableCatchingRunnable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * A class responsible for starting the {@link SplitFetcher} and manage the life cycles of them.
 * This class works with the {@link SourceReaderBase}.
 *
 * <p>The split fetcher manager could be used to support different threading models by implementing
 * the {@link #addSplits(List)} method differently. For example, a single thread split fetcher
 * manager would only start a single fetcher and assign all the splits to it. A one-thread-per-split
 * fetcher may spawn a new thread every time a new split is assigned.
 */
public abstract class SplitFetcherManager<E, SplitT extends SourceSplit> {
	private static final Logger LOG = LoggerFactory.getLogger(SplitFetcherManager.class);

	private final Consumer<Throwable> errorHandler;

	/** An atomic integer to generate monotonically increasing fetcher ids. */
	private final AtomicInteger fetcherIdGenerator;

	/** A supplier to provide split readers. */
	private final Supplier<SplitReader<E, SplitT>> splitReaderFactory;

	/** Uncaught exception in the split fetchers.*/
	private final AtomicReference<Throwable> uncaughtFetcherException;

	/** The element queue that the split fetchers will put elements into. */
	private final BlockingQueue<RecordsWithSplitIds<E>> elementsQueue;

	/** A map keeping track of all the split fetchers. */
	protected final Map<Integer, SplitFetcher<E, SplitT>> fetchers;

	/** An executor service with two threads. One for the fetcher and one for the future completing thread. */
	private final ExecutorService executors;

	/** Indicating the split fetcher manager has closed or not. */
	private volatile boolean closed;

	/**
	 * Create a split fetcher manager.
	 *
	 * @param futureNotifier a notifier to notify the complete of a future.
	 * @param elementsQueue the queue that split readers will put elements into.
	 * @param splitReaderFactory a supplier that could be used to create split readers.
	 */
	public SplitFetcherManager(
			FutureNotifier futureNotifier,
			FutureCompletingBlockingQueue<RecordsWithSplitIds<E>> elementsQueue,
			Supplier<SplitReader<E, SplitT>> splitReaderFactory) {
		this.elementsQueue = elementsQueue;
		this.errorHandler = new Consumer<Throwable>() {
			@Override
			public void accept(Throwable t) {
				LOG.error("Received uncaught exception.", t);
				if (!uncaughtFetcherException.compareAndSet(null, t)) {
					// Add the exception to the exception list.
					uncaughtFetcherException.get().addSuppressed(t);
					// Wake up the main thread to let it know the exception.
					futureNotifier.notifyComplete();
				}
			}
		};
		this.splitReaderFactory = splitReaderFactory;
		this.uncaughtFetcherException = new AtomicReference<>(null);
		this.fetcherIdGenerator = new AtomicInteger(0);
		this.fetchers = new ConcurrentHashMap<>();

		// Create the executor with a thread factory that fails the source reader if one of
		// the fetcher thread exits abnormally.
		this.executors = Executors.newCachedThreadPool(r -> new Thread(r, "SourceFetcher"));
		this.closed = false;
	}

	public abstract void addSplits(List<SplitT> splitsToAdd);

	protected void startFetcher(SplitFetcher<E, SplitT> fetcher) {
		executors.submit(new ThrowableCatchingRunnable(errorHandler, fetcher));
	}

	/**
	 * Synchronize method to ensure no fetcher is created after the split fetcher manager has closed.
	 *
	 * @return the created split fetcher.
	 * @throws IllegalStateException if the split fetcher manager has closed.
	 */
	protected synchronized SplitFetcher<E, SplitT> createSplitFetcher() {
		if (closed) {
			throw new IllegalStateException("The split fetcher manager has closed.");
		}
		// Create SplitReader.
		SplitReader<E, SplitT> splitReader = splitReaderFactory.get();

		int fetcherId = fetcherIdGenerator.getAndIncrement();
		SplitFetcher<E, SplitT> splitFetcher = new SplitFetcher<>(
			fetcherId,
			elementsQueue,
			splitReader,
			() -> fetchers.remove(fetcherId));
		fetchers.put(fetcherId, splitFetcher);
		return splitFetcher;
	}

	/**
	 * Check and shutdown the fetchers that have completed their work.
	 *
	 * @return true if all the fetchers have completed the work, false otherwise.
	 */
	public boolean maybeShutdownFinishedFetchers() {
		Iterator<Map.Entry<Integer, SplitFetcher<E, SplitT>>> iter = fetchers.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry<Integer, SplitFetcher<E, SplitT>> entry = iter.next();
			SplitFetcher<E, SplitT> fetcher = entry.getValue();
			if (fetcher.isIdle()) {
				fetcher.shutdown();
				iter.remove();
			}
		}
		return fetchers.isEmpty();
	}

	/**
	 * Close the split fetcher manager.
	 *
	 * @param timeoutMs the max time in milliseconds to wait.
	 * @throws Exception when failed to close the split fetcher manager.
	 */
	public synchronized void close(long timeoutMs) throws Exception {
		closed = true;
		fetchers.values().forEach(SplitFetcher::shutdown);
		executors.shutdown();
		if (!executors.awaitTermination(timeoutMs, TimeUnit.MILLISECONDS)) {
			LOG.warn("Failed to close the source reader in {} ms. There are still {} split fetchers running",
				timeoutMs, fetchers.size());
		}
	}

	public void checkErrors() {
		if (uncaughtFetcherException.get() != null) {
			throw new RuntimeException("One or more fetchers have encountered exception",
				uncaughtFetcherException.get());
		}
	}
}
