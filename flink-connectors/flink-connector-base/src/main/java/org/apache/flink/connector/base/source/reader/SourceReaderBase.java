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

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.event.NoMoreSplitsEvent;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcherManager;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.base.source.reader.synchronization.FutureNotifier;
import org.apache.flink.core.io.InputStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;

/**
 * An abstract implementation of {@link SourceReader} which provides some sychronization between
 * the mail box main thread and the SourceReader internal threads. This class allows user to
 * just provide a {@link SplitReader} and snapshot the split state.
 *
 * @param <E> The rich element type that contains information for split state update or timestamp extraction.
 * @param <T> The final element type to emit.
 * @param <SplitT> the immutable split type.
 * @param <SplitStateT> the mutable type of split state.
 */
public abstract class SourceReaderBase<E, T, SplitT extends SourceSplit, SplitStateT>
		implements SourceReader<T, SplitT> {
	private static final Logger LOG = LoggerFactory.getLogger(SourceReaderBase.class);

	/** A future notifier to notify when this reader requires attention. */
	private final FutureNotifier futureNotifier;

	/** A queue to buffer the elements fetched by the fetcher thread. */
	private final BlockingQueue<RecordsWithSplitIds<E>> elementsQueue;

	/** The state of the splits. */
	private final Map<String, SplitContext<T, SplitStateT>> splitStates;

	/** The record emitter to handle the records read by the SplitReaders. */
	protected final RecordEmitter<E, T, SplitStateT> recordEmitter;

	/** The split fetcher manager to run split fetchers. */
	protected final SplitFetcherManager<E, SplitT> splitFetcherManager;

	/** The configuration for the reader. */
	protected final SourceReaderOptions options;

	/** The raw configurations that may be used by subclasses. */
	protected final Configuration config;

	/** The context of this source reader. */
	protected SourceReaderContext context;

	/** The last element to ensure it is fully handled. */
	private SplitsRecordIterator<E> splitIter;

	/** Indicating whether the SourceReader will be assigned more splits or not.*/
	private boolean noMoreSplitsAssignment;

	public SourceReaderBase(
			FutureNotifier futureNotifier,
			FutureCompletingBlockingQueue<RecordsWithSplitIds<E>> elementsQueue,
			SplitFetcherManager<E, SplitT> splitFetcherManager,
			RecordEmitter<E, T, SplitStateT> recordEmitter,
			Configuration config,
			SourceReaderContext context) {
		this.futureNotifier = futureNotifier;
		this.elementsQueue = elementsQueue;
		this.splitFetcherManager = splitFetcherManager;
		this.recordEmitter = recordEmitter;
		this.splitStates = new HashMap<>();
		this.splitIter = null;
		this.options = new SourceReaderOptions(config);
		this.config = config;
		this.context = context;
		this.noMoreSplitsAssignment = false;
	}

	@Override
	public void start() {

	}

	@Override
	public InputStatus pollNext(ReaderOutput<T> output) throws Exception {
		splitFetcherManager.checkErrors();
		// poll from the queue if the last element was successfully handled. Otherwise
		// just pass the last element again.
		RecordsWithSplitIds<E> recordsWithSplitId = null;
		boolean newFetch = splitIter == null || !splitIter.hasNext();
		if (newFetch) {
			recordsWithSplitId = elementsQueue.poll();
		}

		InputStatus status;
		if (newFetch && recordsWithSplitId == null) {
			// No element available, set to available later if needed.
			status = finishedOrAvailableLater();
		} else {
			// Update the record iterator if it is a new fetch.
			if (newFetch) {
				splitIter = new SplitsRecordIterator<>(recordsWithSplitId);
			}
			// Process one record.
			if (splitIter.hasNext()) {
				// emit the record.
				final E record = splitIter.next();
				final SplitContext<T, SplitStateT> splitContext = splitStates.get(splitIter.currentSplitId());
				final SourceOutput<T> splitOutput = splitContext.getOrCreateSplitOutput(output);
				recordEmitter.emitRecord(record, splitOutput, splitContext.state);
				LOG.trace("Emitted record: {}", record);
			}
			// Do some cleanup if the all the records in the current splitIter have been processed.
			if (!splitIter.hasNext()) {
				// First remove the state of the split.
				splitIter.finishedSplitIds().forEach((id) -> {
					splitStates.remove(id);
					output.releaseOutputForSplit(id);
				});
				// Handle the finished splits.
				onSplitFinished(splitIter.finishedSplitIds());
				// Prepare the return status based on the availability of the next element.
				status = finishedOrAvailableLater();
			} else {
				// There are more records from the current splitIter.
				status = InputStatus.MORE_AVAILABLE;
			}
		}
		LOG.trace("Source reader status: {}", status);
		return status;
	}

	@Override
	public CompletableFuture<Void> isAvailable() {
		// The order matters here. We first get the future. After this point, if the queue
		// is empty or there is no error in the split fetcher manager, we can ensure that
		// the future will be completed by the fetcher once it put an element into the element queue,
		// or it will be completed when an error occurs.
		CompletableFuture<Void> future = futureNotifier.future();
		splitFetcherManager.checkErrors();
		if (!elementsQueue.isEmpty()) {
			// The fetcher got the new elements after the last poll, or their is a finished split.
			// Simply complete the future and return;
			futureNotifier.notifyComplete();
		}
		return future;
	}

	@Override
	public List<SplitT> snapshotState() {
		List<SplitT> splits = new ArrayList<>();
		splitStates.forEach((id, context) -> splits.add(toSplitType(id, context.state)));
		return splits;
	}

	@Override
	public void addSplits(List<SplitT> splits) {
		LOG.trace("Adding splits {}", splits);
		// Initialize the state for each split.
		splits.forEach(s -> splitStates.put(s.splitId(), new SplitContext<>(s.splitId(), initializedState(s))));
		// Hand over the splits to the split fetcher to start fetch.
		splitFetcherManager.addSplits(splits);
	}

	@Override
	public void handleSourceEvents(SourceEvent sourceEvent) {
		LOG.trace("Handling source event: {}", sourceEvent);
		if (sourceEvent instanceof NoMoreSplitsEvent) {
			noMoreSplitsAssignment = true;
			futureNotifier.notifyComplete();
		}
	}

	@Override
	public void close() throws Exception {
		LOG.info("Closing Source Reader.");
		splitFetcherManager.close(options.sourceReaderCloseTimeout);
	}



	// -------------------- Abstract method to allow different implementations ------------------
	/**
	 * Handles the finished splits to clean the state if needed.
	 */
	protected abstract void onSplitFinished(Collection<String> finishedSplitIds);

	/**
	 * When new splits are added to the reader. The initialize the state of the new splits.
	 *
	 * @param split a newly added split.
	 */
	protected abstract SplitStateT initializedState(SplitT split);

	/**
	 * Convert a mutable SplitStateT to immutable SplitT.
	 *
	 * @param splitState splitState.
	 * @return an immutable Split state.
	 */
	protected abstract SplitT toSplitType(String splitId, SplitStateT splitState);

	// ------------------ private helper methods ---------------------

	private InputStatus finishedOrAvailableLater() {
		boolean allFetchersHaveShutdown = splitFetcherManager.maybeShutdownFinishedFetchers();
		boolean allElementsEmitted = elementsQueue.isEmpty() && (splitIter == null || !splitIter.hasNext());
		if (noMoreSplitsAssignment && allFetchersHaveShutdown && allElementsEmitted) {
			return InputStatus.END_OF_INPUT;
		} else {
			return InputStatus.NOTHING_AVAILABLE;
		}
	}

	// ------------------ private helper classes ---------------------

	private static final class SplitContext<T, SplitStateT> {

		final String splitId;
		final SplitStateT state;
		SourceOutput<T> sourceOutput;

		private SplitContext(String splitId, SplitStateT state) {
			this.state = state;
			this.splitId = splitId;
		}

		SourceOutput<T> getOrCreateSplitOutput(ReaderOutput<T> mainOutput) {
			if (sourceOutput == null) {
				sourceOutput = mainOutput.createOutputForSplit(splitId);
			}
			return sourceOutput;
		}
	}
}
