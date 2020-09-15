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
import org.apache.flink.core.io.InputStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkState;

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

	/** A queue to buffer the elements fetched by the fetcher thread. */
	private final FutureCompletingBlockingQueue<RecordsWithSplitIds<E>> elementsQueue;

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

	/** The latest fetched batch of records-by-split from the split reader. */
	@Nullable private RecordsWithSplitIds<E> currentFetch;
	@Nullable private SplitContext<T, SplitStateT> currentSplitContext;
	@Nullable private SourceOutput<T> currentSplitOutput;

	/** Indicating whether the SourceReader will be assigned more splits or not.*/
	private boolean noMoreSplitsAssignment;

	public SourceReaderBase(
			FutureCompletingBlockingQueue<RecordsWithSplitIds<E>> elementsQueue,
			SplitFetcherManager<E, SplitT> splitFetcherManager,
			RecordEmitter<E, T, SplitStateT> recordEmitter,
			Configuration config,
			SourceReaderContext context) {
		this.elementsQueue = elementsQueue;
		this.splitFetcherManager = splitFetcherManager;
		this.recordEmitter = recordEmitter;
		this.splitStates = new HashMap<>();
		this.options = new SourceReaderOptions(config);
		this.config = config;
		this.context = context;
		this.noMoreSplitsAssignment = false;
	}

	@Override
	public void start() {}

	@Override
	public InputStatus pollNext(ReaderOutput<T> output) throws Exception {
		// make sure we have a fetch we are working on, or move to the next
		RecordsWithSplitIds<E> recordsWithSplitId = this.currentFetch;
		if (recordsWithSplitId == null) {
			recordsWithSplitId = getNextFetch(output);
			if (recordsWithSplitId == null) {
				return trace(finishedOrAvailableLater());
			}
		}

		// we need to loop here, because we may have to go across splits
		while (true) {
			// Process one record.
			final E record = recordsWithSplitId.nextRecordFromSplit();
			if (record != null) {
				// emit the record.
				recordEmitter.emitRecord(record, currentSplitOutput, currentSplitContext.state);
				LOG.trace("Emitted record: {}", record);

				// We always emit MORE_AVAILABLE here, even though we do not strictly know whether
				// more is available. If nothing more is available, the next invocation will find
				// this out and return the correct status.
				// That means we emit the occasional 'false positive' for availability, but this
				// saves us doing checks for every record. Ultimately, this is cheaper.
				return trace(InputStatus.MORE_AVAILABLE);
			}
			else if (!moveToNextSplit(recordsWithSplitId, output)) {
				// The fetch is done and we just discovered that and have not emitted anything, yet.
				// We need to move to the next fetch. As a shortcut, we call pollNext() here again,
				// rather than emitting nothing and waiting for the caller to call us again.
				return pollNext(output);
			}
			// else fall through the loop
		}
	}

	private InputStatus trace(InputStatus status) {
		LOG.trace("Source reader status: {}", status);
		return status;
	}

	@Nullable
	private RecordsWithSplitIds<E> getNextFetch(final ReaderOutput<T> output) {
		splitFetcherManager.checkErrors();

		LOG.trace("Getting next source data batch from queue");
		final RecordsWithSplitIds<E> recordsWithSplitId = elementsQueue.poll();
		if (recordsWithSplitId == null || !moveToNextSplit(recordsWithSplitId, output)) {
			// No element available, set to available later if needed.
			return null;
		}

		currentFetch = recordsWithSplitId;
		return recordsWithSplitId;
	}

	private void finishCurrentFetch(final RecordsWithSplitIds<E> fetch, final ReaderOutput<T> output) {
		currentFetch = null;
		currentSplitContext = null;
		currentSplitOutput = null;

		final Set<String> finishedSplits = fetch.finishedSplits();
		if (!finishedSplits.isEmpty()) {
			LOG.info("Finished reading split(s) {}", finishedSplits);
			for (String finishedSplitId : finishedSplits) {
				splitStates.remove(finishedSplitId);
				output.releaseOutputForSplit(finishedSplitId);
			}
			onSplitFinished(finishedSplits);
		}

		fetch.recycle();
	}

	private boolean moveToNextSplit(RecordsWithSplitIds<E> recordsWithSplitIds, ReaderOutput<T> output) {
		final String nextSplitId = recordsWithSplitIds.nextSplit();
		if (nextSplitId == null) {
			LOG.trace("Current fetch is finished.");
			finishCurrentFetch(recordsWithSplitIds, output);
			return false;
		}

		currentSplitContext = splitStates.get(nextSplitId);
		checkState(currentSplitContext != null, "Have records for a split that was not registered");
		currentSplitOutput = currentSplitContext.getOrCreateSplitOutput(output);
		LOG.trace("Emitting records from fetch for split {}", nextSplitId);
		return true;
	}

	@Override
	public CompletableFuture<Void> isAvailable() {
		return currentFetch != null ? FutureCompletingBlockingQueue.AVAILABLE : elementsQueue.getAvailabilityFuture();
	}

	@Override
	public List<SplitT> snapshotState() {
		List<SplitT> splits = new ArrayList<>();
		splitStates.forEach((id, context) -> splits.add(toSplitType(id, context.state)));
		return splits;
	}

	@Override
	public void addSplits(List<SplitT> splits) {
		LOG.info("Adding split(s) to reader: {}", splits);
		// Initialize the state for each split.
		splits.forEach(s -> splitStates.put(s.splitId(), new SplitContext<>(s.splitId(), initializedState(s))));
		// Hand over the splits to the split fetcher to start fetch.
		splitFetcherManager.addSplits(splits);
	}

	@Override
	public void handleSourceEvents(SourceEvent sourceEvent) {
		LOG.trace("Handling source event: {}", sourceEvent);
		if (sourceEvent instanceof NoMoreSplitsEvent) {
			LOG.info("Reader received NoMoreSplits event.");
			noMoreSplitsAssignment = true;
			elementsQueue.notifyAvailable();
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
		final boolean allFetchersHaveShutdown = splitFetcherManager.maybeShutdownFinishedFetchers();
		if (noMoreSplitsAssignment && allFetchersHaveShutdown && elementsQueue.isEmpty()) {
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
